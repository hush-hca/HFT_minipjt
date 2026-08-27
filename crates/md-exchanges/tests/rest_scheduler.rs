use std::time::{Duration, Instant};

use md_exchanges::derivatives::scheduler::{
    BudgetError, HealthSignal, RequestClass, RestScheduler,
};
use reqwest::{
    StatusCode,
    header::{HeaderMap, HeaderValue},
};

#[test]
fn market_and_account_requests_preserve_configured_headroom() {
    let start = Instant::now();
    let scheduler = RestScheduler::binance_weighted(1_200, 200, 100).unwrap();
    scheduler
        .acquire(RequestClass::MarketData, 900, start)
        .unwrap();
    assert!(matches!(
        scheduler.acquire(RequestClass::MarketData, 1, start),
        Err(BudgetError::ReservedHeadroom)
    ));
    scheduler
        .acquire(RequestClass::Account, 100, start)
        .unwrap();
    assert!(matches!(
        scheduler.acquire(RequestClass::Account, 1, start),
        Err(BudgetError::AccountHeadroom)
    ));
    scheduler.acquire(RequestClass::Order, 200, start).unwrap();
}

#[test]
fn unknown_weight_and_overflow_do_not_consume_budget() {
    let start = Instant::now();
    let scheduler = RestScheduler::binance_weighted(1_200, 200, 0).unwrap();
    assert!(matches!(
        scheduler.acquire(RequestClass::MarketData, 0, start),
        Err(BudgetError::UnknownWeight)
    ));
    assert!(matches!(
        scheduler.acquire(RequestClass::MarketData, u32::MAX, start),
        Err(BudgetError::WeightOverflow)
    ));
    assert_eq!(scheduler.snapshot(start).used_weight, 0);
}

#[test]
fn minute_window_reset_is_driven_by_injected_instant() {
    let start = Instant::now();
    let scheduler = RestScheduler::binance_weighted(10, 2, 0).unwrap();
    scheduler
        .acquire(RequestClass::MarketData, 8, start)
        .unwrap();
    assert!(
        scheduler
            .acquire(RequestClass::MarketData, 1, start + Duration::from_secs(59))
            .is_err()
    );
    scheduler
        .acquire(RequestClass::MarketData, 8, start + Duration::from_secs(60))
        .unwrap();
}

#[test]
fn official_used_weight_header_synchronizes_upward() {
    let start = Instant::now();
    let scheduler = RestScheduler::binance_weighted(1_200, 200, 0).unwrap();
    let permit = scheduler
        .acquire(RequestClass::MarketData, 10, start)
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-mbx-used-weight-1m", HeaderValue::from_static("999"));
    scheduler
        .record_response_at(
            &permit,
            &headers,
            StatusCode::OK,
            None,
            start,
            1_700_000_000_000,
        )
        .unwrap();
    assert_eq!(scheduler.snapshot(start).used_weight, 999);
    assert!(matches!(
        scheduler.acquire(RequestClass::MarketData, 2, start),
        Err(BudgetError::ReservedHeadroom)
    ));
}

#[test]
fn official_bybit_limit_headers_synchronize_capacity() {
    let start = Instant::now();
    let scheduler = RestScheduler::bybit_endpoint(10).unwrap();
    let permit = scheduler
        .acquire(RequestClass::MarketData, 1, start)
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-bapi-limit", HeaderValue::from_static("600"));
    headers.insert("x-bapi-limit-status", HeaderValue::from_static("250"));
    headers.insert(
        "x-bapi-limit-reset-timestamp",
        HeaderValue::from_static("1700000000000"),
    );
    scheduler
        .record_response_at(
            &permit,
            &headers,
            StatusCode::OK,
            None,
            start,
            1_700_000_000_000,
        )
        .unwrap();
    let snapshot = scheduler.snapshot(start);
    assert_eq!(snapshot.limit_per_minute, 0);
    assert_eq!(snapshot.venue_request_limit, Some(10));
    assert_eq!(snapshot.venue_requests_remaining, Some(9));
}

#[test]
fn retry_after_blocks_without_sleep_and_emits_health() {
    let start = Instant::now();
    let scheduler = RestScheduler::binance_weighted(1_200, 200, 0).unwrap();
    let permit = scheduler
        .acquire(RequestClass::MarketData, 1, start)
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("retry-after", HeaderValue::from_static("3"));
    let signal = scheduler
        .record_response_at(
            &permit,
            &headers,
            StatusCode::TOO_MANY_REQUESTS,
            None,
            start,
            1_700_000_000_000,
        )
        .unwrap()
        .unwrap();
    assert!(
        matches!(signal, HealthSignal::RateLimited { retry_after } if retry_after == Duration::from_secs(3))
    );
    assert!(matches!(
        scheduler.acquire(RequestClass::Order, 1, start + Duration::from_secs(2)),
        Err(BudgetError::Blocked { .. })
    ));
    scheduler
        .acquire(RequestClass::Order, 1, start + Duration::from_secs(3))
        .unwrap();
}

#[test]
fn bybit_zero_remaining_and_10006_block_until_epoch_reset() {
    let start = Instant::now();
    let scheduler = RestScheduler::bybit_endpoint(10).unwrap();
    let permit = scheduler
        .acquire(RequestClass::MarketData, 1, start)
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-bapi-limit", HeaderValue::from_static("10"));
    headers.insert("x-bapi-limit-status", HeaderValue::from_static("0"));
    headers.insert(
        "x-bapi-limit-reset-timestamp",
        HeaderValue::from_static("1700000001500"),
    );
    scheduler
        .record_response_at(
            &permit,
            &headers,
            StatusCode::OK,
            Some(10006),
            start,
            1_700_000_000_000,
        )
        .unwrap();
    assert!(matches!(
        scheduler.acquire(
            RequestClass::MarketData,
            1,
            start + Duration::from_millis(1499)
        ),
        Err(BudgetError::Blocked { .. })
    ));
    scheduler
        .acquire(
            RequestClass::MarketData,
            1,
            start + Duration::from_millis(1500),
        )
        .unwrap();
}

#[test]
fn status_blocks_are_monotonic_and_ignore_malformed_telemetry() {
    let start = Instant::now();
    let scheduler = RestScheduler::bybit_endpoint(10).unwrap();
    let first = scheduler
        .acquire(RequestClass::MarketData, 1, start)
        .unwrap();
    let second = scheduler
        .acquire(RequestClass::MarketData, 1, start)
        .unwrap();
    let mut malformed = HeaderMap::new();
    malformed.insert("x-bapi-limit", HeaderValue::from_static("bad"));
    let signal = scheduler
        .record_response_at(
            &first,
            &malformed,
            StatusCode::FORBIDDEN,
            None,
            start,
            1_700_000_000_000,
        )
        .unwrap()
        .unwrap();
    assert!(
        matches!(signal, HealthSignal::IpBanned { retry_after } if retry_after == Duration::from_secs(600))
    );
    let later = scheduler
        .record_response_at(
            &second,
            &HeaderMap::new(),
            StatusCode::TOO_MANY_REQUESTS,
            None,
            start + Duration::from_secs(1),
            1_700_000_001_000,
        )
        .unwrap()
        .unwrap();
    assert!(
        matches!(later, HealthSignal::RateLimited { retry_after } if retry_after == Duration::from_secs(599))
    );
    assert!(matches!(
        scheduler.acquire(
            RequestClass::MarketData,
            1,
            start + Duration::from_secs(100)
        ),
        Err(BudgetError::Blocked { .. })
    ));
}

#[test]
fn retry_after_http_date_uses_injected_wall_clock() {
    let start = Instant::now();
    let scheduler = RestScheduler::binance_weighted(1_200, 200, 0).unwrap();
    let permit = scheduler
        .acquire(RequestClass::MarketData, 1, start)
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        "retry-after",
        HeaderValue::from_static("Thu, 01 Jan 1970 00:00:10 GMT"),
    );
    scheduler
        .record_response_at(
            &permit,
            &headers,
            StatusCode::TOO_MANY_REQUESTS,
            None,
            start,
            0,
        )
        .unwrap();
    assert!(matches!(
        scheduler.acquire(RequestClass::Order, 1, start + Duration::from_secs(9)),
        Err(BudgetError::Blocked { .. })
    ));
    scheduler
        .acquire(RequestClass::Order, 1, start + Duration::from_secs(10))
        .unwrap();
}

#[test]
fn bybit_local_bucket_is_bounded_before_telemetry_and_refills() {
    let start = Instant::now();
    let scheduler = RestScheduler::bybit_endpoint(2).unwrap();
    scheduler
        .acquire(RequestClass::MarketData, 1, start)
        .unwrap();
    scheduler
        .acquire(RequestClass::MarketData, 1, start)
        .unwrap();
    assert!(matches!(
        scheduler.acquire(RequestClass::MarketData, 1, start),
        Err(BudgetError::Exhausted)
    ));
    scheduler
        .acquire(RequestClass::MarketData, 1, start + Duration::from_secs(1))
        .unwrap();
}

#[test]
fn bybit_uses_a_true_trailing_one_second_window_at_boundaries() {
    let start = Instant::now();
    let scheduler = RestScheduler::bybit_endpoint(2).unwrap();
    scheduler
        .acquire(RequestClass::MarketData, 1, start)
        .unwrap();
    scheduler
        .acquire(
            RequestClass::MarketData,
            1,
            start + Duration::from_millis(999),
        )
        .unwrap();
    scheduler
        .acquire(RequestClass::MarketData, 1, start + Duration::from_secs(1))
        .unwrap();
    assert!(matches!(
        scheduler.acquire(
            RequestClass::MarketData,
            1,
            start + Duration::from_millis(1_001)
        ),
        Err(BudgetError::Exhausted)
    ));
}

#[test]
fn permits_are_scheduler_bound_and_single_use_for_responses() {
    let start = Instant::now();
    let owner = RestScheduler::bybit_endpoint(10).unwrap();
    let foreign = RestScheduler::bybit_endpoint(10).unwrap();
    let permit = owner.acquire(RequestClass::MarketData, 1, start).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-bapi-limit", HeaderValue::from_static("10"));
    headers.insert("x-bapi-limit-status", HeaderValue::from_static("8"));
    headers.insert(
        "x-bapi-limit-reset-timestamp",
        HeaderValue::from_static("1700000000000"),
    );

    assert!(matches!(
        foreign.record_response_at(
            &permit,
            &headers,
            StatusCode::OK,
            None,
            start,
            1_700_000_000_000,
        ),
        Err(BudgetError::ForeignPermit)
    ));
    assert_eq!(foreign.snapshot(start).venue_requests_remaining, Some(10));

    owner
        .record_response_at(
            &permit,
            &headers,
            StatusCode::OK,
            None,
            start,
            1_700_000_000_000,
        )
        .unwrap();
    assert_eq!(owner.snapshot(start).venue_requests_remaining, Some(8));

    let mut replay = HeaderMap::new();
    replay.insert("x-bapi-limit", HeaderValue::from_static("10"));
    replay.insert("x-bapi-limit-status", HeaderValue::from_static("9"));
    replay.insert(
        "x-bapi-limit-reset-timestamp",
        HeaderValue::from_static("1700000000000"),
    );
    assert!(matches!(
        owner.record_response_at(
            &permit,
            &replay,
            StatusCode::OK,
            None,
            start,
            1_700_000_000_000,
        ),
        Err(BudgetError::PermitAlreadyRecorded)
    ));
    assert_eq!(owner.snapshot(start).venue_requests_remaining, Some(8));
}

#[test]
fn abandoned_permits_complete_once_and_allow_out_of_order_tracking_to_compact() {
    let start = Instant::now();
    let owner = RestScheduler::binance_weighted(1_000, 100, 0).unwrap();
    let foreign = RestScheduler::binance_weighted(1_000, 100, 0).unwrap();
    let first = owner.acquire(RequestClass::MarketData, 1, start).unwrap();
    let mut later = Vec::new();
    for _ in 0..100 {
        later.push(owner.acquire(RequestClass::MarketData, 1, start).unwrap());
    }

    assert!(matches!(
        foreign.abandon_permit(&first),
        Err(BudgetError::ForeignPermit)
    ));
    owner.abandon_permit(&first).unwrap();
    assert!(matches!(
        owner.abandon_permit(&first),
        Err(BudgetError::PermitAlreadyRecorded)
    ));

    for permit in later.iter().rev() {
        owner
            .record_response_at(
                permit,
                &HeaderMap::new(),
                StatusCode::OK,
                None,
                start,
                1_700_000_000_000,
            )
            .unwrap();
    }
    assert_eq!(owner.snapshot(start).pending_response_completions, 0);
    assert!(matches!(
        owner.record_response_at(
            &later[0],
            &HeaderMap::new(),
            StatusCode::OK,
            None,
            start,
            1_700_000_000_000,
        ),
        Err(BudgetError::PermitAlreadyRecorded)
    ));
}

#[test]
fn missing_completion_tracking_is_bounded_and_fails_closed() {
    let start = Instant::now();
    let scheduler = RestScheduler::binance_weighted(10_000, 1, 0).unwrap();
    let mut permits = Vec::new();
    for _ in 0..4_097 {
        permits.push(
            scheduler
                .acquire(RequestClass::MarketData, 1, start)
                .unwrap(),
        );
    }
    for permit in &permits[1..=4_096] {
        scheduler
            .record_response_at(
                permit,
                &HeaderMap::new(),
                StatusCode::OK,
                None,
                start,
                1_700_000_000_000,
            )
            .unwrap();
    }
    assert_eq!(
        scheduler.snapshot(start).pending_response_completions,
        4_096
    );
    let before_rejected_acquire = scheduler.snapshot(start);
    assert!(matches!(
        scheduler.acquire(RequestClass::MarketData, 1, start),
        Err(BudgetError::CompletionTrackingExhausted)
    ));
    assert_eq!(scheduler.snapshot(start), before_rejected_acquire);

    scheduler.abandon_permit(&permits[0]).unwrap();
    assert_eq!(scheduler.snapshot(start).pending_response_completions, 0);
    scheduler
        .acquire(RequestClass::MarketData, 1, start)
        .unwrap();
}

#[test]
fn stale_bybit_response_cannot_restore_capacity() {
    let start = Instant::now();
    let scheduler = RestScheduler::bybit_endpoint(10).unwrap();
    let first = scheduler
        .acquire(RequestClass::MarketData, 1, start)
        .unwrap();
    let second = scheduler
        .acquire(RequestClass::MarketData, 1, start)
        .unwrap();
    let headers = |remaining: &'static str| {
        let mut headers = HeaderMap::new();
        headers.insert("x-bapi-limit", HeaderValue::from_static("10"));
        headers.insert("x-bapi-limit-status", HeaderValue::from_static(remaining));
        headers.insert(
            "x-bapi-limit-reset-timestamp",
            HeaderValue::from_static("1700000000000"),
        );
        headers
    };
    scheduler
        .record_response_at(
            &second,
            &headers("8"),
            StatusCode::OK,
            None,
            start,
            1_700_000_000_000,
        )
        .unwrap();
    scheduler
        .record_response_at(
            &first,
            &headers("9"),
            StatusCode::OK,
            None,
            start,
            1_700_000_000_000,
        )
        .unwrap();
    assert_eq!(scheduler.snapshot(start).venue_requests_remaining, Some(8));
    for _ in 0..8 {
        scheduler
            .acquire(RequestClass::MarketData, 1, start)
            .unwrap();
    }
    assert!(matches!(
        scheduler.acquire(RequestClass::MarketData, 1, start),
        Err(BudgetError::Exhausted)
    ));
}

#[test]
fn extreme_cooldowns_are_capped_and_never_shorten_an_existing_block() {
    let start = Instant::now();
    let scheduler = RestScheduler::bybit_endpoint(10).unwrap();
    let first = scheduler
        .acquire(RequestClass::MarketData, 1, start)
        .unwrap();
    let second = scheduler
        .acquire(RequestClass::MarketData, 1, start)
        .unwrap();
    let first_signal = scheduler
        .record_response_at(
            &first,
            &HeaderMap::new(),
            StatusCode::FORBIDDEN,
            None,
            start,
            0,
        )
        .unwrap()
        .unwrap();
    assert!(
        matches!(first_signal, HealthSignal::IpBanned { retry_after } if retry_after == Duration::from_secs(600))
    );

    let mut extreme = HeaderMap::new();
    extreme.insert(
        "retry-after",
        HeaderValue::from_static("18446744073709551615"),
    );
    let second_signal = scheduler
        .record_response_at(
            &second,
            &extreme,
            StatusCode::TOO_MANY_REQUESTS,
            None,
            start + Duration::from_secs(1),
            1_000,
        )
        .unwrap()
        .unwrap();
    assert!(
        matches!(second_signal, HealthSignal::RateLimited { retry_after } if retry_after == Duration::from_secs(259_200))
    );
    assert!(matches!(
        scheduler.acquire(
            RequestClass::MarketData,
            1,
            start + Duration::from_secs(259_199)
        ),
        Err(BudgetError::Blocked { .. })
    ));
}
