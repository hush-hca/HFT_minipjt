use std::{
    collections::{BTreeSet, VecDeque},
    sync::Mutex,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use reqwest::{StatusCode, header::HeaderMap};
use thiserror::Error;

const BINANCE_WINDOW: Duration = Duration::from_secs(60);
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);
const BYBIT_IP_COOLDOWN: Duration = Duration::from_secs(600);
const BYBIT_WINDOW: Duration = Duration::from_secs(1);
const MAX_COOLDOWN: Duration = Duration::from_secs(259_200);
const MAX_PENDING_COMPLETIONS: usize = 4_096;
static NEXT_SCHEDULER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RequestClass {
    MarketData,
    Account,
    Order,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SchedulerMode {
    BinanceWeightedMinute,
    BybitEndpointRollingSecond,
}

/// One acquired REST request obligation.
///
/// The caller must complete it exactly once with
/// [`RestScheduler::record_response_at`] or [`RestScheduler::abandon_permit`]
/// after transport, cancellation, or decoding failure.
#[derive(Debug, Eq, PartialEq)]
pub struct Permit {
    scheduler_id: u64,
    request_seq: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct BudgetSnapshot {
    pub mode: SchedulerMode,
    pub limit_per_minute: u32,
    pub used_weight: u32,
    pub used_account_weight: u32,
    pub reserved_order_weight: u32,
    pub account_headroom: u32,
    pub blocked_until: Option<Instant>,
    pub venue_request_limit: Option<u32>,
    pub venue_requests_remaining: Option<u32>,
    pub venue_reset_epoch_ms: Option<i64>,
    pub pending_response_completions: usize,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum HealthSignal {
    RateLimited { retry_after: Duration },
    IpBanned { retry_after: Duration },
}

#[derive(Debug, Error, Clone, Copy, Eq, PartialEq)]
pub enum BudgetError {
    #[error("REST weight is unknown; the endpoint poller must remain disabled")]
    UnknownWeight,
    #[error("REST weight arithmetic overflow")]
    WeightOverflow,
    #[error("invalid REST budget configuration")]
    InvalidConfiguration,
    #[error("Bybit endpoint schedulers count requests and require weight one")]
    InvalidBybitWeight,
    #[error("market-data request would consume reserved order/account headroom")]
    ReservedHeadroom,
    #[error("account request exceeds configured account headroom")]
    AccountHeadroom,
    #[error("REST budget is exhausted")]
    Exhausted,
    #[error("REST requests are blocked until {until:?}")]
    Blocked { until: Instant },
    #[error("invalid rate-limit response header {name}")]
    InvalidHeader { name: &'static str },
    #[error("response telemetry does not match scheduler mode")]
    ModeMismatch,
    #[error("REST scheduler mutex is poisoned")]
    Poisoned,
    #[error("REST scheduler time arithmetic overflow")]
    TimeOverflow,
    #[error("REST response permit belongs to another scheduler")]
    ForeignPermit,
    #[error("REST response permit was already recorded")]
    PermitAlreadyRecorded,
    #[error("REST response completion tracking is exhausted; abandon the missing earlier permit")]
    CompletionTrackingExhausted,
}

#[derive(Debug)]
pub struct RestScheduler {
    scheduler_id: u64,
    mode: Mode,
    state: Mutex<State>,
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Binance {
        limit_per_minute: u32,
        reserved_order_weight: u32,
        account_headroom: u32,
    },
    Bybit {
        configured_limit: u32,
    },
}

#[derive(Debug, Default)]
struct State {
    window_start: Option<Instant>,
    used_weight: u32,
    used_account_weight: u32,
    blocked_until: Option<Instant>,
    bybit_limit: Option<u32>,
    bybit_header_remaining: Option<u32>,
    bybit_reset_epoch_ms: Option<i64>,
    bybit_telemetry_expires_at: Option<Instant>,
    bybit_acquisitions: VecDeque<Instant>,
    next_request_seq: u64,
    last_response_seq: Option<u64>,
    response_frontier: u64,
    completed_out_of_order: BTreeSet<u64>,
}

impl RestScheduler {
    pub fn binance_weighted(
        limit_per_minute: u32,
        reserved_order_weight: u32,
        account_headroom: u32,
    ) -> Result<Self, BudgetError> {
        if limit_per_minute == 0
            || reserved_order_weight == 0
            || reserved_order_weight
                .checked_add(account_headroom)
                .is_none_or(|reserved| reserved >= limit_per_minute)
        {
            return Err(BudgetError::InvalidConfiguration);
        }
        Ok(Self {
            scheduler_id: next_scheduler_id()?,
            mode: Mode::Binance {
                limit_per_minute,
                reserved_order_weight,
                account_headroom,
            },
            state: Mutex::new(State::default()),
        })
    }

    pub fn bybit_endpoint(configured_limit: u32) -> Result<Self, BudgetError> {
        if configured_limit == 0 {
            return Err(BudgetError::InvalidConfiguration);
        }
        Ok(Self {
            scheduler_id: next_scheduler_id()?,
            mode: Mode::Bybit { configured_limit },
            state: Mutex::new(State::default()),
        })
    }

    pub fn acquire(
        &self,
        class: RequestClass,
        weight: u32,
        now: Instant,
    ) -> Result<Permit, BudgetError> {
        if weight == 0 {
            return Err(BudgetError::UnknownWeight);
        }
        let mut state = self.state.lock().map_err(|_| BudgetError::Poisoned)?;
        if state.completed_out_of_order.len() >= MAX_PENDING_COMPLETIONS {
            return Err(BudgetError::CompletionTrackingExhausted);
        }
        match self.mode {
            Mode::Binance { .. } => refresh_binance_window(&mut state, now),
            Mode::Bybit { configured_limit } => {
                refresh_bybit_state(&mut state, now, configured_limit)
            }
        }
        if let Some(until) = state.blocked_until {
            if now < until {
                return Err(BudgetError::Blocked { until });
            }
            state.blocked_until = None;
        }

        match self.mode {
            Mode::Binance {
                limit_per_minute,
                reserved_order_weight,
                account_headroom,
            } => acquire_binance(
                &mut state,
                class,
                weight,
                limit_per_minute,
                reserved_order_weight,
                account_headroom,
            )?,
            Mode::Bybit { configured_limit } => {
                if weight != 1 {
                    return Err(BudgetError::InvalidBybitWeight);
                }
                let effective_limit = state
                    .bybit_limit
                    .unwrap_or(configured_limit)
                    .min(configured_limit);
                let local_used = u32::try_from(state.bybit_acquisitions.len())
                    .map_err(|_| BudgetError::WeightOverflow)?;
                let local_remaining = effective_limit.saturating_sub(local_used);
                let remaining = state
                    .bybit_header_remaining
                    .unwrap_or(effective_limit)
                    .min(local_remaining);
                if remaining == 0 {
                    return Err(BudgetError::Exhausted);
                }
                state.bybit_acquisitions.push_back(now);
                if let Some(header_remaining) = state.bybit_header_remaining.as_mut() {
                    *header_remaining = header_remaining.saturating_sub(1);
                }
            }
        }
        let request_seq = state.next_request_seq;
        state.next_request_seq = state
            .next_request_seq
            .checked_add(1)
            .ok_or(BudgetError::WeightOverflow)?;
        Ok(Permit {
            scheduler_id: self.scheduler_id,
            request_seq,
        })
    }

    /// Completes a permit without response telemetry.
    ///
    /// Use this exactly once when the request was cancelled or failed before a
    /// usable response could be recorded. This releases response-order tracking;
    /// it does not refund venue rate-limit capacity.
    pub fn abandon_permit(&self, permit: &Permit) -> Result<(), BudgetError> {
        self.validate_permit_owner(permit)?;
        let mut state = self.state.lock().map_err(|_| BudgetError::Poisoned)?;
        mark_response_recorded(&mut state, permit.request_seq)
    }

    /// Completes a permit with venue response telemetry exactly once.
    ///
    /// Call [`Self::abandon_permit`] instead if transport, cancellation, or
    /// decoding fails before a usable response reaches this boundary.
    pub fn record_response_at(
        &self,
        permit: &Permit,
        headers: &HeaderMap,
        status: StatusCode,
        bybit_ret_code: Option<i64>,
        now: Instant,
        wall_clock_epoch_ms: i64,
    ) -> Result<Option<HealthSignal>, BudgetError> {
        self.validate_permit_owner(permit)?;
        let status_block =
            self.status_block(headers, status, bybit_ret_code, now, wall_clock_epoch_ms)?;
        let blocking = status_block.is_some();
        let telemetry = self.parse_telemetry(headers);

        let mut state = self.state.lock().map_err(|_| BudgetError::Poisoned)?;
        match self.mode {
            Mode::Binance { .. } => refresh_binance_window(&mut state, now),
            Mode::Bybit { configured_limit } => {
                refresh_bybit_state(&mut state, now, configured_limit)
            }
        }
        mark_response_recorded(&mut state, permit.request_seq)?;
        let is_fresh = state
            .last_response_seq
            .is_none_or(|last| permit.request_seq > last);
        match telemetry {
            Ok(telemetry) if is_fresh => {
                apply_telemetry(&mut state, telemetry, self.mode, now);
                state.last_response_seq = Some(permit.request_seq);
            }
            Ok(_) => {}
            Err(_) if !is_fresh => {}
            Err(error) => {
                state.last_response_seq = Some(permit.request_seq);
                if !blocking {
                    return Err(error);
                }
            }
        }

        let mut signal = None;
        if let Some((until, health)) = status_block {
            extend_block(&mut state.blocked_until, until);
            let retry_after = state
                .blocked_until
                .map(|blocked| blocked.saturating_duration_since(now))
                .unwrap_or(DEFAULT_RETRY_AFTER);
            signal = Some(match health {
                HealthSignal::RateLimited { .. } => HealthSignal::RateLimited { retry_after },
                HealthSignal::IpBanned { .. } => HealthSignal::IpBanned { retry_after },
            });
        }
        if matches!(self.mode, Mode::Bybit { .. }) && state.bybit_header_remaining == Some(0) {
            let until = instant_from_epoch(
                now,
                wall_clock_epoch_ms,
                state.bybit_reset_epoch_ms,
                DEFAULT_RETRY_AFTER,
            )?;
            extend_block(&mut state.blocked_until, until);
            let retry_after = state
                .blocked_until
                .map(|blocked| blocked.saturating_duration_since(now))
                .unwrap_or(DEFAULT_RETRY_AFTER);
            signal.get_or_insert(HealthSignal::RateLimited { retry_after });
        }
        Ok(signal)
    }

    pub fn snapshot(&self, now: Instant) -> BudgetSnapshot {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match self.mode {
            Mode::Binance { .. } => refresh_binance_window(&mut state, now),
            Mode::Bybit { configured_limit } => {
                refresh_bybit_state(&mut state, now, configured_limit)
            }
        }
        let (mode, limit, reserve, account) = match self.mode {
            Mode::Binance {
                limit_per_minute,
                reserved_order_weight,
                account_headroom,
            } => (
                SchedulerMode::BinanceWeightedMinute,
                limit_per_minute,
                reserved_order_weight,
                account_headroom,
            ),
            Mode::Bybit { .. } => (SchedulerMode::BybitEndpointRollingSecond, 0, 0, 0),
        };
        BudgetSnapshot {
            mode,
            limit_per_minute: limit,
            used_weight: state.used_weight,
            used_account_weight: state.used_account_weight,
            reserved_order_weight: reserve,
            account_headroom: account,
            blocked_until: state.blocked_until,
            venue_request_limit: state.bybit_limit,
            venue_requests_remaining: bybit_remaining(&state, self.mode),
            venue_reset_epoch_ms: state.bybit_reset_epoch_ms,
            pending_response_completions: state.completed_out_of_order.len(),
        }
    }

    fn validate_permit_owner(&self, permit: &Permit) -> Result<(), BudgetError> {
        if permit.scheduler_id == self.scheduler_id {
            Ok(())
        } else {
            Err(BudgetError::ForeignPermit)
        }
    }

    fn parse_telemetry(&self, headers: &HeaderMap) -> Result<Telemetry, BudgetError> {
        match self.mode {
            Mode::Binance { .. } => {
                if headers.contains_key("x-bapi-limit") {
                    return Err(BudgetError::ModeMismatch);
                }
                Ok(Telemetry::Binance {
                    used_weight: headers
                        .get("x-mbx-used-weight-1m")
                        .map(|raw| parse_u32_header(raw, "x-mbx-used-weight-1m"))
                        .transpose()?,
                })
            }
            Mode::Bybit { .. } => {
                if headers.contains_key("x-mbx-used-weight-1m") {
                    return Err(BudgetError::ModeMismatch);
                }
                let limit = headers
                    .get("x-bapi-limit")
                    .map(|raw| parse_u32_header(raw, "x-bapi-limit"))
                    .transpose()?;
                let remaining = headers
                    .get("x-bapi-limit-status")
                    .map(|raw| parse_u32_header(raw, "x-bapi-limit-status"))
                    .transpose()?;
                let reset_epoch_ms = headers
                    .get("x-bapi-limit-reset-timestamp")
                    .map(|raw| parse_i64_header(raw, "x-bapi-limit-reset-timestamp"))
                    .transpose()?;
                if limit.is_some() != remaining.is_some()
                    || limit.is_some() != reset_epoch_ms.is_some()
                    || limit
                        .zip(remaining)
                        .is_some_and(|(limit, remaining)| limit == 0 || remaining > limit)
                {
                    return Err(BudgetError::InvalidHeader {
                        name: "x-bapi-limit headers",
                    });
                }
                Ok(Telemetry::Bybit {
                    limit,
                    remaining,
                    reset_epoch_ms,
                })
            }
        }
    }

    fn status_block(
        &self,
        headers: &HeaderMap,
        status: StatusCode,
        bybit_ret_code: Option<i64>,
        now: Instant,
        wall_clock_epoch_ms: i64,
    ) -> Result<Option<(Instant, HealthSignal)>, BudgetError> {
        if matches!(self.mode, Mode::Bybit { .. }) && status == StatusCode::FORBIDDEN {
            let until = safe_add(now, BYBIT_IP_COOLDOWN)?;
            return Ok(Some((
                until,
                HealthSignal::IpBanned {
                    retry_after: BYBIT_IP_COOLDOWN,
                },
            )));
        }
        let is_limited = status == StatusCode::TOO_MANY_REQUESTS
            || (matches!(self.mode, Mode::Bybit { .. }) && bybit_ret_code == Some(10006));
        if !is_limited {
            return Ok(None);
        }
        let retry = parse_retry_after(headers, wall_clock_epoch_ms)
            .or_else(|| bybit_reset_delay(headers, wall_clock_epoch_ms))
            .unwrap_or(DEFAULT_RETRY_AFTER)
            .min(MAX_COOLDOWN);
        let until = safe_add(now, retry)?;
        Ok(Some((
            until,
            HealthSignal::RateLimited { retry_after: retry },
        )))
    }
}

#[derive(Debug, Clone, Copy)]
enum Telemetry {
    Binance {
        used_weight: Option<u32>,
    },
    Bybit {
        limit: Option<u32>,
        remaining: Option<u32>,
        reset_epoch_ms: Option<i64>,
    },
}

fn apply_telemetry(state: &mut State, telemetry: Telemetry, mode: Mode, now: Instant) {
    match telemetry {
        Telemetry::Binance {
            used_weight: Some(used),
        } => state.used_weight = state.used_weight.max(used),
        Telemetry::Binance { used_weight: None } => {}
        Telemetry::Bybit {
            limit,
            remaining,
            reset_epoch_ms,
        } => {
            let Mode::Bybit { configured_limit } = mode else {
                return;
            };
            let effective_limit = limit.unwrap_or(configured_limit).min(configured_limit);
            state.bybit_limit = Some(effective_limit);
            if let Some(remaining) = remaining {
                let current = state.bybit_header_remaining.unwrap_or(effective_limit);
                state.bybit_header_remaining = Some(current.min(remaining).min(effective_limit));
                state.bybit_telemetry_expires_at = now.checked_add(BYBIT_WINDOW);
            }
            state.bybit_reset_epoch_ms = reset_epoch_ms.or(state.bybit_reset_epoch_ms);
        }
    }
}

fn acquire_binance(
    state: &mut State,
    class: RequestClass,
    weight: u32,
    limit: u32,
    reserved: u32,
    account_headroom: u32,
) -> Result<(), BudgetError> {
    if weight > limit {
        return Err(BudgetError::WeightOverflow);
    }
    let next_used = state
        .used_weight
        .checked_add(weight)
        .ok_or(BudgetError::WeightOverflow)?;
    match class {
        RequestClass::MarketData => {
            let ceiling = limit
                .checked_sub(reserved)
                .and_then(|value| value.checked_sub(account_headroom))
                .ok_or(BudgetError::InvalidConfiguration)?;
            if next_used > ceiling {
                return Err(BudgetError::ReservedHeadroom);
            }
        }
        RequestClass::Account => {
            let next_account = state
                .used_account_weight
                .checked_add(weight)
                .ok_or(BudgetError::WeightOverflow)?;
            if next_account > account_headroom {
                return Err(BudgetError::AccountHeadroom);
            }
            if next_used > limit - reserved {
                return Err(BudgetError::ReservedHeadroom);
            }
            state.used_account_weight = next_account;
        }
        RequestClass::Order if next_used > limit => return Err(BudgetError::Exhausted),
        RequestClass::Order => {}
    }
    state.used_weight = next_used;
    Ok(())
}

fn refresh_binance_window(state: &mut State, now: Instant) {
    match state.window_start {
        Some(start) if now >= start && now.duration_since(start) >= BINANCE_WINDOW => {
            state.window_start = Some(now);
            state.used_weight = 0;
            state.used_account_weight = 0;
        }
        None => state.window_start = Some(now),
        _ => {}
    }
}

fn refresh_bybit_state(state: &mut State, now: Instant, configured_limit: u32) {
    while state
        .bybit_acquisitions
        .front()
        .is_some_and(|acquired| now >= *acquired && now.duration_since(*acquired) >= BYBIT_WINDOW)
    {
        state.bybit_acquisitions.pop_front();
    }
    if state
        .bybit_telemetry_expires_at
        .is_some_and(|expires_at| now >= expires_at)
    {
        state.bybit_header_remaining = None;
        state.bybit_telemetry_expires_at = None;
        state.bybit_reset_epoch_ms = None;
    }
    let effective_limit = state
        .bybit_limit
        .unwrap_or(configured_limit)
        .min(configured_limit);
    state.bybit_limit = Some(effective_limit);
}

fn bybit_remaining(state: &State, mode: Mode) -> Option<u32> {
    let Mode::Bybit { configured_limit } = mode else {
        return None;
    };
    let effective_limit = state
        .bybit_limit
        .unwrap_or(configured_limit)
        .min(configured_limit);
    let local_used = u32::try_from(state.bybit_acquisitions.len()).unwrap_or(u32::MAX);
    Some(
        state
            .bybit_header_remaining
            .unwrap_or(effective_limit)
            .min(effective_limit.saturating_sub(local_used)),
    )
}

fn mark_response_recorded(state: &mut State, request_seq: u64) -> Result<(), BudgetError> {
    if request_seq < state.response_frontier || state.completed_out_of_order.contains(&request_seq)
    {
        return Err(BudgetError::PermitAlreadyRecorded);
    }
    if request_seq == state.response_frontier {
        state.response_frontier = state
            .response_frontier
            .checked_add(1)
            .ok_or(BudgetError::WeightOverflow)?;
        while state
            .completed_out_of_order
            .remove(&state.response_frontier)
        {
            state.response_frontier = state
                .response_frontier
                .checked_add(1)
                .ok_or(BudgetError::WeightOverflow)?;
        }
    } else {
        if state.completed_out_of_order.len() >= MAX_PENDING_COMPLETIONS {
            return Err(BudgetError::CompletionTrackingExhausted);
        }
        state.completed_out_of_order.insert(request_seq);
    }
    Ok(())
}

fn next_scheduler_id() -> Result<u64, BudgetError> {
    NEXT_SCHEDULER_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| BudgetError::WeightOverflow)
}

fn extend_block(blocked_until: &mut Option<Instant>, candidate: Instant) {
    if blocked_until.is_none_or(|current| candidate > current) {
        *blocked_until = Some(candidate);
    }
}

fn instant_from_epoch(
    now: Instant,
    wall_clock_epoch_ms: i64,
    target_epoch_ms: Option<i64>,
    fallback: Duration,
) -> Result<Instant, BudgetError> {
    let delay = target_epoch_ms
        .and_then(|target| target.checked_sub(wall_clock_epoch_ms))
        .and_then(|millis| u64::try_from(millis).ok())
        .map(Duration::from_millis)
        .filter(|delay| !delay.is_zero())
        .unwrap_or(fallback)
        .min(MAX_COOLDOWN);
    safe_add(now, delay)
}

fn safe_add(now: Instant, delay: Duration) -> Result<Instant, BudgetError> {
    now.checked_add(delay.min(MAX_COOLDOWN))
        .ok_or(BudgetError::TimeOverflow)
}

fn bybit_reset_delay(headers: &HeaderMap, wall_clock_epoch_ms: i64) -> Option<Duration> {
    let target = headers
        .get("x-bapi-limit-reset-timestamp")?
        .to_str()
        .ok()?
        .parse::<i64>()
        .ok()?;
    let millis = u64::try_from(target.checked_sub(wall_clock_epoch_ms)?).ok()?;
    Some(Duration::from_millis(millis).min(MAX_COOLDOWN))
}

fn parse_retry_after(headers: &HeaderMap, wall_clock_epoch_ms: i64) -> Option<Duration> {
    let raw = headers.get("retry-after")?.to_str().ok()?;
    if let Ok(seconds) = raw.parse::<u64>() {
        return Some(Duration::from_secs(seconds.min(MAX_COOLDOWN.as_secs())));
    }
    let target_ms = parse_http_date_epoch_ms(raw)?;
    let millis = u64::try_from(target_ms.checked_sub(wall_clock_epoch_ms)?).ok()?;
    Some(Duration::from_millis(millis).min(MAX_COOLDOWN))
}

fn parse_http_date_epoch_ms(value: &str) -> Option<i64> {
    let (_, rest) = value.split_once(',')?;
    let mut fields = rest.split_whitespace();
    let day = fields.next()?.parse::<u32>().ok()?;
    let month = match fields.next()? {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    let year = fields.next()?.parse::<i64>().ok()?;
    let mut time = fields.next()?.split(':');
    let hour = time.next()?.parse::<i64>().ok()?;
    let minute = time.next()?.parse::<i64>().ok()?;
    let second = time.next()?.parse::<i64>().ok()?;
    if !(1970..=9999).contains(&year)
        || fields.next()? != "GMT"
        || fields.next().is_some()
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let seconds = days_from_civil(year, month, day)
        .checked_mul(86_400)?
        .checked_add(hour.checked_mul(3_600)?)?
        .checked_add(minute.checked_mul(60)?)?
        .checked_add(second)?;
    seconds.checked_mul(1_000)
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => 31,
    }
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let adjusted_year = year - i64::from(month <= 2);
    let era = if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    } / 400;
    let year_of_era = adjusted_year - era * 400;
    let shifted_month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn parse_u32_header(
    raw: &reqwest::header::HeaderValue,
    name: &'static str,
) -> Result<u32, BudgetError> {
    raw.to_str()
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or(BudgetError::InvalidHeader { name })
}

fn parse_i64_header(
    raw: &reqwest::header::HeaderValue,
    name: &'static str,
) -> Result<i64, BudgetError> {
    raw.to_str()
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or(BudgetError::InvalidHeader { name })
}
