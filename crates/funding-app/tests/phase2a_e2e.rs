use std::sync::{Arc, Mutex};
use std::time::Duration;

use funding_app::{Phase2Collector, Phase2aStatus, SyntheticPublicSource};
use funding_core::config::FundingConfig;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn loopback_public_collection_drains_and_validates_every_family() {
    let root = tempfile::tempdir().unwrap();
    let mut config =
        FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();
    config.output_root = root.path().to_path_buf();
    config.batch_rows = 2;
    config.flush_interval_ms = 10;

    let source = SyntheticPublicSource::complete_fixture();
    let shutdown = CancellationToken::new();
    let trigger = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        trigger.cancel();
    });
    let report = Phase2Collector::with_source(config, source)
        .unwrap()
        .run(shutdown)
        .await
        .unwrap();

    assert_eq!(report.status, Phase2aStatus::Passed);
    assert!(report.missing_event_families.is_empty());
    assert!(report.missing_evidence.is_empty());
    assert_eq!(report.common_mainnet_symbols, ["BTC/USDT", "ETH/USDT"]);
    assert_eq!(report.common_testnet_symbols, ["BTC/USDT"]);
    assert!(report.reconnects >= 1);
    assert!(report.sequence_gaps >= 1);
    for family in [
        "instrument",
        "mark_index",
        "funding_estimate",
        "funding_settlement",
        "open_interest",
        "trader_ratio",
        "quote_conversion",
    ] {
        assert!(
            report
                .per_family
                .get(family)
                .is_some_and(|value| value.events > 0)
        );
    }
    assert_eq!(report.public_only_requests.credential_headers, 0);
    assert_eq!(report.public_only_requests.authenticated_requests, 0);
    assert_eq!(report.scheduler.pending_response_completions, 0);
    assert!(report.scheduler.abandoned_permits >= 1);
    assert!(
        md_storage::validate_path(&report.output_root)
            .unwrap()
            .is_valid()
    );
    assert!(report.report_path.ends_with("phase2a-report.json"));
}

#[tokio::test]
async fn producer_failure_drains_and_writes_a_failed_report() {
    let root = tempfile::tempdir().unwrap();
    let mut config =
        FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();
    config.output_root = root.path().join("producer-failure");
    let result =
        Phase2Collector::with_source(config.clone(), SyntheticPublicSource::failing_fixture())
            .unwrap()
            .run(CancellationToken::new())
            .await;
    assert!(result.is_err());
    let report: serde_json::Value = serde_json::from_slice(
        &std::fs::read(config.output_root.join("phase2a-report.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(report["status"], "failed");
    assert!(
        report["health_errors"]
            .as_array()
            .unwrap()
            .iter()
            .any(|error| error
                .as_str()
                .unwrap()
                .contains("injected producer failure"))
    );
}

#[tokio::test]
async fn cancelled_unresponsive_discovery_abandons_every_permit_and_reports() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held_connections = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            // Keep the accepted socket open and never produce an HTTP response.
            held_connections.push(stream);
        }
    });
    let root = tempfile::tempdir().unwrap();
    let mut config =
        FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();
    config.output_root = root.path().join("cancelled-discovery");
    config.assets = vec!["BTC".into()];
    for venue in config.venues.values_mut() {
        venue.mainnet.rest_url = format!("http://{address}");
        venue.testnet.rest_url = format!("http://{address}");
    }
    let shutdown = CancellationToken::new();
    let trigger = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        trigger.cancel();
    });
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        Phase2Collector::new(config.clone()).unwrap().run(shutdown),
    )
    .await
    .expect("duration cancellation must bound discovery");
    assert!(result.is_err());
    let report: serde_json::Value = serde_json::from_slice(
        &std::fs::read(config.output_root.join("phase2a-report.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(report["scheduler"]["pending_response_completions"], 0);
    assert_eq!(report["scheduler"]["abandoned_permits"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literal_loopback_venues_exercise_the_network_orchestrator_publicly() {
    let root = tempfile::tempdir().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let binance_rest = spawn_rest(Venue::Binance, Arc::clone(&requests)).await;
    let bybit_rest = spawn_rest(Venue::Bybit, Arc::clone(&requests)).await;
    let binance_ws = spawn_binance_ws(Arc::clone(&requests)).await;
    let bybit_ws = spawn_bybit_ws(Arc::clone(&requests)).await;
    let upbit_ws = spawn_domestic_ws(true, Arc::clone(&requests)).await;
    let bithumb_ws = spawn_domestic_ws(false, Arc::clone(&requests)).await;

    let mut config =
        FundingConfig::load(std::path::Path::new("../../config/funding.toml")).unwrap();
    config.output_root = root.path().join("network-data");
    config.assets = vec!["BTC".into()];
    config.batch_rows = 2;
    config.flush_interval_ms = 10;
    for venue in config.venues.values_mut() {
        let (rest, ws) = if venue.mainnet.rest_url.contains("bybit") {
            (&bybit_rest, &bybit_ws)
        } else {
            (&binance_rest, &binance_ws)
        };
        venue.mainnet.rest_url = rest.clone();
        venue.mainnet.public_websocket_url = ws.clone();
        venue.testnet.rest_url = rest.clone();
        venue.testnet.public_websocket_url = ws.clone();
    }
    config.validate().unwrap();
    let report_path = config.output_root.join("phase2a-report.json");

    let shutdown = CancellationToken::new();
    let trigger = shutdown.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(7)).await;
        trigger.cancel();
    });
    let report = Phase2Collector::new(config)
        .unwrap()
        .with_public_quote_websockets(upbit_ws, bithumb_ws)
        .run(shutdown)
        .await
        .unwrap_or_else(|error| {
            let persisted = std::fs::read_to_string(report_path).unwrap_or_default();
            panic!("collector failed: {error:#}; report={persisted}")
        });

    assert_eq!(report.common_mainnet_symbols, ["BTC/USDT"]);
    assert_eq!(report.common_testnet_symbols, ["BTC/USDT"]);
    assert!(report.reconnects >= 2, "{report:#?}");
    assert!(report.sequence_gaps >= 1, "{report:#?}");
    for family in [
        "instrument",
        "mark_index",
        "funding_estimate",
        "funding_settlement",
        "open_interest",
        "trader_ratio",
        "quote_conversion",
    ] {
        assert!(
            report
                .per_family
                .get(family)
                .is_some_and(|count| count.events > 0),
            "missing {family}: {report:#?}"
        );
    }
    assert!(
        md_storage::validate_path(&report.output_root)
            .unwrap()
            .is_valid()
    );
    assert_eq!(report.scheduler.pending_response_completions, 0);
    assert!(report.scheduler.abandoned_permits >= 1);
    assert!(report.scheduler.rate_limit_blocks >= 1);
    assert_eq!(
        report.per_family["funding_settlement"].events, 2,
        "repeated limit=1 settlements must deduplicate per venue"
    );
    assert_eq!(report.public_only_requests.credential_headers, 0);
    assert_eq!(report.public_only_requests.authenticated_requests, 0);
    assert!(report.public_only_requests.no_credentials_client_invariant);
    let requests = requests.lock().unwrap();
    assert!(
        report.public_only_requests.requests >= requests.len() as u64,
        "runtime audit missed physical attempts: report={report:#?}, captured={requests:#?}"
    );
    assert!(
        requests
            .iter()
            .filter(|line| line.contains("/v5/market/account-ratio"))
            .count()
            <= 2,
        "403 retry loop was not bounded: {requests:#?}"
    );
    assert!(report.health_errors.len() <= 64);
    for path in [
        "/fapi/v1/exchangeInfo",
        "/fapi/v1/fundingInfo",
        "/v5/market/instruments-info",
    ] {
        assert!(
            requests.iter().any(|line| line.contains(path)),
            "missing discovery request {path}: {requests:#?}"
        );
    }
    assert!(
        requests.iter().any(|line| line.starts_with("WS ")),
        "missing websocket handshake capture: {requests:#?}"
    );
    assert!(
        requests
            .iter()
            .any(|line| line.contains("/v5/market/instruments-info")
                && line.contains("cursor=page2")),
        "Bybit discovery did not schedule its second cursor page: {requests:#?}"
    );
    assert!(
        requests
            .iter()
            .any(|line| line.contains("SUBSCRIBE") || line.contains("publicTrade")),
        "missing websocket subscription capture: {requests:#?}"
    );
    assert!(
        requests
            .iter()
            .any(|line| line.contains("/fapi/v1/openInterest"))
    );
    assert!(
        requests
            .iter()
            .any(|line| line.contains("/v5/market/account-ratio"))
    );
    assert!(
        requests.iter().all(|request| {
            let lower = request.to_ascii_lowercase();
            !lower.contains("x-mbx-apikey")
                && !lower.contains("authorization:")
                && !lower.contains("api-key")
        }),
        "credential-bearing loopback request: {requests:#?}"
    );
}

#[derive(Clone, Copy)]
enum Venue {
    Binance,
    Bybit,
}

async fn spawn_rest(venue: Venue, requests: Arc<Mutex<Vec<String>>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let fail_binance_oi_once = Arc::new(std::sync::atomic::AtomicBool::new(matches!(
        venue,
        Venue::Binance
    )));
    let limit_bybit_ratio_once = Arc::new(std::sync::atomic::AtomicBool::new(matches!(
        venue,
        Venue::Bybit
    )));
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let requests = Arc::clone(&requests);
            let fail_binance_oi_once = Arc::clone(&fail_binance_oi_once);
            let limit_bybit_ratio_once = Arc::clone(&limit_bybit_ratio_once);
            tokio::spawn(async move {
                let mut buffer = vec![0_u8; 16_384];
                let size = stream.read(&mut buffer).await.unwrap_or(0);
                let request = String::from_utf8_lossy(&buffer[..size]).into_owned();
                requests.lock().unwrap().push(request.clone());
                let target = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or("/");
                if target.starts_with("/fapi/v1/openInterest")
                    && fail_binance_oi_once.swap(false, std::sync::atomic::Ordering::SeqCst)
                {
                    let _ = stream.shutdown().await;
                    return;
                }
                let non_json_rate_limit = target.starts_with("/v5/market/account-ratio")
                    && limit_bybit_ratio_once.swap(false, std::sync::atomic::Ordering::SeqCst);
                let body = if non_json_rate_limit {
                    "<html>rate limited</html>".to_owned()
                } else {
                    rest_body(venue, target)
                };
                let status = if non_json_rate_limit {
                    "429 Too Many Requests"
                } else {
                    "200 OK"
                };
                let retry_after = if non_json_rate_limit {
                    "Retry-After: 1\r\n"
                } else {
                    ""
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\n{retry_after}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    format!("http://{address}")
}

fn rest_body(venue: Venue, target: &str) -> String {
    if matches!(venue, Venue::Bybit) && target.starts_with("/v5/market/instruments-info") {
        let mut value: serde_json::Value = serde_json::from_str(include_str!(
            "../../md-exchanges/tests/fixtures/bybit_linear_instruments.json"
        ))
        .unwrap();
        if target.contains("cursor=page2") {
            value["result"]["list"] = serde_json::json!([]);
            value["result"]["nextPageCursor"] = serde_json::json!("");
        } else {
            value["result"]["nextPageCursor"] = serde_json::json!("page2");
        }
        return serde_json::to_string(&value).unwrap();
    }
    match (venue, target.split('?').next().unwrap_or(target)) {
        (Venue::Binance, "/fapi/v1/exchangeInfo") => include_str!("../../md-exchanges/tests/fixtures/binance_usdm_instruments_phase2.json").into(),
        (Venue::Binance, "/fapi/v1/fundingInfo") => r#"[{"symbol":"BTCUSDT","adjustedFundingRateCap":"0.005","adjustedFundingRateFloor":"-0.005","fundingIntervalHours":8}]"#.into(),
        (Venue::Binance, "/fapi/v1/openInterest") => fresh(include_str!("../../md-exchanges/tests/fixtures/binance_open_interest.json"), false),
        (Venue::Binance, "/fapi/v1/fundingRate") => single_row(include_str!("../../md-exchanges/tests/fixtures/binance_funding_history.json")),
        (Venue::Bybit, "/v5/market/open-interest") => single_row(&fresh(include_str!("../../md-exchanges/tests/fixtures/bybit_open_interest.json"), false)),
        (Venue::Bybit, "/v5/market/account-ratio") => single_row(&fresh(include_str!("../../md-exchanges/tests/fixtures/bybit_long_short.json"), false)),
        (Venue::Bybit, "/v5/market/funding/history") => single_row(include_str!("../../md-exchanges/tests/fixtures/bybit_funding_history.json")),
        _ => "{}".into(),
    }
}

fn single_row(payload: &str) -> String {
    let mut value: serde_json::Value = serde_json::from_str(payload).unwrap();
    if let Some(rows) = value.as_array_mut() {
        rows.truncate(1);
    }
    if let Some(rows) = value
        .pointer_mut("/result/list")
        .and_then(serde_json::Value::as_array_mut)
    {
        rows.truncate(1);
    }
    serde_json::to_string(&value).unwrap()
}

#[allow(clippy::result_large_err)] // tungstenite's handshake callback fixes this Result type.
async fn spawn_binance_ws(requests: Arc<Mutex<Vec<String>>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let requests = Arc::clone(&requests);
            tokio::spawn(async move {
                let path = Arc::new(Mutex::new(String::new()));
                let captured = Arc::clone(&path);
                let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(
                    stream,
                    move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                          response| {
                        *captured.lock().unwrap() = request.uri().to_string();
                        Ok(response)
                    },
                )
                .await
                else {
                    return;
                };
                let path = path.lock().unwrap().clone();
                requests.lock().unwrap().push(format!("WS {path}"));
                if path.starts_with("/stream") {
                    let book = fresh(
                        &include_str!("../../md-exchanges/tests/fixtures/binance_usdm_book.json")
                            .replace("ETHUSDT", "BTCUSDT")
                            .replace("ethusdt", "btcusdt"),
                        false,
                    );
                    let trade = fresh(
                        &include_str!("../../md-exchanges/tests/fixtures/binance_usdm_trade.json")
                            .replace("ETHUSDT", "BTCUSDT")
                            .replace("ethusdt", "btcusdt"),
                        false,
                    );
                    let _ = ws.send(Message::Text(book.into())).await;
                    let _ = ws.send(Message::Text(trade.into())).await;
                } else {
                    if let Some(Ok(Message::Text(subscription))) = ws.next().await {
                        requests.lock().unwrap().push(subscription.to_string());
                    }
                    let _ = ws
                        .send(Message::Text(
                            fresh(
                                include_str!(
                                    "../../md-exchanges/tests/fixtures/binance_mark_funding.json"
                                ),
                                true,
                            )
                            .into(),
                        ))
                        .await;
                }
                while let Some(message) = ws.next().await {
                    if matches!(message, Ok(Message::Close(_)) | Err(_)) {
                        break;
                    }
                }
            });
        }
    });
    format!("ws://{address}/ws")
}

async fn spawn_bybit_ws(requests: Arc<Mutex<Vec<String>>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let book_sessions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let requests = Arc::clone(&requests);
            let sessions = Arc::clone(&book_sessions);
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                let Some(Ok(Message::Text(subscription))) = ws.next().await else {
                    return;
                };
                requests.lock().unwrap().push(subscription.to_string());
                let _ = ws
                    .send(Message::Text(
                        r#"{"success":true,"ret_msg":"","op":"subscribe"}"#.into(),
                    ))
                    .await;
                if subscription.contains("orderbook.50") {
                    let session = sessions.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if session == 0 {
                        let _ = ws
                            .send(Message::Text(
                                fresh(
                                    include_str!(
                                        "../../md-exchanges/tests/fixtures/bybit_book_snapshot.json"
                                    ),
                                    false,
                                )
                                .into(),
                            ))
                            .await;
                        let _ = ws.send(Message::Close(None)).await;
                        return;
                    }
                    if session == 1 {
                        let delta = fresh(
                            include_str!("../../md-exchanges/tests/fixtures/bybit_book_delta.json"),
                            false,
                        );
                        for _ in 0..10 {
                            let _ = ws.send(Message::Text(delta.clone().into())).await;
                        }
                        return;
                    }
                    let _ = ws
                        .send(Message::Text(
                            fresh(
                                include_str!(
                                    "../../md-exchanges/tests/fixtures/bybit_book_snapshot.json"
                                ),
                                false,
                            )
                            .into(),
                        ))
                        .await;
                    let _ = ws
                        .send(Message::Text(
                            fresh(
                                include_str!(
                                    "../../md-exchanges/tests/fixtures/bybit_book_delta.json"
                                ),
                                false,
                            )
                            .into(),
                        ))
                        .await;
                    let _ = ws
                        .send(Message::Text(
                            fresh(
                                include_str!("../../md-exchanges/tests/fixtures/bybit_trade.json"),
                                false,
                            )
                            .into(),
                        ))
                        .await;
                } else {
                    let _ = ws
                        .send(Message::Text(
                            fresh(
                                include_str!(
                                    "../../md-exchanges/tests/fixtures/bybit_ticker_funding.json"
                                ),
                                true,
                            )
                            .into(),
                        ))
                        .await;
                    let _ = ws
                        .send(Message::Text(
                            fresh(
                                include_str!(
                                    "../../md-exchanges/tests/fixtures/bybit_ticker_delta.json"
                                ),
                                true,
                            )
                            .into(),
                        ))
                        .await;
                }
                while let Some(message) = ws.next().await {
                    if matches!(message, Ok(Message::Close(_)) | Err(_)) {
                        break;
                    }
                }
            });
        }
    });
    format!("ws://{address}/v5/public/linear")
}

async fn spawn_domestic_ws(upbit: bool, requests: Arc<Mutex<Vec<String>>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let requests = Arc::clone(&requests);
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                if let Some(Ok(Message::Text(subscription))) = ws.next().await {
                    requests.lock().unwrap().push(subscription.to_string());
                }
                let fixture = if upbit {
                    include_str!("../../md-exchanges/tests/fixtures/upbit_book.json")
                        .replace("KRW-BTC", "KRW-USDT")
                } else {
                    include_str!("../../md-exchanges/tests/fixtures/bithumb_book.json")
                        .replace("KRW-BTC", "KRW-USDT")
                };
                let _ = ws.send(Message::Text(fresh(&fixture, false).into())).await;
                while let Some(message) = ws.next().await {
                    if matches!(message, Ok(Message::Close(_)) | Err(_)) {
                        break;
                    }
                }
            });
        }
    });
    format!("ws://{address}/stream")
}

fn fresh(payload: &str, next_funding: bool) -> String {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let mut value: serde_json::Value = serde_json::from_str(payload).unwrap();
    fresh_value(&mut value, now_ms, next_funding);
    serde_json::to_string(&value).unwrap()
}

fn fresh_value(value: &mut serde_json::Value, now_ms: i64, next_funding: bool) {
    match value {
        serde_json::Value::Object(fields) => {
            for (name, value) in fields {
                if matches!(
                    name.as_str(),
                    "E" | "ts" | "time" | "timestamp" | "fundingTime" | "fundingRateTimestamp"
                ) {
                    let timestamp = if name == "timestamp"
                        && value
                            .as_i64()
                            .or_else(|| value.as_str().and_then(|raw| raw.parse().ok()))
                            .is_some_and(|original| original > 100_000_000_000_000)
                    {
                        now_ms.saturating_mul(1_000)
                    } else {
                        now_ms
                    };
                    let replacement = if value.is_string() {
                        serde_json::Value::String(timestamp.to_string())
                    } else {
                        serde_json::Value::from(timestamp)
                    };
                    *value = replacement;
                } else if next_funding && matches!(name.as_str(), "T" | "nextFundingTime") {
                    let future = now_ms + 3_600_000;
                    *value = if value.is_string() {
                        serde_json::Value::String(future.to_string())
                    } else {
                        serde_json::Value::from(future)
                    };
                } else {
                    fresh_value(value, now_ms, next_funding);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                fresh_value(value, now_ms, next_funding);
            }
        }
        _ => {}
    }
}
