use assert_cmd::Command;
use collector::CollectorApp;
use futures_util::{SinkExt, StreamExt};
use md_core::config::{AdapterConfig, CollectorConfig, RetryConfig};
use md_core::model::AdapterId;
use md_storage::validate_path;
use predicates::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

#[test]
fn cli_exposes_exact_command_surface() {
    Command::cargo_bin("collector")
        .unwrap()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("collect"))
        .stdout(predicate::str::contains("validate"))
        .stdout(predicate::str::contains("smoke"));

    Command::cargo_bin("collector")
        .unwrap()
        .args(["collect", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--config"))
        .stdout(predicate::str::contains("--strict-symbols"));

    Command::cargo_bin("collector")
        .unwrap()
        .args(["validate", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--path"))
        .stdout(predicate::str::contains("--json"));

    Command::cargo_bin("collector")
        .unwrap()
        .args(["smoke", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--config"))
        .stdout(predicate::str::contains("--duration"))
        .stdout(predicate::str::contains("--output"));
}

#[test]
fn validate_command_returns_nonzero_for_bad_dataset() {
    let root = tempfile::tempdir().unwrap();
    let bad = root.path().join("books.arrow");
    std::fs::write(&bad, b"not arrow").unwrap();

    Command::cargo_bin("collector")
        .unwrap()
        .args(["validate", "--path", bad.to_str().unwrap()])
        .assert()
        .failure()
        .stderr(predicate::str::contains("UNREADABLE_ARROW"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_local_exchanges_reconnect_finalize_and_validate_all_event_kinds() {
    let root = tempfile::tempdir().unwrap();
    let output = root.path().join("data");
    let fixtures = [
        FakeVenue {
            name: "upbit_spot",
            quote: "KRW",
            discovery: include_str!("../../md-exchanges/tests/fixtures/upbit_markets.json"),
            book: include_str!("../../md-exchanges/tests/fixtures/upbit_book.json"),
            trade: include_str!("../../md-exchanges/tests/fixtures/upbit_trade.json"),
            force_first_close: true,
            domestic: true,
        },
        FakeVenue {
            name: "bithumb_spot",
            quote: "KRW",
            discovery: include_str!("../../md-exchanges/tests/fixtures/bithumb_markets.json"),
            book: include_str!("../../md-exchanges/tests/fixtures/bithumb_book.json"),
            trade: include_str!("../../md-exchanges/tests/fixtures/bithumb_trade.json"),
            force_first_close: false,
            domestic: true,
        },
        FakeVenue {
            name: "binance_spot",
            quote: "USDT",
            discovery: include_str!("../../md-exchanges/tests/fixtures/binance_spot_markets.json"),
            book: include_str!("../../md-exchanges/tests/fixtures/binance_spot_book.json"),
            trade: include_str!("../../md-exchanges/tests/fixtures/binance_spot_trade.json"),
            force_first_close: false,
            domestic: false,
        },
        FakeVenue {
            name: "binance_usdm",
            quote: "USDT",
            discovery: include_str!("../../md-exchanges/tests/fixtures/binance_usdm_markets.json"),
            book: include_str!("../../md-exchanges/tests/fixtures/binance_usdm_book.json"),
            trade: include_str!("../../md-exchanges/tests/fixtures/binance_usdm_trade.json"),
            force_first_close: false,
            domestic: false,
        },
    ];

    let connections = Arc::new(AtomicUsize::new(0));
    let mut adapters = BTreeMap::new();
    for fixture in fixtures {
        let rest_url = spawn_http(fixture.discovery).await;
        let ws_url = spawn_websocket(fixture, Arc::clone(&connections)).await;
        adapters.insert(
            fixture.name.to_owned(),
            AdapterConfig {
                enabled: true,
                quote: fixture.quote.to_owned(),
                rest_url,
                websocket_url: ws_url,
                proactive_reconnect_secs: None,
            },
        );
    }
    let config = CollectorConfig {
        output_root: output.clone(),
        assets: vec!["BTC".to_owned()],
        strict_symbols: true,
        channel_capacity: 1_024,
        batch_rows: 8_192,
        flush_interval_ms: 50,
        enqueue_timeout_ms: 500,
        stats_interval_secs: 60,
        retry: RetryConfig {
            initial_ms: 5,
            max_ms: 10,
            reset_after_secs: 300,
        },
        adapters,
    };
    let app = CollectorApp::new(config).unwrap();
    let stats = app.stats();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(app.run(shutdown.clone()));

    let completed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let complete = [
                AdapterId::UpbitSpot,
                AdapterId::BithumbSpot,
                AdapterId::BinanceSpot,
                AdapterId::BinanceUsdm,
            ]
            .into_iter()
            .all(|adapter| {
                let snapshot = stats.snapshot(adapter);
                snapshot.books >= 1 && snapshot.trades >= 1
            });
            if complete
                && stats.snapshot(AdapterId::UpbitSpot).reconnects.peer_closed >= 1
                && connections.load(Ordering::SeqCst) >= 5
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        completed.is_ok(),
        "all fake adapters should publish and Upbit should reconnect: {:#?}",
        stats.snapshots()
    );
    shutdown.cancel();
    let report = run.await.unwrap().unwrap();

    let validation = validate_path(&output).unwrap();
    assert!(validation.is_valid(), "{:#?}", validation.errors);
    assert_eq!(validation.files, 8, "book and trade stream per adapter");
    assert!(validation.rows > 8);
    assert_eq!(
        report
            .adapters
            .iter()
            .map(|adapter| adapter.rejected_events)
            .sum::<u64>(),
        0
    );
    assert_eq!(
        report
            .adapters
            .iter()
            .map(|adapter| adapter.backpressure_disconnects)
            .sum::<u64>(),
        0
    );
    let upbit = report
        .adapters
        .iter()
        .find(|adapter| adapter.adapter == "upbit_spot")
        .unwrap();
    assert_eq!(upbit.reconnects.peer_closed, 1);
    assert!(connections.load(Ordering::SeqCst) >= 5);
}

#[derive(Clone, Copy)]
struct FakeVenue {
    name: &'static str,
    quote: &'static str,
    discovery: &'static str,
    book: &'static str,
    trade: &'static str,
    force_first_close: bool,
    domestic: bool,
}

async fn spawn_http(body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = vec![0_u8; 4096];
        let _ = stream.read(&mut request).await;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
    });
    format!("http://{address}/markets")
}

async fn spawn_websocket(fixture: FakeVenue, connections: Arc<AtomicUsize>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut venue_connection = 0_usize;
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            venue_connection += 1;
            connections.fetch_add(1, Ordering::SeqCst);
            let fixture = fixture;
            tokio::spawn(async move {
                let Ok(mut websocket) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                if fixture.domestic {
                    let subscription = websocket.next().await;
                    assert!(subscription.is_some(), "domestic adapter must subscribe");
                }
                let book = if fixture.name == "binance_usdm" {
                    fixture
                        .book
                        .replace("ethusdt", "btcusdt")
                        .replace("ETHUSDT", "BTCUSDT")
                } else {
                    fixture.book.to_owned()
                };
                let trade = if fixture.name == "binance_usdm" {
                    fixture
                        .trade
                        .replace("ethusdt", "btcusdt")
                        .replace("ETHUSDT", "BTCUSDT")
                } else {
                    fixture.trade.to_owned()
                };
                websocket
                    .send(Message::Text(freshen_timestamps(&book).into()))
                    .await
                    .unwrap();
                websocket
                    .send(Message::Text(freshen_timestamps(&trade).into()))
                    .await
                    .unwrap();
                if fixture.force_first_close && venue_connection == 1 {
                    websocket.send(Message::Close(None)).await.unwrap();
                    return;
                }
                while let Some(message) = websocket.next().await {
                    match message {
                        Ok(Message::Ping(payload)) => {
                            if websocket.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Ok(Message::Text(text)) if text == "PING" => {
                            if websocket.send(Message::Text("PONG".into())).await.is_err() {
                                break;
                            }
                        }
                        Ok(Message::Close(_)) | Err(_) => break,
                        _ => {}
                    }
                }
            });
        }
    });
    format!("ws://{address}/stream")
}

fn freshen_timestamps(payload: &str) -> String {
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    let mut value: serde_json::Value = serde_json::from_str(payload).unwrap();
    replace_timestamps(&mut value, now_us);
    serde_json::to_string(&value).unwrap()
}

fn replace_timestamps(value: &mut serde_json::Value, now_us: i64) {
    match value {
        serde_json::Value::Object(fields) => {
            for (name, value) in fields {
                if matches!(name.as_str(), "timestamp" | "trade_timestamp" | "E" | "T")
                    && value.is_number()
                {
                    let microsecond_source = value
                        .as_i64()
                        .is_some_and(|number| number > 1_000_000_000_000_000);
                    *value = serde_json::Value::from(if microsecond_source {
                        now_us
                    } else {
                        now_us / 1_000
                    });
                } else {
                    replace_timestamps(value, now_us);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                replace_timestamps(value, now_us);
            }
        }
        _ => {}
    }
}
