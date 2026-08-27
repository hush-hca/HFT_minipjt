use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use md_core::model::{
    AdapterId, CanonicalSymbol, EventMeta, NormalizedEvent, TakerSide, TimestampPrecision,
    TradeTick,
};
use md_exchanges::{
    AdapterRuntime, Backoff, BybitLinearParser, FrameParser, GapReason, NoopRuntimeStats,
    ParseError, ReconnectReason, RejectReason, RuntimeOptions, RuntimeStats,
    run_supervised_with_options,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[test]
fn capped_backoff_resets_after_healthy_window() {
    let mut backoff = Backoff::without_jitter(1_000, 30_000, 300_000);
    assert_eq!(backoff.next_delay_ms(0), 1_000);
    assert_eq!(backoff.next_delay_ms(0), 2_000);
    assert_eq!(backoff.next_delay_ms(0), 4_000);
    assert_eq!(backoff.next_delay_ms(0), 8_000);
    assert_eq!(backoff.next_delay_ms(0), 16_000);
    assert_eq!(backoff.next_delay_ms(0), 30_000);
    assert_eq!(backoff.next_delay_ms(0), 30_000);
    assert_eq!(backoff.next_delay_ms(301_000), 1_000);
}

#[test]
fn jitter_is_injected_and_cannot_exceed_the_cap() {
    let mut backoff = Backoff::with_jitter(1_000, 30_000, 300_000, |delay| delay / 2);
    assert_eq!(backoff.next_delay_ms(0), 1_500);

    for _ in 0..8 {
        assert!(backoff.next_delay_ms(0) <= 30_000);
    }
}

#[derive(Default)]
struct RecordingParser {
    frames: Mutex<Vec<Vec<u8>>>,
    resets: AtomicUsize,
}

struct AlwaysErrorParser;

impl FrameParser for AlwaysErrorParser {
    fn parse(&self, _frame: &mut [u8], _recv_us: i64) -> Result<Vec<NormalizedEvent>, ParseError> {
        Err(ParseError::UnknownEventType {
            event_type: "test-error".to_owned(),
        })
    }
}

struct TwoTradeParser;

impl FrameParser for TwoTradeParser {
    fn parse(&self, frame: &mut [u8], recv_us: i64) -> Result<Vec<NormalizedEvent>, ParseError> {
        Ok((0..2)
            .map(|index| {
                NormalizedEvent::Trade(TradeTick {
                    meta: EventMeta {
                        schema_version: 1,
                        event_id: Uuid::now_v7(),
                        adapter: AdapterId::UpbitSpot,
                        symbol: CanonicalSymbol::new("BTC", "KRW"),
                        source_symbol: "KRW-BTC".to_owned(),
                        source_stream: "trade".to_owned(),
                        source_sequence: Some(index),
                        exchange_event_ts_us: Some(recv_us),
                        exchange_trade_ts_us: Some(recv_us),
                        event_ts_precision: TimestampPrecision::Microsecond,
                        trade_ts_precision: TimestampPrecision::Microsecond,
                        local_recv_ts_us: recv_us,
                        raw_size_bytes: u32::try_from(frame.len()).unwrap(),
                    },
                    trade_id: index.to_string(),
                    price: 100_000_000_000_000_000_000,
                    quantity: 1_000_000_000_000_000_000,
                    taker_side: TakerSide::Buy,
                })
            })
            .collect())
    }
}

#[derive(Default)]
struct RecordingStats {
    reconnects: Mutex<Vec<ReconnectReason>>,
    rejects: Mutex<Vec<RejectReason>>,
    gaps: Mutex<Vec<GapReason>>,
    closed_gaps: AtomicUsize,
    parse_errors: AtomicUsize,
    backpressure_disconnects: AtomicUsize,
    delivered_trades: AtomicUsize,
}

impl RuntimeStats for RecordingStats {
    fn on_frame(&self, _adapter: AdapterId, _bytes: u32) {}
    fn on_events(&self, _adapter: AdapterId, _books: u64, _book_rows: u64, trades: u64) {
        self.delivered_trades
            .fetch_add(trades as usize, Ordering::SeqCst);
    }
    fn on_parse_error(&self, _adapter: AdapterId) {
        self.parse_errors.fetch_add(1, Ordering::SeqCst);
    }
    fn on_receive_lag_us(&self, _adapter: AdapterId, _lag_us: u64) {}
    fn on_queue_depth(&self, _adapter: AdapterId, _depth: usize) {}
    fn on_reconnect(&self, _adapter: AdapterId, reason: ReconnectReason) {
        self.reconnects.lock().unwrap().push(reason);
    }
    fn on_rejected_event(&self, _adapter: AdapterId, reason: RejectReason) {
        self.rejects.lock().unwrap().push(reason);
    }
    fn on_backpressure_disconnect(&self, _adapter: AdapterId) {
        self.backpressure_disconnects.fetch_add(1, Ordering::SeqCst);
    }
    fn open_gap(&self, _adapter: AdapterId, reason: GapReason) {
        self.gaps.lock().unwrap().push(reason);
    }
    fn close_gap(&self, _adapter: AdapterId) {
        self.closed_gaps.fetch_add(1, Ordering::SeqCst);
    }
}

fn fast_options() -> RuntimeOptions {
    let mut options = RuntimeOptions::default().with_jitter(|_| 0);
    options.initial_backoff = Duration::from_millis(1);
    options.max_backoff = Duration::from_millis(2);
    options.idle_keepalive = Duration::from_secs(30);
    options.idle_timeout = Duration::from_secs(60);
    options.send_timeout = Duration::from_millis(20);
    options
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

impl FrameParser for RecordingParser {
    fn parse(&self, frame: &mut [u8], _recv_us: i64) -> Result<Vec<NormalizedEvent>, ParseError> {
        self.frames.lock().unwrap().push(frame.to_vec());
        Ok(Vec::new())
    }

    fn reset(&self) {
        self.resets.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn reconnect_resends_subscription_and_replies_to_ping() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let subscriptions = Arc::new(Mutex::new(Vec::new()));
    let server_subscriptions = Arc::clone(&subscriptions);

    let server = tokio::spawn(async move {
        for connection_number in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(stream).await.unwrap();
            let subscription = websocket.next().await.unwrap().unwrap();
            server_subscriptions
                .lock()
                .unwrap()
                .push(subscription.into_text().unwrap().to_string());

            if connection_number == 0 {
                websocket
                    .send(Message::Ping(vec![1, 2, 3].into()))
                    .await
                    .unwrap();
                let pong = websocket.next().await.unwrap().unwrap();
                assert_eq!(pong, Message::Pong(vec![1, 2, 3].into()));
                websocket.close(None).await.unwrap();
            }
        }
    });

    let shutdown = CancellationToken::new();
    let parser = Arc::new(RecordingParser::default());
    let runtime = AdapterRuntime::new(
        AdapterId::UpbitSpot,
        format!("ws://{address}"),
        r#"[{"ticket":"test"}]"#,
        Duration::from_secs(60),
        parser.clone(),
    );
    let (tx, _rx) = mpsc::channel(4);
    let options = fast_options();

    let task = tokio::spawn(run_supervised_with_options(
        runtime,
        tx,
        shutdown.clone(),
        Arc::new(NoopRuntimeStats),
        options,
    ));

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if subscriptions.lock().unwrap().len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    task.await.unwrap().unwrap();
    server.await.unwrap();

    assert_eq!(parser.resets.load(Ordering::SeqCst), 2);

    assert_eq!(
        subscriptions.lock().unwrap().as_slice(),
        &[r#"[{"ticket":"test"}]"#, r#"[{"ticket":"test"}]"#]
    );
}

struct ReconnectRequiredParser;

impl FrameParser for ReconnectRequiredParser {
    fn parse(&self, _frame: &mut [u8], _recv_us: i64) -> Result<Vec<NormalizedEvent>, ParseError> {
        Err(ParseError::SnapshotRequired)
    }
}

#[tokio::test]
async fn sequence_state_error_forces_reconnect_after_one_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        websocket.next().await.unwrap().unwrap();
        websocket.send(Message::Text("delta".into())).await.unwrap();
        while websocket.next().await.is_some() {}
    });
    let stats = Arc::new(RecordingStats::default());
    let shutdown = CancellationToken::new();
    let (tx, _rx) = mpsc::channel(1);
    let task = tokio::spawn(run_supervised_with_options(
        AdapterRuntime::new(
            AdapterId::BybitLinear,
            format!("ws://{address}"),
            "subscription",
            Duration::from_secs(60),
            Arc::new(ReconnectRequiredParser),
        ),
        tx,
        shutdown.clone(),
        stats.clone(),
        fast_options(),
    ));
    wait_until(|| {
        stats
            .reconnects
            .lock()
            .unwrap()
            .contains(&ReconnectReason::Protocol)
    })
    .await;
    assert_eq!(stats.parse_errors.load(Ordering::SeqCst), 1);
    shutdown.cancel();
    task.await.unwrap().unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn bybit_success_ack_is_not_rejected_and_failure_reconnects_immediately() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        websocket.next().await.unwrap().unwrap();
        websocket
            .send(Message::Text(
                r#"{"success":true,"ret_msg":"","op":"subscribe","conn_id":"conn"}"#.into(),
            ))
            .await
            .unwrap();
        websocket
            .send(Message::Text(
                r#"{"success":false,"ret_msg":"invalid topic","op":"subscribe","conn_id":"conn"}"#
                    .into(),
            ))
            .await
            .unwrap();
        while websocket.next().await.is_some() {}
    });
    let stats = Arc::new(RecordingStats::default());
    let shutdown = CancellationToken::new();
    let (tx, _rx) = mpsc::channel(1);
    let task = tokio::spawn(run_supervised_with_options(
        AdapterRuntime::new(
            AdapterId::BybitLinear,
            format!("ws://{address}"),
            "subscription",
            Duration::from_secs(60),
            Arc::new(BybitLinearParser::new(CanonicalSymbol::new("BTC", "USDT"))),
        ),
        tx,
        shutdown.clone(),
        stats.clone(),
        fast_options(),
    ));
    wait_until(|| {
        stats
            .reconnects
            .lock()
            .unwrap()
            .contains(&ReconnectReason::Protocol)
    })
    .await;
    assert_eq!(stats.parse_errors.load(Ordering::SeqCst), 1);
    assert_eq!(
        stats.rejects.lock().unwrap().as_slice(),
        &[RejectReason::Parse]
    );
    shutdown.cancel();
    task.await.unwrap().unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn ten_consecutive_parse_errors_trigger_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stats = Arc::new(RecordingStats::default());
    let server_stats = Arc::clone(&stats);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        websocket.next().await.unwrap().unwrap();
        for _ in 0..10 {
            websocket.send(Message::Text("{}".into())).await.unwrap();
        }
        wait_until(|| server_stats.parse_errors.load(Ordering::SeqCst) == 10).await;
    });

    let shutdown = CancellationToken::new();
    let (tx, _rx) = mpsc::channel(4);
    let task = tokio::spawn(run_supervised_with_options(
        AdapterRuntime::new(
            AdapterId::BinanceSpot,
            format!("ws://{address}"),
            "streams=test",
            Duration::from_secs(60),
            Arc::new(AlwaysErrorParser),
        ),
        tx,
        shutdown.clone(),
        stats.clone(),
        fast_options(),
    ));

    wait_until(|| {
        stats
            .reconnects
            .lock()
            .unwrap()
            .contains(&ReconnectReason::ParseThreshold)
    })
    .await;
    assert_eq!(stats.parse_errors.load(Ordering::SeqCst), 10);
    assert_eq!(stats.rejects.lock().unwrap().len(), 10);
    shutdown.cancel();
    task.await.unwrap().unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn proactive_rotation_reconnects_and_resubscribes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let subscriptions = Arc::new(AtomicUsize::new(0));
    let server_count = Arc::clone(&subscriptions);
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut websocket = accept_async(stream).await.unwrap();
            websocket.next().await.unwrap().unwrap();
            server_count.fetch_add(1, Ordering::SeqCst);
            while websocket.next().await.is_some() {}
        }
    });

    let stats = Arc::new(RecordingStats::default());
    let shutdown = CancellationToken::new();
    let (tx, _rx) = mpsc::channel(4);
    let task = tokio::spawn(run_supervised_with_options(
        AdapterRuntime::new(
            AdapterId::BinanceUsdm,
            format!("ws://{address}"),
            "streams=test",
            Duration::from_millis(20),
            Arc::new(RecordingParser::default()),
        ),
        tx,
        shutdown.clone(),
        stats.clone(),
        fast_options(),
    ));

    wait_until(|| subscriptions.load(Ordering::SeqCst) == 2).await;
    assert!(
        stats
            .reconnects
            .lock()
            .unwrap()
            .contains(&ReconnectReason::ProactiveRotation)
    );
    shutdown.cancel();
    task.await.unwrap().unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn domestic_idle_keepalive_is_application_ping() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        websocket.next().await.unwrap().unwrap();
        let keepalive = websocket.next().await.unwrap().unwrap();
        assert_eq!(keepalive, Message::Text("PING".into()));
    });

    let shutdown = CancellationToken::new();
    let (tx, _rx) = mpsc::channel(4);
    let mut options = fast_options();
    options.idle_keepalive = Duration::from_millis(10);
    options.idle_timeout = Duration::from_millis(50);
    let task = tokio::spawn(run_supervised_with_options(
        AdapterRuntime::new(
            AdapterId::BithumbSpot,
            format!("ws://{address}"),
            "subscription",
            Duration::from_secs(60),
            Arc::new(RecordingParser::default()),
        ),
        tx,
        shutdown.clone(),
        Arc::new(NoopRuntimeStats),
        options,
    ));

    server.await.unwrap();
    shutdown.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn bounded_event_send_reports_backpressure_without_waiting_five_seconds() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        websocket.next().await.unwrap().unwrap();
        websocket
            .send(Message::Text("two-events".into()))
            .await
            .unwrap();
        while websocket.next().await.is_some() {}
    });

    let stats = Arc::new(RecordingStats::default());
    let shutdown = CancellationToken::new();
    let (tx, _rx) = mpsc::channel(1);
    let task = tokio::spawn(run_supervised_with_options(
        AdapterRuntime::new(
            AdapterId::UpbitSpot,
            format!("ws://{address}"),
            "subscription",
            Duration::from_secs(60),
            Arc::new(TwoTradeParser),
        ),
        tx,
        shutdown.clone(),
        stats.clone(),
        fast_options(),
    ));

    wait_until(|| stats.backpressure_disconnects.load(Ordering::SeqCst) == 1).await;
    assert!(
        stats
            .rejects
            .lock()
            .unwrap()
            .contains(&RejectReason::Backpressure)
    );
    assert!(
        stats
            .gaps
            .lock()
            .unwrap()
            .contains(&GapReason::Backpressure)
    );
    assert_eq!(stats.delivered_trades.load(Ordering::SeqCst), 1);
    shutdown.cancel();
    task.await.unwrap().unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn cancellation_ends_an_idle_session_successfully() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let connected = Arc::new(AtomicUsize::new(0));
    let server_connected = Arc::clone(&connected);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        websocket.next().await.unwrap().unwrap();
        server_connected.store(1, Ordering::SeqCst);
        while websocket.next().await.is_some() {}
    });

    let shutdown = CancellationToken::new();
    let (tx, _rx) = mpsc::channel(1);
    let task = tokio::spawn(run_supervised_with_options(
        AdapterRuntime::new(
            AdapterId::BinanceSpot,
            format!("ws://{address}"),
            "subscription",
            Duration::from_secs(60),
            Arc::new(RecordingParser::default()),
        ),
        tx,
        shutdown.clone(),
        Arc::new(NoopRuntimeStats),
        fast_options(),
    ));

    wait_until(|| connected.load(Ordering::SeqCst) == 1).await;
    shutdown.cancel();
    task.await.unwrap().unwrap();
    server.await.unwrap();
}
