use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use md_core::model::{AdapterId, NormalizedEvent};
use rand::RngExt;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use tracing::debug;
use url::Url;

use crate::{ParseError, backoff::Backoff};

const PARSE_ERROR_THRESHOLD: u8 = 10;

pub trait FrameParser: Send + Sync {
    fn parse(&self, frame: &mut [u8], recv_us: i64) -> Result<Vec<NormalizedEvent>, ParseError>;
}

macro_rules! parser_adapter {
    ($name:ident, $function:path) => {
        #[derive(Debug, Clone, Copy, Default)]
        pub struct $name;

        impl FrameParser for $name {
            fn parse(
                &self,
                frame: &mut [u8],
                recv_us: i64,
            ) -> Result<Vec<NormalizedEvent>, ParseError> {
                $function(frame, recv_us)
            }
        }
    };
}

parser_adapter!(UpbitParser, crate::upbit::parse_frame);
parser_adapter!(BithumbParser, crate::bithumb::parse_frame);
parser_adapter!(BinanceSpotParser, crate::binance_spot::parse_frame);
parser_adapter!(BinanceUsdmParser, crate::binance_usdm::parse_frame);

#[derive(Clone)]
pub struct AdapterRuntime {
    pub id: AdapterId,
    pub websocket_url: String,
    pub subscription: String,
    pub proactive_reconnect: Duration,
    pub parser: Arc<dyn FrameParser>,
}

impl AdapterRuntime {
    pub fn new(
        id: AdapterId,
        websocket_url: impl Into<String>,
        subscription: impl Into<String>,
        proactive_reconnect: Duration,
        parser: Arc<dyn FrameParser>,
    ) -> Self {
        Self {
            id,
            websocket_url: websocket_url.into(),
            subscription: subscription.into(),
            proactive_reconnect,
            parser,
        }
    }
}

impl fmt::Debug for AdapterRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdapterRuntime")
            .field("id", &self.id)
            .field("websocket_url", &self.websocket_url)
            .field("subscription", &self.subscription)
            .field("proactive_reconnect", &self.proactive_reconnect)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ReconnectReason {
    PeerClosed,
    IdleTimeout,
    Protocol,
    ParseThreshold,
    ProactiveRotation,
    Backpressure,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RejectReason {
    Parse,
    Validation,
    Backpressure,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum GapReason {
    Disconnected,
    Backpressure,
}

pub trait RuntimeStats: Send + Sync {
    fn on_frame(&self, adapter: AdapterId, bytes: u32);
    fn on_events(&self, adapter: AdapterId, books: u64, book_rows: u64, trades: u64);
    fn on_parse_error(&self, adapter: AdapterId);
    fn on_receive_lag_us(&self, adapter: AdapterId, lag_us: u64);
    fn on_queue_depth(&self, adapter: AdapterId, depth: usize);
    fn on_reconnect(&self, adapter: AdapterId, reason: ReconnectReason);
    fn on_rejected_event(&self, adapter: AdapterId, reason: RejectReason);
    fn on_backpressure_disconnect(&self, adapter: AdapterId);
    fn open_gap(&self, adapter: AdapterId, reason: GapReason);
    fn close_gap(&self, adapter: AdapterId);
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopRuntimeStats;

impl RuntimeStats for NoopRuntimeStats {
    fn on_frame(&self, _adapter: AdapterId, _bytes: u32) {}
    fn on_events(&self, _adapter: AdapterId, _books: u64, _book_rows: u64, _trades: u64) {}
    fn on_parse_error(&self, _adapter: AdapterId) {}
    fn on_receive_lag_us(&self, _adapter: AdapterId, _lag_us: u64) {}
    fn on_queue_depth(&self, _adapter: AdapterId, _depth: usize) {}
    fn on_reconnect(&self, _adapter: AdapterId, _reason: ReconnectReason) {}
    fn on_rejected_event(&self, _adapter: AdapterId, _reason: RejectReason) {}
    fn on_backpressure_disconnect(&self, _adapter: AdapterId) {}
    fn open_gap(&self, _adapter: AdapterId, _reason: GapReason) {}
    fn close_gap(&self, _adapter: AdapterId) {}
}

#[derive(Clone)]
pub struct RuntimeOptions {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub healthy_reset: Duration,
    pub idle_keepalive: Duration,
    pub idle_timeout: Duration,
    pub send_timeout: Duration,
    jitter: Arc<dyn Fn(u64) -> u64 + Send + Sync>,
}

impl RuntimeOptions {
    pub fn with_jitter(mut self, jitter: impl Fn(u64) -> u64 + Send + Sync + 'static) -> Self {
        self.jitter = Arc::new(jitter);
        self
    }

    fn validate(&self) -> Result<(), RuntimeError> {
        if self.initial_backoff.is_zero()
            || self.max_backoff < self.initial_backoff
            || self.healthy_reset.is_zero()
            || self.idle_keepalive.is_zero()
            || self.idle_timeout < self.idle_keepalive
            || self.send_timeout.is_zero()
        {
            return Err(RuntimeError::InvalidConfig(
                "runtime durations are zero or inconsistently ordered".to_owned(),
            ));
        }
        Ok(())
    }
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            healthy_reset: Duration::from_secs(5 * 60),
            idle_keepalive: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(90),
            send_timeout: Duration::from_secs(5),
            jitter: Arc::new(|base_ms| {
                let maximum = (base_ms / 5).max(1);
                rand::rng().random_range(0..=maximum)
            }),
        }
    }
}

impl fmt::Debug for RuntimeOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeOptions")
            .field("initial_backoff", &self.initial_backoff)
            .field("max_backoff", &self.max_backoff)
            .field("healthy_reset", &self.healthy_reset)
            .field("idle_keepalive", &self.idle_keepalive)
            .field("idle_timeout", &self.idle_timeout)
            .field("send_timeout", &self.send_timeout)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("invalid WebSocket URL `{url}`: {source}")]
    InvalidUrl {
        url: String,
        #[source]
        source: url::ParseError,
    },
    #[error("invalid WebSocket runtime configuration: {0}")]
    InvalidConfig(String),
    #[error("normalized-event receiver closed")]
    EventReceiverClosed,
    #[error("system clock is before Unix epoch")]
    ClockBeforeEpoch,
}

#[derive(Debug)]
enum SessionEnd {
    Cancelled,
    Reconnect {
        reason: ReconnectReason,
        healthy_for: Duration,
    },
}

pub async fn run_supervised(
    runtime: AdapterRuntime,
    tx: mpsc::Sender<NormalizedEvent>,
    shutdown: CancellationToken,
    stats: Arc<dyn RuntimeStats>,
) -> Result<(), RuntimeError> {
    run_supervised_with_options(runtime, tx, shutdown, stats, RuntimeOptions::default()).await
}

pub async fn run_supervised_with_options(
    runtime: AdapterRuntime,
    tx: mpsc::Sender<NormalizedEvent>,
    shutdown: CancellationToken,
    stats: Arc<dyn RuntimeStats>,
    options: RuntimeOptions,
) -> Result<(), RuntimeError> {
    let url = Url::parse(&runtime.websocket_url).map_err(|source| RuntimeError::InvalidUrl {
        url: runtime.websocket_url.clone(),
        source,
    })?;
    if !matches!(url.scheme(), "ws" | "wss") || url.host_str().is_none() {
        return Err(RuntimeError::InvalidConfig(
            "WebSocket URL must use ws/wss and include a host".to_owned(),
        ));
    }
    if runtime.proactive_reconnect.is_zero() {
        return Err(RuntimeError::InvalidConfig(
            "proactive reconnect must be positive".to_owned(),
        ));
    }
    options.validate()?;

    let jitter = Arc::clone(&options.jitter);
    let mut backoff = Backoff::with_jitter(
        duration_ms(options.initial_backoff),
        duration_ms(options.max_backoff),
        duration_ms(options.healthy_reset),
        move |delay| jitter(delay),
    );

    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let ending = run_session(&runtime, &tx, &shutdown, stats.as_ref(), &options).await;
        let (reason, healthy_for) = match ending {
            Ok(SessionEnd::Cancelled) => return Ok(()),
            Ok(SessionEnd::Reconnect {
                reason,
                healthy_for,
            }) => (reason, healthy_for),
            Err(RuntimeError::EventReceiverClosed) => {
                return Err(RuntimeError::EventReceiverClosed);
            }
            Err(error @ (RuntimeError::InvalidUrl { .. } | RuntimeError::InvalidConfig(_))) => {
                return Err(error);
            }
            Err(RuntimeError::ClockBeforeEpoch) => return Err(RuntimeError::ClockBeforeEpoch),
        };

        stats.on_reconnect(runtime.id, reason);
        if reason != ReconnectReason::Backpressure {
            stats.open_gap(runtime.id, GapReason::Disconnected);
        }
        let healthy_ms = duration_ms(healthy_for);
        let delay = Duration::from_millis(backoff.next_delay_ms(healthy_ms));
        debug!(adapter = ?runtime.id, ?reason, ?delay, "reconnecting WebSocket adapter");
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            () = tokio::time::sleep(delay) => {}
        }
    }
}

async fn run_session(
    runtime: &AdapterRuntime,
    tx: &mpsc::Sender<NormalizedEvent>,
    shutdown: &CancellationToken,
    stats: &dyn RuntimeStats,
    options: &RuntimeOptions,
) -> Result<SessionEnd, RuntimeError> {
    let connect_result = tokio::select! {
        () = shutdown.cancelled() => return Ok(SessionEnd::Cancelled),
        result = connect_async(runtime.websocket_url.as_str()) => result,
    };
    let (mut websocket, _) = match connect_result {
        Ok(connection) => connection,
        Err(_) => {
            return Ok(SessionEnd::Reconnect {
                reason: ReconnectReason::Protocol,
                healthy_for: Duration::ZERO,
            });
        }
    };

    if !runtime.subscription.is_empty()
        && timed_ws_send(
            &mut websocket,
            Message::Text(runtime.subscription.clone().into()),
            options.send_timeout,
            shutdown,
        )
        .await
        .is_err()
    {
        return Ok(SessionEnd::Reconnect {
            reason: ReconnectReason::Protocol,
            healthy_for: Duration::ZERO,
        });
    }
    stats.close_gap(runtime.id);
    let healthy_started = Instant::now();

    let proactive_ms = duration_ms(runtime.proactive_reconnect);
    let rotation_jitter_ms = (options.jitter)(proactive_ms).min(proactive_ms.saturating_sub(1));
    let proactive_after = runtime
        .proactive_reconnect
        .saturating_sub(Duration::from_millis(rotation_jitter_ms));
    let proactive = tokio::time::sleep(proactive_after);
    tokio::pin!(proactive);
    let mut idle = tokio::time::interval(options.idle_keepalive);
    idle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    idle.tick().await;
    let mut last_frame = Instant::now();
    let mut consecutive_parse_errors = 0_u8;

    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                let _ = tokio::time::timeout(options.send_timeout, websocket.close(None)).await;
                return Ok(SessionEnd::Cancelled);
            }
            () = &mut proactive => {
                let _ = tokio::time::timeout(options.send_timeout, websocket.close(None)).await;
                return Ok(SessionEnd::Reconnect {
                    reason: ReconnectReason::ProactiveRotation,
                    healthy_for: healthy_started.elapsed(),
                });
            }
            _ = idle.tick() => {
                let elapsed = last_frame.elapsed();
                if elapsed >= options.idle_timeout {
                    return Ok(SessionEnd::Reconnect {
                        reason: ReconnectReason::IdleTimeout,
                        healthy_for: healthy_started.elapsed(),
                    });
                }
                if elapsed < options.idle_keepalive {
                    continue;
                }
                let keepalive = if is_domestic(runtime.id) {
                    Message::Text("PING".into())
                } else {
                    Message::Ping(Vec::new().into())
                };
                if timed_ws_send(&mut websocket, keepalive, options.send_timeout, shutdown).await.is_err() {
                    return Ok(SessionEnd::Reconnect {
                        reason: ReconnectReason::Protocol,
                        healthy_for: healthy_started.elapsed(),
                    });
                }
            }
            frame = websocket.next() => {
                last_frame = Instant::now();
                match frame {
                    None => return Ok(SessionEnd::Reconnect { reason: ReconnectReason::PeerClosed, healthy_for: healthy_started.elapsed() }),
                    Some(Err(_)) => return Ok(SessionEnd::Reconnect { reason: ReconnectReason::Protocol, healthy_for: healthy_started.elapsed() }),
                    Some(Ok(Message::Close(_))) => return Ok(SessionEnd::Reconnect { reason: ReconnectReason::PeerClosed, healthy_for: healthy_started.elapsed() }),
                    Some(Ok(Message::Ping(payload))) => {
                        if timed_ws_send(&mut websocket, Message::Pong(payload), options.send_timeout, shutdown).await.is_err() {
                            return Ok(SessionEnd::Reconnect { reason: ReconnectReason::Protocol, healthy_for: healthy_started.elapsed() });
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Text(text))) => {
                        let recv_us = system_time_us()?;
                        let mut bytes = text.as_bytes().to_vec();
                        match process_frame(runtime, tx, stats, &mut bytes, recv_us, options, shutdown).await? {
                            FrameResult::Accepted => consecutive_parse_errors = 0,
                            FrameResult::Rejected => {
                                consecutive_parse_errors = consecutive_parse_errors.saturating_add(1);
                                if consecutive_parse_errors >= PARSE_ERROR_THRESHOLD {
                                    return Ok(SessionEnd::Reconnect { reason: ReconnectReason::ParseThreshold, healthy_for: healthy_started.elapsed() });
                                }
                            }
                            FrameResult::Backpressure => return Ok(SessionEnd::Reconnect { reason: ReconnectReason::Backpressure, healthy_for: healthy_started.elapsed() }),
                            FrameResult::Cancelled => return Ok(SessionEnd::Cancelled),
                        }
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        let recv_us = system_time_us()?;
                        let mut bytes = bytes.to_vec();
                        match process_frame(runtime, tx, stats, &mut bytes, recv_us, options, shutdown).await? {
                            FrameResult::Accepted => consecutive_parse_errors = 0,
                            FrameResult::Rejected => {
                                consecutive_parse_errors = consecutive_parse_errors.saturating_add(1);
                                if consecutive_parse_errors >= PARSE_ERROR_THRESHOLD {
                                    return Ok(SessionEnd::Reconnect { reason: ReconnectReason::ParseThreshold, healthy_for: healthy_started.elapsed() });
                                }
                            }
                            FrameResult::Backpressure => return Ok(SessionEnd::Reconnect { reason: ReconnectReason::Backpressure, healthy_for: healthy_started.elapsed() }),
                            FrameResult::Cancelled => return Ok(SessionEnd::Cancelled),
                        }
                    }
                    Some(Ok(Message::Frame(_))) => {}
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum FrameResult {
    Accepted,
    Rejected,
    Backpressure,
    Cancelled,
}

async fn process_frame(
    runtime: &AdapterRuntime,
    tx: &mpsc::Sender<NormalizedEvent>,
    stats: &dyn RuntimeStats,
    bytes: &mut [u8],
    recv_us: i64,
    options: &RuntimeOptions,
    shutdown: &CancellationToken,
) -> Result<FrameResult, RuntimeError> {
    stats.on_frame(runtime.id, u32::try_from(bytes.len()).unwrap_or(u32::MAX));
    let events = match runtime.parser.parse(bytes, recv_us) {
        Ok(events) => events,
        Err(error) => {
            stats.on_parse_error(runtime.id);
            let reason = if matches!(error, ParseError::Validation(_)) {
                RejectReason::Validation
            } else {
                RejectReason::Parse
            };
            stats.on_rejected_event(runtime.id, reason);
            return Ok(FrameResult::Rejected);
        }
    };

    let mut books = 0_u64;
    let mut book_rows = 0_u64;
    let mut trades = 0_u64;
    for event in &events {
        match event {
            NormalizedEvent::Book(book) => {
                books += 1;
                book_rows = book_rows.saturating_add(
                    u64::try_from(book.bids.len().saturating_add(book.asks.len()))
                        .unwrap_or(u64::MAX),
                );
            }
            NormalizedEvent::Trade(_) => trades += 1,
        }
        if let Some(source_us) = event.meta().exchange_event_ts_us
            && recv_us >= source_us
        {
            stats.on_receive_lag_us(runtime.id, (recv_us - source_us) as u64);
        }
    }
    stats.on_events(runtime.id, books, book_rows, trades);

    for event in events {
        let send = tokio::time::timeout(options.send_timeout, tx.send(event));
        let result = tokio::select! {
            () = shutdown.cancelled() => return Ok(FrameResult::Cancelled),
            result = send => result,
        };
        match result {
            Ok(Ok(())) => stats.on_queue_depth(runtime.id, tx.max_capacity() - tx.capacity()),
            Ok(Err(_)) => return Err(RuntimeError::EventReceiverClosed),
            Err(_) => {
                stats.on_rejected_event(runtime.id, RejectReason::Backpressure);
                stats.on_backpressure_disconnect(runtime.id);
                stats.open_gap(runtime.id, GapReason::Backpressure);
                return Ok(FrameResult::Backpressure);
            }
        }
    }
    Ok(FrameResult::Accepted)
}

async fn timed_ws_send<S>(
    websocket: &mut tokio_tungstenite::WebSocketStream<S>,
    message: Message,
    timeout: Duration,
    shutdown: &CancellationToken,
) -> Result<(), ()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tokio::select! {
        () = shutdown.cancelled() => Err(()),
        result = tokio::time::timeout(timeout, websocket.send(message)) => {
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) | Err(_) => Err(()),
            }
        }
    }
}

fn is_domestic(adapter: AdapterId) -> bool {
    matches!(adapter, AdapterId::UpbitSpot | AdapterId::BithumbSpot)
}

fn system_time_us() -> Result<i64, RuntimeError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RuntimeError::ClockBeforeEpoch)?;
    i64::try_from(elapsed.as_micros()).map_err(|_| RuntimeError::ClockBeforeEpoch)
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
