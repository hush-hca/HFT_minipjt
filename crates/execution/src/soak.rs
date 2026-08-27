use funding_core::execution::{
    ClientOrderId, ExecutionFill, FillId, OrderIntent, OrderSide, OrderType, TimeInForce,
};
use md_core::model::{AdapterId, CanonicalSymbol};
use ring::digest::{SHA256, digest};
use thiserror::Error;

use crate::{
    journal::{JournalError, OrderJournal},
    oms::{CanonicalOrder, OmsEvent, OrderState, reduce_order},
    reconcile::{FakeVenue, ReconcileError, ReconcileReason, Reconciler, VenueSnapshot},
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SoakConfig {
    pub canonical_orders: u64,
    pub filled_orders: u64,
    pub seed: u64,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SoakReport {
    pub canonical_orders: u64,
    pub filled_orders: u64,
    pub unique_fill_events: u64,
    pub order_state_attribution_ppm: u64,
    pub fill_attribution_ppm: u64,
    pub injected_duplicates: u64,
    pub injected_reorders: u64,
    pub injected_omissions: u64,
    pub injected_disconnects: u64,
    pub injected_cancel_fill_races: u64,
    pub injected_unknown_acks: u64,
    pub post_repair_exact: bool,
    pub duplicate_submitted_orders: u64,
    pub unknown_terminal_orders: u64,
    pub residual_positions: u64,
    pub residual_delta: i128,
    pub canonical_digest_hex: String,
}
#[derive(Debug, Error)]
pub enum SoakError {
    #[error("invalid soak config: {0}")]
    Invalid(&'static str),
    #[error("journal: {0}")]
    Journal(#[from] JournalError),
    #[error("reconcile: {0}")]
    Reconcile(#[from] ReconcileError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
pub fn fill_fault_budget(denominator: u64) -> Result<u64, SoakError> {
    if denominator == 0 {
        return Err(SoakError::Invalid("fill denominator is zero"));
    }
    Ok(denominator / 1_000)
}

pub fn run_soak(config: SoakConfig) -> Result<SoakReport, SoakError> {
    if config.canonical_orders == 0
        || config.filled_orders == 0
        || config.filled_orders > config.canonical_orders
    {
        return Err(SoakError::Invalid("order/fill counts are invalid"));
    }
    let started = std::time::Instant::now();
    let progress = |stage: &str| {
        if config.canonical_orders >= 1_000_000 {
            eprintln!("soak phase={stage} elapsed={:?}", started.elapsed());
        }
    };
    let dir = std::env::temp_dir().join(format!(
        "funding-oms-soak-{}-{}-{}",
        std::process::id(),
        config.seed,
        config.canonical_orders
    ));
    let files = SoakFiles(dir);
    files.cleanup();
    let dir = files.path();
    let mut journal = OrderJournal::open(dir)?;
    let mut authoritative = Vec::with_capacity(config.canonical_orders as usize);
    let mut fill_events = Vec::with_capacity(config.filled_orders as usize);
    const CHUNK: usize = 4096;
    let mut chunk = Vec::with_capacity(CHUNK);
    for n in 0..config.canonical_orders {
        let intent = make_intent(n, config.seed);
        chunk.push(intent.clone());
        let mut order = CanonicalOrder::new(intent).map_err(JournalError::from)?;
        if n < config.filled_orders {
            let fill = make_fill(n, config.seed);
            order =
                reduce_order(&order, &OmsEvent::Fill(fill.clone())).map_err(JournalError::from)?;
            fill_events.push(fill);
        } else {
            order = reduce_order(
                &order,
                &OmsEvent::Status {
                    state: OrderState::Canceled,
                    cumulative_quantity: 0,
                    source_sequence: Some(1),
                    venue_order_id: None,
                },
            )
            .map_err(JournalError::from)?;
        }
        authoritative.push(order);
        if chunk.len() == CHUNK {
            journal.record_intents(&chunk)?;
            chunk.clear();
        }
    }
    if !chunk.is_empty() {
        journal.record_intents(&chunk)?;
    }
    progress("intents-recorded");
    let omitted = fill_fault_budget(fill_events.len() as u64)?;
    let duplicate_count = (config.filled_orders / 100)
        .max(1)
        .min(config.filled_orders);
    let race_count = (config.filled_orders / 100).max(1);
    let unknown_count = (config.filled_orders / 200).max(1);
    let mut stream: Vec<usize> = (0..fill_events.len()).collect();
    for chunk in stream.chunks_mut(32) {
        chunk.reverse()
    }
    let injected_reorders = stream.len().saturating_sub(stream.len().div_ceil(32)) as u64;
    let mut injected_disconnects = 0;
    let mut injected_duplicates = 0;
    let mut injected_cancel_fill_races = 0;
    let mut injected_unknown_acks = 0;
    let disconnect_stride = stream.len().clamp(2, 4096) / 2;
    let mut fault_batch = Vec::with_capacity(disconnect_stride.saturating_mul(4));
    for (position, n) in stream.into_iter().enumerate() {
        if position > 0 && position % disconnect_stride == 0 {
            journal.apply_events(&fault_batch)?;
            fault_batch.clear();
            drop(journal);
            journal = OrderJournal::open(dir)?;
            injected_disconnects += 1;
        }
        let fill = &fill_events[n];
        if (n as u64) < omitted {
            continue;
        }
        let id = &fill.client_order_id;
        if (n as u64) < unknown_count {
            fault_batch.push((
                fill.venue,
                format!("submitted-{n}"),
                id.clone(),
                OmsEvent::Submitted,
            ));
            fault_batch.push((
                fill.venue,
                format!("unknown-{n}"),
                id.clone(),
                OmsEvent::UnknownSubmit,
            ));
            injected_unknown_acks += 1;
        }
        if (n as u64) < race_count {
            fault_batch.push((
                fill.venue,
                format!("cancel-race-{n}"),
                id.clone(),
                OmsEvent::Status {
                    state: OrderState::Canceled,
                    cumulative_quantity: 0,
                    source_sequence: Some(1),
                    venue_order_id: None,
                },
            ));
            injected_cancel_fill_races += 1;
        }
        fault_batch.push((
            fill.venue,
            format!("fill-{n}"),
            id.clone(),
            OmsEvent::Fill(fill.clone()),
        ));
        if (n as u64) < duplicate_count {
            fault_batch.push((
                fill.venue,
                format!("fill-{n}"),
                id.clone(),
                OmsEvent::Fill(fill.clone()),
            ));
            injected_duplicates += 1;
        }
    }
    if !fault_batch.is_empty() {
        journal.apply_events(&fault_batch)?;
    }
    let mut cancel_batch = Vec::with_capacity(CHUNK);
    for n in config.filled_orders..config.canonical_orders {
        let id = ClientOrderId(format!("oms-{:016x}", permute(n ^ config.seed)));
        cancel_batch.push((
            AdapterId::BinanceUsdm,
            format!("cancel-{n}"),
            id,
            OmsEvent::Status {
                state: OrderState::Canceled,
                cumulative_quantity: 0,
                source_sequence: Some(1),
                venue_order_id: None,
            },
        ));
        if cancel_batch.len() == CHUNK {
            journal.apply_events(&cancel_batch)?;
            cancel_batch.clear();
        }
    }
    if !cancel_batch.is_empty() {
        journal.apply_events(&cancel_batch)?;
    }
    progress("fault-stream-recorded");
    let local = journal.snapshot()?.orders;
    let local_by_id: std::collections::BTreeMap<_, _> = local
        .iter()
        .map(|o| {
            (
                (venue_ord(o.intent.venue), o.intent.client_order_id.clone()),
                o,
            )
        })
        .collect();
    let authoritative_by_id: std::collections::BTreeMap<_, _> = authoritative
        .iter()
        .map(|o| {
            (
                (venue_ord(o.intent.venue), o.intent.client_order_id.clone()),
                o,
            )
        })
        .collect();
    let correct_orders = authoritative_by_id
        .iter()
        .filter(|(k, v)| {
            local_by_id
                .get(*k)
                .is_some_and(|have| same_attribution(have, v))
        })
        .count() as u64;
    let correct_fills = fill_events
        .iter()
        .filter(|f| {
            local_by_id
                .get(&(venue_ord(f.venue), f.client_order_id.clone()))
                .is_some_and(|o| o.fills.get(&f.fill_id) == Some(*f))
        })
        .count() as u64;
    let order_ppm = ppm(correct_orders, config.canonical_orders)?;
    let fill_ppm = ppm(correct_fills, fill_events.len() as u64)?;
    progress("pre-repair-compared");
    let api = FakeVenue::new(VenueSnapshot::from_orders(
        "soak-authoritative-v1",
        authoritative,
    ));
    progress("authoritative-snapshot-built");
    let repaired = Reconciler::new(&mut journal, &api).run(ReconcileReason::Shutdown)?;
    progress("reconciled");
    let mut report = SoakReport {
        canonical_orders: config.canonical_orders,
        filled_orders: config.filled_orders,
        unique_fill_events: fill_events.len() as u64,
        order_state_attribution_ppm: order_ppm,
        fill_attribution_ppm: fill_ppm,
        injected_duplicates,
        injected_reorders,
        injected_omissions: omitted,
        injected_disconnects,
        injected_cancel_fill_races,
        injected_unknown_acks,
        post_repair_exact: repaired.exact,
        duplicate_submitted_orders: count_duplicate_client_ids(&repaired.snapshot.orders),
        unknown_terminal_orders: repaired
            .snapshot
            .orders
            .iter()
            .filter(|o| !is_known_terminal(o))
            .count() as u64,
        residual_positions: repaired.residual_positions,
        residual_delta: repaired.residual_delta,
        canonical_digest_hex: String::new(),
    };
    report.canonical_digest_hex = report_digest(&report, &repaired.snapshot);
    progress("digest-built");
    Ok(report)
}

struct SoakFiles(std::path::PathBuf);
impl SoakFiles {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
    fn cleanup(&self) {
        let _ = std::fs::remove_file(&self.0);
        let _ = std::fs::remove_file(format!("{}-wal", self.0.display()));
        let _ = std::fs::remove_file(format!("{}-shm", self.0.display()));
    }
}
impl Drop for SoakFiles {
    fn drop(&mut self) {
        self.cleanup();
    }
}

fn same_attribution(have: &CanonicalOrder, want: &CanonicalOrder) -> bool {
    have.intent.venue == want.intent.venue
        && have.intent.client_order_id == want.intent.client_order_id
        && have.request_hash == want.request_hash
        && have.venue_order_id == want.venue_order_id
        && have.state == want.state
        && have.attributed_fill_quantity == want.attributed_fill_quantity
        && have.venue_cumulative_quantity == want.venue_cumulative_quantity
        && have.cumulative_fee == want.cumulative_fee
        && have.fills == want.fills
}
fn count_duplicate_client_ids(orders: &[CanonicalOrder]) -> u64 {
    let unique = orders
        .iter()
        .map(|o| (venue_ord(o.intent.venue), &o.intent.client_order_id))
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    orders.len().saturating_sub(unique) as u64
}
fn is_known_terminal(o: &CanonicalOrder) -> bool {
    matches!(
        o.state,
        OrderState::Filled | OrderState::Canceled | OrderState::Rejected | OrderState::Expired
    )
}
fn ppm(correct: u64, total: u64) -> Result<u64, SoakError> {
    if total == 0 {
        return Err(SoakError::Invalid("ppm denominator is zero"));
    }
    let v = (u128::from(correct) * 1_000_000) / u128::from(total);
    u64::try_from(v).map_err(|_| SoakError::Invalid("ppm overflow"))
}
fn make_intent(n: u64, seed: u64) -> OrderIntent {
    OrderIntent {
        venue: AdapterId::BinanceUsdm,
        client_order_id: ClientOrderId(format!("oms-{:016x}", permute(n ^ seed))),
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        side: if n % 2 == 0 {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::PostOnly,
        quantity: 1,
        limit_price: Some(100),
        reduce_only: false,
        created_ts_us: (n + 1) as i64,
    }
}
fn make_fill(n: u64, seed: u64) -> ExecutionFill {
    let i = make_intent(n, seed);
    ExecutionFill {
        venue: i.venue,
        client_order_id: i.client_order_id,
        venue_order_id: None,
        fill_id: FillId(format!("fill-{:016x}", permute(n ^ seed ^ 0xa5a5))),
        price: 100,
        quantity: 1,
        fee: 1,
        fee_asset: "USDT".into(),
        source_ts_us: (n + 2) as i64,
    }
}
fn permute(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
fn venue_ord(v: AdapterId) -> u8 {
    match v {
        AdapterId::UpbitSpot => 0,
        AdapterId::BithumbSpot => 1,
        AdapterId::BinanceSpot => 2,
        AdapterId::BinanceUsdm => 3,
        AdapterId::BybitLinear => 4,
    }
}
fn report_digest(r: &SoakReport, snapshot: &crate::journal::JournalSnapshot) -> String {
    let mut b = b"OMS-SOAK-REPORT\0\x01".to_vec();
    for v in [
        r.canonical_orders,
        r.filled_orders,
        r.unique_fill_events,
        r.order_state_attribution_ppm,
        r.fill_attribution_ppm,
        r.injected_duplicates,
        r.injected_reorders,
        r.injected_omissions,
        r.injected_disconnects,
        r.injected_cancel_fill_races,
        r.injected_unknown_acks,
        r.duplicate_submitted_orders,
        r.unknown_terminal_orders,
        r.residual_positions,
    ] {
        b.extend(v.to_be_bytes())
    }
    b.extend(r.residual_delta.to_be_bytes());
    b.push(u8::from(r.post_repair_exact));
    for order in &snapshot.orders {
        b.extend(order.request_hash);
        b.push(order.state as u8);
        b.extend(order.attributed_fill_quantity.to_be_bytes());
        b.extend(order.venue_cumulative_quantity.to_be_bytes());
        for fill in order.fills.values() {
            b.push(venue_ord(fill.venue));
            b.extend((fill.client_order_id.0.len() as u32).to_be_bytes());
            b.extend(fill.client_order_id.0.as_bytes());
            b.extend((fill.fill_id.0.len() as u32).to_be_bytes());
            b.extend(fill.fill_id.0.as_bytes());
            match &fill.venue_order_id {
                Some(id) => {
                    b.push(1);
                    b.extend((id.0.len() as u32).to_be_bytes());
                    b.extend(id.0.as_bytes());
                }
                None => b.push(0),
            }
            b.extend(fill.price.to_be_bytes());
            b.extend(fill.quantity.to_be_bytes());
            b.extend(fill.fee.to_be_bytes());
            b.extend((fill.fee_asset.len() as u32).to_be_bytes());
            b.extend(fill.fee_asset.as_bytes());
            b.extend(fill.source_ts_us.to_be_bytes());
        }
    }
    for position in &snapshot.positions {
        b.push(venue_ord(position.venue));
        b.extend((position.symbol.base.len() as u32).to_be_bytes());
        b.extend(position.symbol.base.as_bytes());
        b.extend((position.symbol.quote.len() as u32).to_be_bytes());
        b.extend(position.symbol.quote.as_bytes());
        b.extend(position.signed_quantity.to_be_bytes());
    }
    digest(&SHA256, &b)
        .as_ref()
        .iter()
        .map(|v| format!("{v:02x}"))
        .collect()
}
