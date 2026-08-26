use std::collections::BTreeMap;

use funding_core::execution::{
    BalanceSnapshot, ClientOrderId, ExecutionFill, FundingIncome, Position,
};
use md_core::model::AdapterId;
use thiserror::Error;

use crate::{
    journal::{JournalError, OrderJournal},
    oms::{CanonicalOrder, OmsEvent},
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ReconcileReason {
    Startup,
    UnknownSubmit,
    PrivateGap,
    Shutdown,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SnapshotMeta {
    pub token: String,
    pub as_of_ts_us: i64,
    pub authoritative_complete: bool,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Page<T> {
    pub values: Vec<T>,
    pub next_cursor: Option<u64>,
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum AuthoritativeLookup<T> {
    Found(T),
    AbsentComplete,
}

pub trait VenueReconcileApi: Send + Sync {
    fn begin_snapshot(&self) -> Result<SnapshotMeta, ReconcileError>;
    fn order_by_client_id(
        &self,
        token: &str,
        venue: AdapterId,
        id: &ClientOrderId,
    ) -> Result<AuthoritativeLookup<CanonicalOrder>, ReconcileError>;
    fn orders_page(
        &self,
        token: &str,
        cursor: Option<u64>,
        limit: usize,
    ) -> Result<Page<CanonicalOrder>, ReconcileError>;
    fn fills_page(
        &self,
        token: &str,
        cursor: Option<u64>,
        limit: usize,
    ) -> Result<Page<ExecutionFill>, ReconcileError>;
    fn positions_page(
        &self,
        token: &str,
        cursor: Option<u64>,
        limit: usize,
    ) -> Result<Page<Position>, ReconcileError>;
    fn balances_page(
        &self,
        token: &str,
        cursor: Option<u64>,
        limit: usize,
    ) -> Result<Page<BalanceSnapshot>, ReconcileError>;
    fn funding_income_page(
        &self,
        token: &str,
        cursor: Option<u64>,
        limit: usize,
    ) -> Result<Page<FundingIncome>, ReconcileError>;
}

#[derive(Debug, Error)]
pub enum ReconcileError {
    #[error("journal: {0}")]
    Journal(#[from] JournalError),
    #[error("snapshot is not authoritative and complete")]
    Incomplete,
    #[error("venue snapshot error: {0}")]
    Venue(String),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ReconciliationReport {
    pub reason: ReconcileReason,
    pub exact: bool,
    pub repaired_orders: u64,
    pub repaired_fills: u64,
    pub unresolved_orders: u64,
    pub duplicate_orders_created: u64,
    pub residual_positions: u64,
    pub residual_delta: i128,
    pub snapshot: crate::journal::JournalSnapshot,
}

pub struct Reconciler<'a> {
    journal: &'a mut OrderJournal,
    api: &'a dyn VenueReconcileApi,
}
impl<'a> Reconciler<'a> {
    pub fn new(journal: &'a mut OrderJournal, api: &'a dyn VenueReconcileApi) -> Self {
        Self { journal, api }
    }
    pub fn run(self, reason: ReconcileReason) -> Result<ReconciliationReport, ReconcileError> {
        let meta = self.api.begin_snapshot()?;
        if !meta.authoritative_complete {
            return Err(ReconcileError::Incomplete);
        }
        let run_id = self
            .journal
            .begin_reconciliation(reason_code(reason), &meta.token)?;
        let mut targeted = BTreeMap::new();
        for order in self
            .journal
            .snapshot()?
            .orders
            .iter()
            .filter(|o| o.blocks_new_orders())
        {
            if let AuthoritativeLookup::Found(found) = self.api.order_by_client_id(
                &meta.token,
                order.intent.venue,
                &order.intent.client_order_id,
            )? {
                if found.intent.venue != order.intent.venue
                    || found.intent.client_order_id != order.intent.client_order_id
                {
                    return Err(ReconcileError::Venue(
                        "targeted order identity mismatch".into(),
                    ));
                }
                targeted.insert(
                    (
                        venue_key(found.intent.venue),
                        found.intent.client_order_id.clone(),
                    ),
                    found,
                );
            }
        }
        let mut remote = collect_orders(self.api, &meta.token)?;
        for (key, value) in targeted {
            if remote
                .insert(key, value.clone())
                .is_some_and(|old| !canonical_equal(&old, &value))
            {
                return Err(ReconcileError::Venue(
                    "targeted and paged order conflict".into(),
                ));
            }
        }
        let remote_fills = collect_fills(self.api, &meta.token)?;
        let remote_positions = collect_all(|c, l| self.api.positions_page(&meta.token, c, l))?;
        let remote_balances = collect_all(|c, l| self.api.balances_page(&meta.token, c, l))?;
        let remote_funding = collect_all(|c, l| self.api.funding_income_page(&meta.token, c, l))?;
        let mut local: BTreeMap<_, _> = self
            .journal
            .snapshot()?
            .orders
            .into_iter()
            .map(|o| {
                (
                    (venue_key(o.intent.venue), o.intent.client_order_id.clone()),
                    o,
                )
            })
            .collect();
        let mut repaired_fills = 0;
        let mut repaired_orders = 0;
        for fill in remote_fills.values() {
            let key = (venue_key(fill.venue), fill.client_order_id.clone());
            let missing = local
                .get(&key)
                .is_some_and(|o| !o.fills.contains_key(&fill.fill_id));
            if missing {
                self.journal.apply_event(
                    fill.venue,
                    &format!("reconcile:{}:fill:{}", meta.token, fill.fill_id.0),
                    &fill.client_order_id,
                    &OmsEvent::Fill(fill.clone()),
                )?;
                repaired_fills += 1;
            }
        }
        local = self
            .journal
            .snapshot()?
            .orders
            .into_iter()
            .map(|o| {
                (
                    (venue_key(o.intent.venue), o.intent.client_order_id.clone()),
                    o,
                )
            })
            .collect();
        for (key, want) in &remote {
            let Some(have) = local.get(key) else { continue };
            let id = &key.1;
            if have.request_hash != want.request_hash {
                continue;
            }
            if have.state != want.state
                || have.venue_cumulative_quantity != want.venue_cumulative_quantity
                || have.venue_order_id != want.venue_order_id
            {
                self.journal.apply_event(
                    want.intent.venue,
                    &format!("reconcile:{}:status:{}", meta.token, id.0),
                    id,
                    &OmsEvent::Status {
                        state: want.state,
                        cumulative_quantity: want.venue_cumulative_quantity,
                        source_sequence: want.last_source_sequence,
                        venue_order_id: want.venue_order_id.clone(),
                    },
                )?;
                repaired_orders += 1;
            }
        }
        self.journal
            .replace_account_facts(&remote_positions, &remote_balances, &remote_funding)?;
        let final_local: BTreeMap<_, _> = self
            .journal
            .snapshot()?
            .orders
            .into_iter()
            .map(|mut o| {
                o.reconciled = false;
                (
                    (venue_key(o.intent.venue), o.intent.client_order_id.clone()),
                    o,
                )
            })
            .collect();
        let normalized_remote: BTreeMap<_, _> = remote;
        let derived_positions = derive_positions(final_local.values())?;
        let authoritative_positions = position_map(&remote_positions)?;
        let account_exact = derived_positions == authoritative_positions;
        let exact = account_exact
            && final_local.len() == normalized_remote.len()
            && final_local.iter().all(|(id, have)| {
                normalized_remote
                    .get(id)
                    .is_some_and(|want| canonical_equal(have, want))
            });
        if exact {
            self.journal.mark_reconciled(
                &final_local
                    .values()
                    .map(|o| (o.intent.venue, o.intent.client_order_id.clone()))
                    .collect::<Vec<_>>(),
            )?;
        }
        let snapshot = self.journal.snapshot()?;
        let residual_delta = snapshot.orders.iter().try_fold(0_i128, |acc, order| {
            let signed = if order.intent.side == funding_core::execution::OrderSide::Buy {
                order.attributed_fill_quantity
            } else {
                -order.attributed_fill_quantity
            };
            acc.checked_add(signed)
                .ok_or_else(|| ReconcileError::Venue("residual overflow".into()))
        })?;
        self.journal.finish_reconciliation(run_id, exact)?;
        Ok(ReconciliationReport {
            reason,
            exact,
            repaired_orders,
            repaired_fills,
            unresolved_orders: if exact {
                0
            } else {
                final_local
                    .iter()
                    .filter(|(id, o)| {
                        normalized_remote
                            .get(*id)
                            .is_none_or(|want| !canonical_equal(o, want))
                    })
                    .count() as u64
            },
            duplicate_orders_created: 0,
            residual_positions: u64::from(residual_delta != 0),
            residual_delta,
            snapshot,
        })
    }
}

fn derive_positions<'a>(
    orders: impl Iterator<Item = &'a CanonicalOrder>,
) -> Result<BTreeMap<(u8, String, String), i128>, ReconcileError> {
    let mut out = BTreeMap::new();
    for o in orders {
        let key = (
            venue_key(o.intent.venue),
            o.intent.symbol.base.clone(),
            o.intent.symbol.quote.clone(),
        );
        let signed = if o.intent.side == funding_core::execution::OrderSide::Buy {
            o.attributed_fill_quantity
        } else {
            -o.attributed_fill_quantity
        };
        let next = out
            .get(&key)
            .copied()
            .unwrap_or(0_i128)
            .checked_add(signed)
            .ok_or_else(|| ReconcileError::Venue("position overflow".into()))?;
        if next == 0 {
            out.remove(&key);
        } else {
            out.insert(key, next);
        }
    }
    Ok(out)
}
fn position_map(
    positions: &[Position],
) -> Result<BTreeMap<(u8, String, String), i128>, ReconcileError> {
    let mut out = BTreeMap::new();
    for p in positions {
        let key = (
            venue_key(p.venue),
            p.symbol.base.clone(),
            p.symbol.quote.clone(),
        );
        if p.signed_quantity == 0 {
            continue;
        }
        if out.insert(key, p.signed_quantity).is_some() {
            return Err(ReconcileError::Venue(
                "duplicate authoritative position".into(),
            ));
        }
    }
    Ok(out)
}

fn reason_code(reason: ReconcileReason) -> &'static str {
    match reason {
        ReconcileReason::Startup => "startup",
        ReconcileReason::UnknownSubmit => "unknown_submit",
        ReconcileReason::PrivateGap => "private_gap",
        ReconcileReason::Shutdown => "shutdown",
    }
}

fn canonical_equal(have: &CanonicalOrder, want: &CanonicalOrder) -> bool {
    have.intent == want.intent
        && have.request_hash == want.request_hash
        && have.venue_order_id == want.venue_order_id
        && have.state == want.state
        && have.attributed_fill_quantity == want.attributed_fill_quantity
        && have.venue_cumulative_quantity == want.venue_cumulative_quantity
        && have.cumulative_fee == want.cumulative_fee
        && have.fills == want.fills
}

fn collect_orders(
    api: &dyn VenueReconcileApi,
    token: &str,
) -> Result<BTreeMap<(u8, ClientOrderId), CanonicalOrder>, ReconcileError> {
    let mut out = BTreeMap::new();
    let mut cursor = None;
    let mut seen = std::collections::BTreeSet::new();
    let mut pages = 0;
    loop {
        let page = api.orders_page(token, cursor, 4096)?;
        for order in page.values {
            if out
                .insert(
                    (
                        venue_key(order.intent.venue),
                        order.intent.client_order_id.clone(),
                    ),
                    order,
                )
                .is_some()
            {
                return Err(ReconcileError::Venue("duplicate remote client id".into()));
            }
        }
        let next = page.next_cursor;
        if next.is_some_and(|value| !seen.insert(value)) {
            return Err(ReconcileError::Venue("cursor cycle".into()));
        }
        cursor = next;
        pages += 1;
        if cursor.is_none() {
            break;
        }
        if pages > 1_000_000 {
            return Err(ReconcileError::Venue("cursor cycle".into()));
        }
    }
    Ok(out)
}
fn collect_fills(
    api: &dyn VenueReconcileApi,
    token: &str,
) -> Result<BTreeMap<String, ExecutionFill>, ReconcileError> {
    let mut out = BTreeMap::new();
    let mut cursor = None;
    let mut seen = std::collections::BTreeSet::new();
    let mut pages = 0;
    loop {
        let page = api.fills_page(token, cursor, 4096)?;
        for fill in page.values {
            let key = format!("{:?}:{}", fill.venue, fill.fill_id.0);
            if let Some(old) = out.insert(key, fill.clone()) {
                if old != fill {
                    return Err(ReconcileError::Venue("conflicting remote fill id".into()));
                }
            }
        }
        let next = page.next_cursor;
        if next.is_some_and(|value| !seen.insert(value)) {
            return Err(ReconcileError::Venue("cursor cycle".into()));
        }
        cursor = next;
        pages += 1;
        if cursor.is_none() {
            break;
        }
        if pages > 1_000_000 {
            return Err(ReconcileError::Venue("cursor page limit".into()));
        }
    }
    Ok(out)
}

fn collect_all<T>(
    mut fetch: impl FnMut(Option<u64>, usize) -> Result<Page<T>, ReconcileError>,
) -> Result<Vec<T>, ReconcileError> {
    let mut out = Vec::new();
    let mut cursor = None;
    let mut seen = std::collections::BTreeSet::new();
    loop {
        let page = fetch(cursor, 4096)?;
        out.extend(page.values);
        let next = page.next_cursor;
        if next.is_some_and(|value| !seen.insert(value)) {
            return Err(ReconcileError::Venue("cursor cycle".into()));
        }
        cursor = next;
        if cursor.is_none() {
            break;
        }
        if seen.len() > 1_000_000 {
            return Err(ReconcileError::Venue("cursor page limit".into()));
        }
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct VenueSnapshot {
    pub meta: SnapshotMeta,
    pub orders: Vec<CanonicalOrder>,
    pub fills: Vec<ExecutionFill>,
    pub positions: Vec<Position>,
    pub balances: Vec<BalanceSnapshot>,
    pub funding_income: Vec<FundingIncome>,
}
impl VenueSnapshot {
    pub fn from_orders(token: &str, orders: Vec<CanonicalOrder>) -> Self {
        let fills = orders
            .iter()
            .flat_map(|o| o.fills.values().cloned())
            .collect();
        let positions = derive_positions(orders.iter())
            .expect("fixture position arithmetic")
            .into_iter()
            .map(|((venue, base, quote), signed_quantity)| Position {
                venue: parse_venue_key(venue),
                symbol: md_core::model::CanonicalSymbol::new(base, quote),
                signed_quantity,
            })
            .collect();
        Self {
            meta: SnapshotMeta {
                token: token.into(),
                as_of_ts_us: 1,
                authoritative_complete: true,
            },
            orders,
            fills,
            positions,
            balances: Vec::new(),
            funding_income: Vec::new(),
        }
    }
}
fn parse_venue_key(v: u8) -> AdapterId {
    match v {
        0 => AdapterId::UpbitSpot,
        1 => AdapterId::BithumbSpot,
        2 => AdapterId::BinanceSpot,
        3 => AdapterId::BinanceUsdm,
        4 => AdapterId::BybitLinear,
        _ => unreachable!(),
    }
}
#[derive(Debug, Clone)]
pub struct FakeVenue {
    snapshot: VenueSnapshot,
}
impl FakeVenue {
    pub fn new(snapshot: VenueSnapshot) -> Self {
        Self { snapshot }
    }
    fn check(&self, t: &str) -> Result<(), ReconcileError> {
        if t == self.snapshot.meta.token {
            Ok(())
        } else {
            Err(ReconcileError::Venue("snapshot token mismatch".into()))
        }
    }
}
impl VenueReconcileApi for FakeVenue {
    fn begin_snapshot(&self) -> Result<SnapshotMeta, ReconcileError> {
        Ok(self.snapshot.meta.clone())
    }
    fn order_by_client_id(
        &self,
        t: &str,
        venue: AdapterId,
        id: &ClientOrderId,
    ) -> Result<AuthoritativeLookup<CanonicalOrder>, ReconcileError> {
        self.check(t)?;
        Ok(self
            .snapshot
            .orders
            .iter()
            .find(|o| o.intent.venue == venue && &o.intent.client_order_id == id)
            .cloned()
            .map_or(
                AuthoritativeLookup::AbsentComplete,
                AuthoritativeLookup::Found,
            ))
    }
    fn orders_page(
        &self,
        t: &str,
        c: Option<u64>,
        l: usize,
    ) -> Result<Page<CanonicalOrder>, ReconcileError> {
        self.check(t)?;
        page(&self.snapshot.orders, c, l)
    }
    fn fills_page(
        &self,
        t: &str,
        c: Option<u64>,
        l: usize,
    ) -> Result<Page<ExecutionFill>, ReconcileError> {
        self.check(t)?;
        page(&self.snapshot.fills, c, l)
    }
    fn positions_page(
        &self,
        t: &str,
        c: Option<u64>,
        l: usize,
    ) -> Result<Page<Position>, ReconcileError> {
        self.check(t)?;
        page(&self.snapshot.positions, c, l)
    }
    fn balances_page(
        &self,
        t: &str,
        c: Option<u64>,
        l: usize,
    ) -> Result<Page<BalanceSnapshot>, ReconcileError> {
        self.check(t)?;
        page(&self.snapshot.balances, c, l)
    }
    fn funding_income_page(
        &self,
        t: &str,
        c: Option<u64>,
        l: usize,
    ) -> Result<Page<FundingIncome>, ReconcileError> {
        self.check(t)?;
        page(&self.snapshot.funding_income, c, l)
    }
}
fn page<T: Clone>(v: &[T], c: Option<u64>, limit: usize) -> Result<Page<T>, ReconcileError> {
    if limit == 0 {
        return Err(ReconcileError::Venue("zero page limit".into()));
    }
    let start = c.unwrap_or(0) as usize;
    if start > v.len() {
        return Err(ReconcileError::Venue("invalid cursor".into()));
    }
    let end = start.saturating_add(limit).min(v.len());
    Ok(Page {
        values: v[start..end].to_vec(),
        next_cursor: (end < v.len()).then_some(end as u64),
    })
}
fn venue_key(v: AdapterId) -> u8 {
    match v {
        AdapterId::UpbitSpot => 0,
        AdapterId::BithumbSpot => 1,
        AdapterId::BinanceSpot => 2,
        AdapterId::BinanceUsdm => 3,
        AdapterId::BybitLinear => 4,
    }
}
