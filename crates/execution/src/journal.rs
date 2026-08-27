use std::path::Path;

use funding_core::execution::{
    BalanceSnapshot, ClientOrderId, ExecutionFill, FillId, FundingIncome, OrderIntent, OrderSide,
    OrderType, Position, TimeInForce, VenueOrderId,
};
use md_core::model::{AdapterId, CanonicalSymbol};
use ring::digest::{SHA256, digest};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use thiserror::Error;

use crate::oms::{CanonicalOrder, OmsError, OmsEvent, OrderState, reduce_order};

const SCHEMA_VERSION: i64 = 1;

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("sqlite journal error: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("oms error: {0}")]
    Oms(#[from] OmsError),
    #[error("journal conflict: {0}")]
    Conflict(String),
    #[error("unsupported schema version {0}")]
    FutureSchema(i64),
    #[error("journal invariant failed: {0}")]
    Invariant(String),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct JournalSnapshot {
    pub orders: Vec<CanonicalOrder>,
    pub positions: Vec<Position>,
    pub balances: Vec<BalanceSnapshot>,
    pub funding_income: Vec<FundingIncome>,
}

pub struct OrderJournal {
    conn: Connection,
}

impl OrderJournal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, JournalError> {
        let conn = Connection::open(path)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        migrate(&conn)?;
        let mode: String = conn.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
        let fk: i64 = conn.pragma_query_value(None, "foreign_keys", |r| r.get(0))?;
        let sync: i64 = conn.pragma_query_value(None, "synchronous", |r| r.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") || fk != 1 || sync < 2 {
            return Err(JournalError::Invariant(
                "required SQLite PRAGMAs unavailable".into(),
            ));
        }
        Ok(Self { conn })
    }

    pub fn record_intent(&mut self, intent: &OrderIntent) -> Result<(), JournalError> {
        self.record_intents(std::slice::from_ref(intent))
    }
    pub fn record_intents(&mut self, intents: &[OrderIntent]) -> Result<(), JournalError> {
        let tx = self.conn.transaction()?;
        for intent in intents {
            let canonical = CanonicalOrder::new(intent.clone())?;
            insert_intent(&tx, &canonical)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn apply_event(
        &mut self,
        venue: AdapterId,
        event_id: &str,
        client_id: &ClientOrderId,
        event: &OmsEvent,
    ) -> Result<CanonicalOrder, JournalError> {
        let tx = self.conn.transaction()?;
        let order = apply_event_tx(&tx, venue, event_id, client_id, event)?;
        tx.commit()?;
        Ok(order)
    }
    pub fn apply_events(
        &mut self,
        events: &[(AdapterId, String, ClientOrderId, OmsEvent)],
    ) -> Result<(), JournalError> {
        let tx = self.conn.transaction()?;
        for (venue, event_id, client_id, event) in events {
            apply_event_tx(&tx, *venue, event_id, client_id, event)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn order(
        &self,
        venue: AdapterId,
        id: &ClientOrderId,
    ) -> Result<Option<CanonicalOrder>, JournalError> {
        load_order(&self.conn, venue, id)
    }
    pub fn snapshot(&self) -> Result<JournalSnapshot, JournalError> {
        let mut stmt = self.conn.prepare("SELECT venue,client_id,request_hash,base,quote,side,order_type,tif,quantity,limit_price,reduce_only,created_ts_us,venue_order_id,state,attributed_qty,venue_cumulative_qty,cumulative_fee,last_source_sequence,last_status_state,last_status_qty,last_status_venue_id,reconciled FROM orders ORDER BY venue,client_id")?;
        let raw = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, Vec<u8>>(8)?,
                    r.get::<_, Option<Vec<u8>>>(9)?,
                    r.get::<_, i64>(10)?,
                    r.get::<_, i64>(11)?,
                    r.get::<_, Option<String>>(12)?,
                    r.get::<_, String>(13)?,
                    r.get::<_, Vec<u8>>(14)?,
                    r.get::<_, Vec<u8>>(15)?,
                    r.get::<_, Vec<u8>>(16)?,
                    r.get::<_, Option<Vec<u8>>>(17)?,
                    r.get::<_, Option<String>>(18)?,
                    r.get::<_, Option<Vec<u8>>>(19)?,
                    r.get::<_, Option<String>>(20)?,
                    r.get::<_, i64>(21)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut orders = Vec::with_capacity(raw.len());
        let mut indices = std::collections::BTreeMap::new();
        for (
            venue_s,
            id_s,
            hash,
            base,
            quote,
            side,
            kind,
            tif,
            qty,
            price,
            ro,
            ts,
            vid,
            state,
            aq,
            vq,
            fee,
            seq,
            last_state,
            last_qty,
            last_vid,
            rec,
        ) in raw
        {
            let venue = parse_venue(&venue_s)?;
            let id = ClientOrderId(id_s);
            let last_status = match (last_state, last_qty) {
                (Some(s), Some(q)) => Some(crate::oms::StatusIdentity {
                    state: parse_state(&s)?,
                    cumulative_quantity: from_be(&q)?,
                    venue_order_id: last_vid.map(VenueOrderId),
                }),
                (None, None) => None,
                _ => return Err(JournalError::Invariant("partial status identity".into())),
            };
            let intent = OrderIntent {
                venue,
                client_order_id: id.clone(),
                symbol: CanonicalSymbol::new(base, quote),
                side: parse_side(&side)?,
                order_type: parse_type(&kind)?,
                time_in_force: parse_tif(&tif)?,
                quantity: from_be(&qty)?,
                limit_price: price.map(|v| from_be(&v)).transpose()?,
                reduce_only: ro != 0,
                created_ts_us: ts,
            };
            let index = orders.len();
            indices.insert((venue_code(venue), id.clone()), index);
            orders.push(CanonicalOrder {
                intent,
                request_hash: hash
                    .try_into()
                    .map_err(|_| JournalError::Invariant("bad request hash".into()))?,
                venue_order_id: vid.map(VenueOrderId),
                state: parse_state(&state)?,
                attributed_fill_quantity: from_be(&aq)?,
                venue_cumulative_quantity: from_be(&vq)?,
                cumulative_fee: from_be(&fee)?,
                fills: std::collections::BTreeMap::new(),
                last_source_sequence: seq
                    .map(|v| {
                        v.try_into()
                            .map(u64::from_be_bytes)
                            .map_err(|_| JournalError::Invariant("bad u64".into()))
                    })
                    .transpose()?,
                last_status,
                reconciled: rec != 0,
            });
        }
        let mut fills = self.conn.prepare("SELECT venue,client_id,fill_id,venue_order_id,price,quantity,fee,fee_asset,source_ts_us FROM fills ORDER BY venue,client_id,fill_id")?;
        let rows = fills.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Vec<u8>>(4)?,
                r.get::<_, Vec<u8>>(5)?,
                r.get::<_, Vec<u8>>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, i64>(8)?,
            ))
        })?;
        for row in rows {
            let (v, id, fid, vid, p, q, fee, asset, ts) = row?;
            let venue = parse_venue(&v)?;
            let cid = ClientOrderId(id);
            let index = *indices
                .get(&(venue_code(venue), cid.clone()))
                .ok_or_else(|| JournalError::Invariant("fill owner missing".into()))?;
            let fill = ExecutionFill {
                venue,
                client_order_id: cid,
                fill_id: FillId(fid),
                venue_order_id: vid.map(VenueOrderId),
                price: from_be(&p)?,
                quantity: from_be(&q)?,
                fee: from_be(&fee)?,
                fee_asset: asset,
                source_ts_us: ts,
            };
            orders[index].fills.insert(fill.fill_id.clone(), fill);
        }
        let positions = load_positions(&self.conn)?;
        let balances = load_balances(&self.conn)?;
        let funding_income = load_funding_income(&self.conn)?;
        Ok(JournalSnapshot {
            orders,
            positions,
            balances,
            funding_income,
        })
    }
    pub(crate) fn replace_account_facts(
        &mut self,
        positions: &[Position],
        balances: &[BalanceSnapshot],
        funding: &[FundingIncome],
    ) -> Result<(), JournalError> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM positions", [])?;
        tx.execute("DELETE FROM balances", [])?;
        tx.execute("DELETE FROM funding_income", [])?;
        for p in positions {
            tx.execute(
                "INSERT INTO positions VALUES(?1,?2,?3,?4)",
                params![
                    venue_code(p.venue),
                    p.symbol.base,
                    p.symbol.quote,
                    be(p.signed_quantity)
                ],
            )?;
        }
        for b in balances {
            if b.asset.is_empty() || b.source_ts_us <= 0 || b.available > b.total {
                return Err(JournalError::Invariant("invalid balance snapshot".into()));
            }
            tx.execute(
                "INSERT INTO balances VALUES(?1,?2,?3,?4,?5)",
                params![
                    venue_code(b.venue),
                    b.asset,
                    be(b.total),
                    be(b.available),
                    b.source_ts_us
                ],
            )?;
        }
        for f in funding {
            if f.income_id.is_empty() || f.source_ts_us <= 0 {
                return Err(JournalError::Invariant("invalid funding income".into()));
            }
            tx.execute(
                "INSERT INTO funding_income VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    venue_code(f.venue),
                    f.income_id,
                    f.symbol.base,
                    f.symbol.quote,
                    be(f.amount),
                    f.source_ts_us
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    pub(crate) fn mark_reconciled(
        &mut self,
        ids: &[(AdapterId, ClientOrderId)],
    ) -> Result<(), JournalError> {
        let tx = self.conn.transaction()?;
        for (venue, id) in ids {
            tx.execute(
                "UPDATE orders SET reconciled=1 WHERE venue=?1 AND client_id=?2",
                params![venue_code(*venue), &id.0],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    pub(crate) fn begin_reconciliation(
        &mut self,
        reason: &str,
        token: &str,
    ) -> Result<i64, JournalError> {
        if reason.is_empty() || token.is_empty() {
            return Err(JournalError::Conflict(
                "reconciliation identity is empty".into(),
            ));
        }
        self.conn.execute(
            "INSERT INTO reconciliation_runs(reason,snapshot_token,status,exact) VALUES(?1,?2,'started',0)",
            params![reason, token],
        )?;
        Ok(self.conn.last_insert_rowid())
    }
    pub(crate) fn finish_reconciliation(
        &mut self,
        run_id: i64,
        exact: bool,
    ) -> Result<(), JournalError> {
        let changed = self.conn.execute(
            "UPDATE reconciliation_runs SET status='complete',exact=?1 WHERE run_id=?2 AND status='started'",
            params![i64::from(exact), run_id],
        )?;
        if changed != 1 {
            return Err(JournalError::Invariant(
                "reconciliation run completion mismatch".into(),
            ));
        }
        Ok(())
    }
    pub fn reconciliation_run_count(&self) -> Result<u64, JournalError> {
        let count: i64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM reconciliation_runs", [], |r| r.get(0))?;
        u64::try_from(count).map_err(|_| JournalError::Invariant("negative run count".into()))
    }
}

fn load_positions(conn: &Connection) -> Result<Vec<Position>, JournalError> {
    let mut s = conn
        .prepare("SELECT venue,base,quote,signed_qty FROM positions ORDER BY venue,base,quote")?;
    let rows = s
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Vec<u8>>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(v, b, q, n)| {
            Ok(Position {
                venue: parse_venue(&v)?,
                symbol: CanonicalSymbol::new(b, q),
                signed_quantity: from_be(&n)?,
            })
        })
        .collect()
}
fn load_balances(conn: &Connection) -> Result<Vec<BalanceSnapshot>, JournalError> {
    let mut s = conn.prepare(
        "SELECT venue,asset,total,available,source_ts_us FROM balances ORDER BY venue,asset",
    )?;
    let rows = s
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Vec<u8>>(2)?,
                r.get::<_, Vec<u8>>(3)?,
                r.get::<_, i64>(4)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(v, a, t, x, ts)| {
            Ok(BalanceSnapshot {
                venue: parse_venue(&v)?,
                asset: a,
                total: from_be(&t)?,
                available: from_be(&x)?,
                source_ts_us: ts,
            })
        })
        .collect()
}
fn load_funding_income(conn: &Connection) -> Result<Vec<FundingIncome>, JournalError> {
    let mut s = conn.prepare("SELECT venue,income_id,base,quote,amount,source_ts_us FROM funding_income ORDER BY venue,income_id")?;
    let rows = s
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, Vec<u8>>(4)?,
                r.get::<_, i64>(5)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(v, id, b, q, n, ts)| {
            Ok(FundingIncome {
                venue: parse_venue(&v)?,
                income_id: id,
                symbol: CanonicalSymbol::new(b, q),
                amount: from_be(&n)?,
                source_ts_us: ts,
            })
        })
        .collect()
}

fn migrate(conn: &Connection) -> Result<(), JournalError> {
    conn.execute_batch("BEGIN IMMEDIATE; CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY); COMMIT;")?;
    let version: Option<i64> =
        conn.query_row("SELECT MAX(version) FROM schema_migrations", [], |r| {
            r.get(0)
        })?;
    if version.unwrap_or(0) > SCHEMA_VERSION {
        return Err(JournalError::FutureSchema(version.unwrap_or(0)));
    }
    if version.is_none() {
        conn.execute_batch("BEGIN IMMEDIATE;
CREATE TABLE orders(venue TEXT NOT NULL CHECK(venue IN ('upbit_spot','bithumb_spot','binance_spot','binance_usdm','bybit_linear')),client_id TEXT NOT NULL CHECK(length(client_id)>0),request_hash BLOB NOT NULL CHECK(length(request_hash)=32),base TEXT NOT NULL CHECK(length(base)>0),quote TEXT NOT NULL CHECK(length(quote)>0),side TEXT NOT NULL CHECK(side IN ('buy','sell')),order_type TEXT NOT NULL CHECK(order_type IN ('limit','market')),tif TEXT NOT NULL CHECK(tif IN ('gtc','ioc','fok','post_only')),quantity BLOB NOT NULL CHECK(length(quantity)=16),limit_price BLOB CHECK(limit_price IS NULL OR length(limit_price)=16),reduce_only INTEGER NOT NULL CHECK(reduce_only IN (0,1)),created_ts_us INTEGER NOT NULL CHECK(created_ts_us>0),venue_order_id TEXT,state TEXT NOT NULL CHECK(state IN ('intent','submitted','acknowledged','partial','filled','canceled','rejected','expired','reconcile')),attributed_qty BLOB NOT NULL CHECK(length(attributed_qty)=16),venue_cumulative_qty BLOB NOT NULL CHECK(length(venue_cumulative_qty)=16),cumulative_fee BLOB NOT NULL CHECK(length(cumulative_fee)=16),last_source_sequence BLOB CHECK(last_source_sequence IS NULL OR length(last_source_sequence)=8),last_status_state TEXT,last_status_qty BLOB CHECK(last_status_qty IS NULL OR length(last_status_qty)=16),last_status_venue_id TEXT,reconciled INTEGER NOT NULL CHECK(reconciled IN (0,1)),PRIMARY KEY(venue,client_id));
CREATE UNIQUE INDEX venue_order_unique ON orders(venue,venue_order_id) WHERE venue_order_id IS NOT NULL;
CREATE TABLE order_events(venue TEXT NOT NULL,event_id TEXT NOT NULL,client_id TEXT NOT NULL,payload_hash BLOB NOT NULL,PRIMARY KEY(venue,event_id),FOREIGN KEY(venue,client_id) REFERENCES orders(venue,client_id));
CREATE TABLE fills(venue TEXT NOT NULL,fill_id TEXT NOT NULL CHECK(length(fill_id)>0),client_id TEXT NOT NULL,venue_order_id TEXT,price BLOB NOT NULL CHECK(length(price)=16),quantity BLOB NOT NULL CHECK(length(quantity)=16),fee BLOB NOT NULL CHECK(length(fee)=16),fee_asset TEXT NOT NULL CHECK(length(fee_asset)>0),source_ts_us INTEGER NOT NULL CHECK(source_ts_us>0),payload_hash BLOB NOT NULL CHECK(length(payload_hash)=32),PRIMARY KEY(venue,fill_id),FOREIGN KEY(venue,client_id) REFERENCES orders(venue,client_id));
CREATE INDEX fills_by_order ON fills(venue,client_id);
CREATE TABLE positions(venue TEXT NOT NULL,base TEXT NOT NULL,quote TEXT NOT NULL,signed_qty BLOB NOT NULL CHECK(length(signed_qty)=16),PRIMARY KEY(venue,base,quote));
CREATE TABLE balances(venue TEXT NOT NULL,asset TEXT NOT NULL,total BLOB NOT NULL CHECK(length(total)=16),available BLOB NOT NULL CHECK(length(available)=16),source_ts_us INTEGER NOT NULL,PRIMARY KEY(venue,asset));
CREATE TABLE funding_income(venue TEXT NOT NULL,income_id TEXT NOT NULL,base TEXT NOT NULL,quote TEXT NOT NULL,amount BLOB NOT NULL CHECK(length(amount)=16),source_ts_us INTEGER NOT NULL,PRIMARY KEY(venue,income_id));
CREATE TABLE reconciliation_runs(run_id INTEGER PRIMARY KEY AUTOINCREMENT,reason TEXT NOT NULL,snapshot_token TEXT NOT NULL,status TEXT NOT NULL,exact INTEGER NOT NULL DEFAULT 0);
CREATE TABLE reconciliation_observations(run_id INTEGER NOT NULL,kind TEXT NOT NULL,ordinal INTEGER NOT NULL,payload_hash BLOB NOT NULL,PRIMARY KEY(run_id,kind,ordinal),FOREIGN KEY(run_id) REFERENCES reconciliation_runs(run_id));
INSERT INTO schema_migrations(version) VALUES(1); COMMIT;")?;
    }
    Ok(())
}

fn apply_event_tx(
    tx: &Transaction<'_>,
    venue: AdapterId,
    event_id: &str,
    client_id: &ClientOrderId,
    event: &OmsEvent,
) -> Result<CanonicalOrder, JournalError> {
    if event_id.is_empty() {
        return Err(JournalError::Conflict("empty event id".into()));
    }
    let order = load_order(tx, venue, client_id)?
        .ok_or_else(|| JournalError::Conflict("unknown client id".into()))?;
    let payload_hash = event_hash(event);
    if let Some((owner, existing)) = tx
        .query_row(
            "SELECT client_id,payload_hash FROM order_events WHERE venue=?1 AND event_id=?2",
            params![venue_code(venue), event_id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?)),
        )
        .optional()?
    {
        return if owner == client_id.0 && existing == payload_hash {
            Ok(order)
        } else {
            Err(JournalError::Conflict("event id payload conflict".into()))
        };
    }
    if let OmsEvent::Fill(fill) = event {
        if let Some((owner, hash)) = tx
            .query_row(
                "SELECT client_id,payload_hash FROM fills WHERE venue=?1 AND fill_id=?2",
                params![venue_code(fill.venue), fill.fill_id.0],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?)),
            )
            .optional()?
        {
            return if owner == fill.client_order_id.0 && hash == fill_hash(fill) {
                Ok(order)
            } else {
                Err(JournalError::Conflict(
                    "venue-global fill id conflict".into(),
                ))
            };
        }
    }
    let next = reduce_order(&order, event)?;
    tx.execute(
        "INSERT INTO order_events(venue,event_id,client_id,payload_hash) VALUES(?1,?2,?3,?4)",
        params![venue_code(venue), event_id, client_id.0, payload_hash],
    )?;
    if let OmsEvent::Fill(fill) = event {
        insert_fill(tx, fill)?;
        update_position(tx, &order.intent, fill.quantity)?;
    }
    update_order(tx, &next)?;
    Ok(next)
}

fn insert_intent(tx: &Transaction<'_>, o: &CanonicalOrder) -> Result<(), JournalError> {
    let existing: Option<Vec<u8>> = tx
        .query_row(
            "SELECT request_hash FROM orders WHERE venue=?1 AND client_id=?2",
            params![venue_code(o.intent.venue), o.intent.client_order_id.0],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(h) = existing {
        return if h == o.request_hash {
            Ok(())
        } else {
            Err(JournalError::Conflict(
                "client id reused for different intent".into(),
            ))
        };
    }
    tx.execute("INSERT INTO orders VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,NULL,?13,?14,?15,?16,NULL,NULL,NULL,NULL,0)",params![venue_code(o.intent.venue),o.intent.client_order_id.0,o.request_hash.to_vec(),o.intent.symbol.base,o.intent.symbol.quote,side_code(o.intent.side),type_code(o.intent.order_type),tif_code(o.intent.time_in_force),be(o.intent.quantity),o.intent.limit_price.map(be),i64::from(o.intent.reduce_only),o.intent.created_ts_us,state_code(o.state),be(0),be(0),be(0)])?;
    Ok(())
}
fn update_order(tx: &Transaction<'_>, o: &CanonicalOrder) -> Result<(), JournalError> {
    tx.execute("UPDATE orders SET venue_order_id=?1,state=?2,attributed_qty=?3,venue_cumulative_qty=?4,cumulative_fee=?5,last_source_sequence=?6,last_status_state=?7,last_status_qty=?8,last_status_venue_id=?9,reconciled=?10 WHERE venue=?11 AND client_id=?12",params![o.venue_order_id.as_ref().map(|v|&v.0),state_code(o.state),be(o.attributed_fill_quantity),be(o.venue_cumulative_quantity),be(o.cumulative_fee),o.last_source_sequence.map(|v|v.to_be_bytes().to_vec()),o.last_status.as_ref().map(|v|state_code(v.state)),o.last_status.as_ref().map(|v|be(v.cumulative_quantity)),o.last_status.as_ref().and_then(|v|v.venue_order_id.as_ref()).map(|v|&v.0),i64::from(o.reconciled),venue_code(o.intent.venue),o.intent.client_order_id.0])?;
    Ok(())
}
fn insert_fill(tx: &Transaction<'_>, f: &ExecutionFill) -> Result<(), JournalError> {
    tx.execute(
        "INSERT INTO fills VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        params![
            venue_code(f.venue),
            f.fill_id.0,
            f.client_order_id.0,
            f.venue_order_id.as_ref().map(|v| &v.0),
            be(f.price),
            be(f.quantity),
            be(f.fee),
            f.fee_asset,
            f.source_ts_us,
            fill_hash(f)
        ],
    )?;
    Ok(())
}
fn update_position(tx: &Transaction<'_>, i: &OrderIntent, q: i128) -> Result<(), JournalError> {
    let signed = if i.side == OrderSide::Buy { q } else { -q };
    let old: Option<Vec<u8>> = tx
        .query_row(
            "SELECT signed_qty FROM positions WHERE venue=?1 AND base=?2 AND quote=?3",
            params![venue_code(i.venue), i.symbol.base, i.symbol.quote],
            |r| r.get(0),
        )
        .optional()?;
    let next = old
        .map(|v| from_be(&v))
        .transpose()?
        .unwrap_or(0)
        .checked_add(signed)
        .ok_or(OmsError::Overflow)?;
    tx.execute("INSERT INTO positions VALUES(?1,?2,?3,?4) ON CONFLICT(venue,base,quote) DO UPDATE SET signed_qty=excluded.signed_qty",params![venue_code(i.venue),i.symbol.base,i.symbol.quote,be(next)])?;
    Ok(())
}

trait Queryable {
    fn query_row<T, F>(&self, sql: &str, p: impl rusqlite::Params, f: F) -> rusqlite::Result<T>
    where
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>;
    fn prepare(&self, sql: &str) -> rusqlite::Result<rusqlite::Statement<'_>>;
}
impl Queryable for Connection {
    fn query_row<T, F>(&self, s: &str, p: impl rusqlite::Params, f: F) -> rusqlite::Result<T>
    where
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        Connection::query_row(self, s, p, f)
    }
    fn prepare(&self, s: &str) -> rusqlite::Result<rusqlite::Statement<'_>> {
        Connection::prepare(self, s)
    }
}
impl Queryable for Transaction<'_> {
    fn query_row<T, F>(&self, s: &str, p: impl rusqlite::Params, f: F) -> rusqlite::Result<T>
    where
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        Connection::query_row(self, s, p, f)
    }
    fn prepare(&self, s: &str) -> rusqlite::Result<rusqlite::Statement<'_>> {
        Connection::prepare(self, s)
    }
}
fn load_order(
    q: &impl Queryable,
    venue: AdapterId,
    id: &ClientOrderId,
) -> Result<Option<CanonicalOrder>, JournalError> {
    let row=q.query_row("SELECT request_hash,base,quote,side,order_type,tif,quantity,limit_price,reduce_only,created_ts_us,venue_order_id,state,attributed_qty,venue_cumulative_qty,cumulative_fee,last_source_sequence,last_status_state,last_status_qty,last_status_venue_id,reconciled FROM orders WHERE venue=?1 AND client_id=?2",params![venue_code(venue),&id.0],|r|Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,String>(5)?,r.get::<_,Vec<u8>>(6)?,r.get::<_,Option<Vec<u8>>>(7)?,r.get::<_,i64>(8)?,r.get::<_,i64>(9)?,r.get::<_,Option<String>>(10)?,r.get::<_,String>(11)?,r.get::<_,Vec<u8>>(12)?,r.get::<_,Vec<u8>>(13)?,r.get::<_,Vec<u8>>(14)?,r.get::<_,Option<Vec<u8>>>(15)?,r.get::<_,Option<String>>(16)?,r.get::<_,Option<Vec<u8>>>(17)?,r.get::<_,Option<String>>(18)?,r.get::<_,i64>(19)?))).optional()?;
    let Some((
        hash,
        base,
        quote,
        side,
        kind,
        tif,
        qty,
        price,
        ro,
        ts,
        vid,
        state,
        aq,
        vq,
        fee,
        seq,
        last_state,
        last_qty,
        last_vid,
        rec,
    )) = row
    else {
        return Ok(None);
    };
    let intent = OrderIntent {
        venue,
        client_order_id: id.clone(),
        symbol: CanonicalSymbol::new(base, quote),
        side: parse_side(&side)?,
        order_type: parse_type(&kind)?,
        time_in_force: parse_tif(&tif)?,
        quantity: from_be(&qty)?,
        limit_price: price.map(|v| from_be(&v)).transpose()?,
        reduce_only: ro != 0,
        created_ts_us: ts,
    };
    let mut stmt=q.prepare("SELECT fill_id,venue_order_id,price,quantity,fee,fee_asset,source_ts_us FROM fills WHERE venue=?1 AND client_id=?2 ORDER BY fill_id")?;
    let fills = stmt
        .query_map(params![venue_code(intent.venue), id.0], |r| {
            Ok(ExecutionFill {
                venue: intent.venue,
                client_order_id: id.clone(),
                fill_id: FillId(r.get(0)?),
                venue_order_id: r.get::<_, Option<String>>(1)?.map(VenueOrderId),
                price: from_be_sql(r.get(2)?)?,
                quantity: from_be_sql(r.get(3)?)?,
                fee: from_be_sql(r.get(4)?)?,
                fee_asset: r.get(5)?,
                source_ts_us: r.get(6)?,
            })
        })?
        .map(|v| v.map(|f| (f.fill_id.clone(), f)))
        .collect::<Result<_, _>>()?;
    let last_status = match (last_state, last_qty) {
        (Some(s), Some(q)) => Some(crate::oms::StatusIdentity {
            state: parse_state(&s)?,
            cumulative_quantity: from_be(&q)?,
            venue_order_id: last_vid.map(VenueOrderId),
        }),
        (None, None) => None,
        _ => return Err(JournalError::Invariant("partial status identity".into())),
    };
    Ok(Some(CanonicalOrder {
        intent,
        request_hash: hash
            .try_into()
            .map_err(|_| JournalError::Invariant("bad request hash".into()))?,
        venue_order_id: vid.map(VenueOrderId),
        state: parse_state(&state)?,
        attributed_fill_quantity: from_be(&aq)?,
        venue_cumulative_quantity: from_be(&vq)?,
        cumulative_fee: from_be(&fee)?,
        fills,
        last_source_sequence: seq
            .map(|v: Vec<u8>| {
                v.try_into()
                    .map(u64::from_be_bytes)
                    .map_err(|_| JournalError::Invariant("bad u64".into()))
            })
            .transpose()?,
        last_status,
        reconciled: rec != 0,
    }))
}

fn event_hash(e: &OmsEvent) -> Vec<u8> {
    let mut b = b"OMS-EVENT\0\x01".to_vec();
    match e {
        OmsEvent::Submitted => b.push(0),
        OmsEvent::Acknowledged { venue_order_id } => {
            b.push(1);
            crate::oms::put_str(&mut b, &venue_order_id.0)
        }
        OmsEvent::Status {
            state,
            cumulative_quantity,
            source_sequence,
            venue_order_id,
        } => {
            b.push(2);
            b.push(*state as u8);
            b.extend(cumulative_quantity.to_be_bytes());
            put_opt_u64(&mut b, *source_sequence);
            put_opt_str(&mut b, venue_order_id.as_ref().map(|v| v.0.as_str()))
        }
        OmsEvent::Fill(f) => {
            b.push(3);
            b.extend(fill_bytes(f))
        }
        OmsEvent::UnknownSubmit => b.push(4),
    }
    digest(&SHA256, &b).as_ref().to_vec()
}
fn fill_hash(f: &ExecutionFill) -> Vec<u8> {
    digest(&SHA256, &fill_bytes(f)).as_ref().to_vec()
}
fn fill_bytes(f: &ExecutionFill) -> Vec<u8> {
    let mut b = b"OMS-FILL\0\x01".to_vec();
    b.push(venue_tag(f.venue));
    crate::oms::put_str(&mut b, &f.client_order_id.0);
    put_opt_str(&mut b, f.venue_order_id.as_ref().map(|v| v.0.as_str()));
    crate::oms::put_str(&mut b, &f.fill_id.0);
    b.extend(f.price.to_be_bytes());
    b.extend(f.quantity.to_be_bytes());
    b.extend(f.fee.to_be_bytes());
    crate::oms::put_str(&mut b, &f.fee_asset);
    b.extend(f.source_ts_us.to_be_bytes());
    b
}
fn put_opt_u64(b: &mut Vec<u8>, v: Option<u64>) {
    match v {
        Some(v) => {
            b.push(1);
            b.extend(v.to_be_bytes())
        }
        None => b.push(0),
    }
}
fn put_opt_str(b: &mut Vec<u8>, v: Option<&str>) {
    match v {
        Some(v) => {
            b.push(1);
            crate::oms::put_str(b, v)
        }
        None => b.push(0),
    }
}
fn be(v: i128) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}
fn from_be(v: &[u8]) -> Result<i128, JournalError> {
    Ok(i128::from_be_bytes(
        v.try_into()
            .map_err(|_| JournalError::Invariant("bad i128".into()))?,
    ))
}
fn from_be_sql(v: Vec<u8>) -> rusqlite::Result<i128> {
    i128::from_be_bytes(v.try_into().map_err(|_| rusqlite::Error::InvalidQuery)?).pipe(Ok)
}
trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}
impl<T> Pipe for T {}
fn venue_tag(v: AdapterId) -> u8 {
    match v {
        AdapterId::UpbitSpot => 0,
        AdapterId::BithumbSpot => 1,
        AdapterId::BinanceSpot => 2,
        AdapterId::BinanceUsdm => 3,
        AdapterId::BybitLinear => 4,
    }
}
fn venue_code(v: AdapterId) -> &'static str {
    match v {
        AdapterId::UpbitSpot => "upbit_spot",
        AdapterId::BithumbSpot => "bithumb_spot",
        AdapterId::BinanceSpot => "binance_spot",
        AdapterId::BinanceUsdm => "binance_usdm",
        AdapterId::BybitLinear => "bybit_linear",
    }
}
fn parse_venue(s: &str) -> Result<AdapterId, JournalError> {
    match s {
        "upbit_spot" => Ok(AdapterId::UpbitSpot),
        "bithumb_spot" => Ok(AdapterId::BithumbSpot),
        "binance_spot" => Ok(AdapterId::BinanceSpot),
        "binance_usdm" => Ok(AdapterId::BinanceUsdm),
        "bybit_linear" => Ok(AdapterId::BybitLinear),
        _ => Err(JournalError::Invariant("bad venue".into())),
    }
}
fn side_code(v: OrderSide) -> &'static str {
    match v {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}
fn parse_side(s: &str) -> Result<OrderSide, JournalError> {
    match s {
        "buy" => Ok(OrderSide::Buy),
        "sell" => Ok(OrderSide::Sell),
        _ => Err(JournalError::Invariant("bad side".into())),
    }
}
fn type_code(v: OrderType) -> &'static str {
    match v {
        OrderType::Limit => "limit",
        OrderType::Market => "market",
    }
}
fn parse_type(s: &str) -> Result<OrderType, JournalError> {
    match s {
        "limit" => Ok(OrderType::Limit),
        "market" => Ok(OrderType::Market),
        _ => Err(JournalError::Invariant("bad type".into())),
    }
}
fn tif_code(v: TimeInForce) -> &'static str {
    match v {
        TimeInForce::GoodTilCanceled => "gtc",
        TimeInForce::ImmediateOrCancel => "ioc",
        TimeInForce::FillOrKill => "fok",
        TimeInForce::PostOnly => "post_only",
    }
}
fn parse_tif(s: &str) -> Result<TimeInForce, JournalError> {
    match s {
        "gtc" => Ok(TimeInForce::GoodTilCanceled),
        "ioc" => Ok(TimeInForce::ImmediateOrCancel),
        "fok" => Ok(TimeInForce::FillOrKill),
        "post_only" => Ok(TimeInForce::PostOnly),
        _ => Err(JournalError::Invariant("bad tif".into())),
    }
}
fn state_code(v: OrderState) -> &'static str {
    match v {
        OrderState::Intent => "intent",
        OrderState::Submitted => "submitted",
        OrderState::Acknowledged => "acknowledged",
        OrderState::PartiallyFilled => "partial",
        OrderState::Filled => "filled",
        OrderState::Canceled => "canceled",
        OrderState::Rejected => "rejected",
        OrderState::Expired => "expired",
        OrderState::Reconcile => "reconcile",
    }
}
fn parse_state(s: &str) -> Result<OrderState, JournalError> {
    match s {
        "intent" => Ok(OrderState::Intent),
        "submitted" => Ok(OrderState::Submitted),
        "acknowledged" => Ok(OrderState::Acknowledged),
        "partial" => Ok(OrderState::PartiallyFilled),
        "filled" => Ok(OrderState::Filled),
        "canceled" => Ok(OrderState::Canceled),
        "rejected" => Ok(OrderState::Rejected),
        "expired" => Ok(OrderState::Expired),
        "reconcile" => Ok(OrderState::Reconcile),
        _ => Err(JournalError::Invariant("bad state".into())),
    }
}
