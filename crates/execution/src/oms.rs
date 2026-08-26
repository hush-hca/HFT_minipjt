use std::collections::BTreeMap;

use funding_core::execution::{ExecutionFill, FillId, OrderIntent, VenueOrderId};
use ring::digest::{SHA256, digest};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum OrderState {
    Intent,
    Submitted,
    Acknowledged,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
    Reconcile,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CanonicalOrder {
    pub intent: OrderIntent,
    pub request_hash: [u8; 32],
    pub venue_order_id: Option<VenueOrderId>,
    pub state: OrderState,
    pub attributed_fill_quantity: i128,
    pub venue_cumulative_quantity: i128,
    pub cumulative_fee: i128,
    pub fills: BTreeMap<FillId, ExecutionFill>,
    pub last_source_sequence: Option<u64>,
    pub last_status: Option<StatusIdentity>,
    pub reconciled: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct StatusIdentity {
    pub state: OrderState,
    pub cumulative_quantity: i128,
    pub venue_order_id: Option<VenueOrderId>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum OmsEvent {
    Submitted,
    Acknowledged {
        venue_order_id: VenueOrderId,
    },
    Status {
        state: OrderState,
        cumulative_quantity: i128,
        source_sequence: Option<u64>,
        venue_order_id: Option<VenueOrderId>,
    },
    Fill(ExecutionFill),
    UnknownSubmit,
}

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum OmsError {
    #[error("invalid intent: {0}")]
    InvalidIntent(&'static str),
    #[error("event identity does not match order")]
    IdentityMismatch,
    #[error("venue order identifier conflict")]
    VenueOrderConflict,
    #[error("fill identifier has conflicting payload")]
    FillConflict,
    #[error("cumulative quantity regressed")]
    CumulativeRegression,
    #[error("fill quantity exceeds intent")]
    Overfill,
    #[error("invalid order transition")]
    InvalidTransition,
    #[error("arithmetic overflow")]
    Overflow,
}

impl CanonicalOrder {
    pub fn new(intent: OrderIntent) -> Result<Self, OmsError> {
        validate_intent(&intent)?;
        Ok(Self {
            request_hash: intent_hash(&intent),
            intent,
            venue_order_id: None,
            state: OrderState::Intent,
            attributed_fill_quantity: 0,
            venue_cumulative_quantity: 0,
            cumulative_fee: 0,
            fills: BTreeMap::new(),
            last_source_sequence: None,
            last_status: None,
            reconciled: false,
        })
    }
    pub fn blocks_new_orders(&self) -> bool {
        self.state == OrderState::Reconcile
    }
}

pub fn reduce_order(order: &CanonicalOrder, event: &OmsEvent) -> Result<CanonicalOrder, OmsError> {
    let mut next = order.clone();
    apply(&mut next, event)?;
    Ok(next)
}

fn apply(order: &mut CanonicalOrder, event: &OmsEvent) -> Result<(), OmsError> {
    match event {
        OmsEvent::Submitted => {
            if order.state == OrderState::Intent {
                order.state = OrderState::Submitted;
                order.reconciled = false;
            }
        }
        OmsEvent::Acknowledged { venue_order_id } => {
            if venue_order_id.0.is_empty() {
                return Err(OmsError::InvalidTransition);
            }
            if is_terminal(order.state) {
                return if order.venue_order_id.as_ref() == Some(venue_order_id) {
                    Ok(())
                } else {
                    Err(OmsError::InvalidTransition)
                };
            }
            bind_venue_id(order, venue_order_id)?;
            if matches!(
                order.state,
                OrderState::Intent | OrderState::Submitted | OrderState::Reconcile
            ) {
                order.state = OrderState::Acknowledged;
                order.reconciled = false;
            }
        }
        OmsEvent::UnknownSubmit => {
            if order.state != OrderState::Submitted {
                return Err(OmsError::InvalidTransition);
            }
            order.state = OrderState::Reconcile;
            order.reconciled = false;
        }
        OmsEvent::Status {
            state,
            cumulative_quantity,
            source_sequence,
            venue_order_id,
        } => {
            if matches!(
                state,
                OrderState::Intent | OrderState::Reconcile | OrderState::Submitted
            ) {
                return Err(OmsError::InvalidTransition);
            }
            if venue_order_id.as_ref().is_some_and(|v| v.0.is_empty()) {
                return Err(OmsError::VenueOrderConflict);
            }
            if *cumulative_quantity < 0 {
                return Err(OmsError::CumulativeRegression);
            }
            if *cumulative_quantity > order.intent.quantity {
                return Err(OmsError::Overfill);
            }
            if (*state == OrderState::Filled && *cumulative_quantity != order.intent.quantity)
                || (*state == OrderState::Rejected && *cumulative_quantity != 0)
            {
                return Err(OmsError::InvalidTransition);
            }
            if order.state == OrderState::Rejected && *state != OrderState::Rejected {
                return Err(OmsError::InvalidTransition);
            }
            let identity = StatusIdentity {
                state: *state,
                cumulative_quantity: *cumulative_quantity,
                venue_order_id: venue_order_id.clone(),
            };
            if source_sequence.is_none() && order.last_source_sequence.is_some() {
                return if order.last_status.as_ref() == Some(&identity) {
                    Ok(())
                } else {
                    Err(OmsError::InvalidTransition)
                };
            }
            if let (Some(incoming), Some(last)) = (source_sequence, order.last_source_sequence) {
                if *incoming < last {
                    return Ok(());
                }
                if *incoming == last {
                    return if order.last_status.as_ref() == Some(&identity) {
                        Ok(())
                    } else {
                        Err(OmsError::InvalidTransition)
                    };
                }
            }
            if *cumulative_quantity < order.venue_cumulative_quantity {
                return Err(OmsError::CumulativeRegression);
            }
            if is_terminal(order.state)
                && *state != order.state
                && !(*state == OrderState::Filled && *cumulative_quantity == order.intent.quantity)
            {
                return Err(OmsError::InvalidTransition);
            }
            if !is_terminal(order.state)
                && !is_terminal(*state)
                && state_rank(*state) < state_rank(order.state)
            {
                return Err(OmsError::InvalidTransition);
            }
            if let Some(id) = venue_order_id {
                bind_venue_id(order, id)?;
            }
            order.venue_cumulative_quantity = *cumulative_quantity;
            if source_sequence.is_some() {
                order.last_source_sequence = *source_sequence;
            }
            order.last_status = Some(identity);
            order.reconciled = false;
            order.state = if *cumulative_quantity == order.intent.quantity {
                OrderState::Filled
            } else if *cumulative_quantity > 0 && !is_terminal(*state) {
                OrderState::PartiallyFilled
            } else {
                *state
            };
        }
        OmsEvent::Fill(fill) => {
            if fill.venue != order.intent.venue
                || fill.client_order_id != order.intent.client_order_id
            {
                return Err(OmsError::IdentityMismatch);
            }
            if order.state == OrderState::Rejected {
                return Err(OmsError::InvalidTransition);
            }
            if fill.fill_id.0.is_empty()
                || fill.venue_order_id.as_ref().is_some_and(|v| v.0.is_empty())
                || fill.fee_asset.is_empty()
                || fill.source_ts_us <= 0
            {
                return Err(OmsError::InvalidIntent(
                    "fill identity, asset, and timestamp are required",
                ));
            }
            if fill.quantity <= 0 || fill.price <= 0 {
                return Err(OmsError::InvalidIntent(
                    "fill price and quantity must be positive",
                ));
            }
            if let Some(existing) = order.fills.get(&fill.fill_id) {
                return if existing == fill {
                    Ok(())
                } else {
                    Err(OmsError::FillConflict)
                };
            }
            if let Some(id) = &fill.venue_order_id {
                bind_venue_id(order, id)?;
            }
            let quantity = order
                .attributed_fill_quantity
                .checked_add(fill.quantity)
                .ok_or(OmsError::Overflow)?;
            if quantity > order.intent.quantity {
                return Err(OmsError::Overfill);
            }
            order.cumulative_fee = order
                .cumulative_fee
                .checked_add(fill.fee)
                .ok_or(OmsError::Overflow)?;
            order.attributed_fill_quantity = quantity;
            order.venue_cumulative_quantity = order.venue_cumulative_quantity.max(quantity);
            order.fills.insert(fill.fill_id.clone(), fill.clone());
            order.reconciled = false;
            if quantity == order.intent.quantity {
                order.state = OrderState::Filled;
            } else if !is_terminal(order.state) {
                order.state = OrderState::PartiallyFilled;
            }
        }
    }
    Ok(())
}

fn bind_venue_id(order: &mut CanonicalOrder, id: &VenueOrderId) -> Result<(), OmsError> {
    match &order.venue_order_id {
        Some(current) if current != id => Err(OmsError::VenueOrderConflict),
        Some(_) => Ok(()),
        None => {
            order.venue_order_id = Some(id.clone());
            Ok(())
        }
    }
}
fn is_terminal(state: OrderState) -> bool {
    matches!(
        state,
        OrderState::Filled | OrderState::Canceled | OrderState::Rejected | OrderState::Expired
    )
}
fn state_rank(state: OrderState) -> u8 {
    match state {
        OrderState::Intent => 0,
        OrderState::Submitted => 1,
        OrderState::Acknowledged => 2,
        OrderState::PartiallyFilled => 3,
        OrderState::Reconcile => 4,
        OrderState::Filled | OrderState::Canceled | OrderState::Rejected | OrderState::Expired => 5,
    }
}
fn validate_intent(i: &OrderIntent) -> Result<(), OmsError> {
    if i.client_order_id.0.is_empty() || i.symbol.base.is_empty() || i.symbol.quote.is_empty() {
        return Err(OmsError::InvalidIntent("identity is empty"));
    }
    if i.quantity <= 0 || i.created_ts_us <= 0 {
        return Err(OmsError::InvalidIntent(
            "quantity/timestamp must be positive",
        ));
    }
    if matches!(i.order_type, funding_core::execution::OrderType::Limit)
        && i.limit_price.is_none_or(|v| v <= 0)
    {
        return Err(OmsError::InvalidIntent("limit price must be positive"));
    }
    Ok(())
}

pub fn intent_hash(i: &OrderIntent) -> [u8; 32] {
    let bytes = encode_intent(i);
    digest(&SHA256, &bytes)
        .as_ref()
        .try_into()
        .expect("sha256 length")
}
pub(crate) fn encode_intent(i: &OrderIntent) -> Vec<u8> {
    let mut b = b"OMS-INTENT\0\x01".to_vec();
    b.push(match i.venue {
        md_core::model::AdapterId::UpbitSpot => 0,
        md_core::model::AdapterId::BithumbSpot => 1,
        md_core::model::AdapterId::BinanceSpot => 2,
        md_core::model::AdapterId::BinanceUsdm => 3,
        md_core::model::AdapterId::BybitLinear => 4,
    });
    put_str(&mut b, &i.client_order_id.0);
    put_str(&mut b, &i.symbol.base);
    put_str(&mut b, &i.symbol.quote);
    b.push(i.side as u8);
    b.push(i.order_type as u8);
    b.push(i.time_in_force as u8);
    b.extend(i.quantity.to_be_bytes());
    match i.limit_price {
        Some(v) => {
            b.push(1);
            b.extend(v.to_be_bytes())
        }
        None => b.push(0),
    }
    b.push(u8::from(i.reduce_only));
    b.extend(i.created_ts_us.to_be_bytes());
    b
}
pub(crate) fn put_str(b: &mut Vec<u8>, s: &str) {
    b.extend((s.len() as u32).to_be_bytes());
    b.extend(s.as_bytes());
}
