use funding_core::execution::{ClientOrderId, OrderIntent, OrderSide, OrderType, TimeInForce};
use md_core::model::{AdapterId, CanonicalSymbol};

pub fn intent(id: &str, qty: i128) -> OrderIntent {
    OrderIntent {
        venue: AdapterId::BinanceUsdm,
        client_order_id: ClientOrderId(id.into()),
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        side: OrderSide::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::PostOnly,
        quantity: qty,
        limit_price: Some(100),
        reduce_only: false,
        created_ts_us: 1,
    }
}
