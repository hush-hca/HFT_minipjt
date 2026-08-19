use arrow_array::{
    Array, Decimal128Array, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::DataType;
use chrono::{TimeZone, Utc};
use md_core::model::{
    AdapterId, BookSnapshot, CanonicalSymbol, EventMeta, PriceLevel, TakerSide, TimestampPrecision,
    TradeTick,
};
use md_storage::{BookBatchBuilder, SchemaContext, TradeBatchBuilder, book_schema, trade_schema};
use std::io::Cursor;
use uuid::Uuid;

fn context() -> SchemaContext {
    SchemaContext {
        adapter: AdapterId::BinanceSpot,
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        utc_hour: Utc.with_ymd_and_hms(2024, 9, 10, 1, 0, 0).unwrap(),
    }
}

fn meta(event_id: Uuid, sequence: Option<u64>) -> EventMeta {
    EventMeta {
        schema_version: 1,
        event_id,
        adapter: AdapterId::BinanceSpot,
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        source_symbol: "BTCUSDT".to_owned(),
        source_stream: "btcusdt@depth20@100ms".to_owned(),
        source_sequence: sequence,
        exchange_event_ts_us: Some(1_725_930_000_123_000),
        exchange_trade_ts_us: None,
        event_ts_precision: TimestampPrecision::Millisecond,
        trade_ts_precision: TimestampPrecision::Unavailable,
        local_recv_ts_us: 1_725_930_000_123_456,
        raw_size_bytes: 321,
    }
}

fn fixture_book(event_id: Uuid) -> BookSnapshot {
    BookSnapshot {
        meta: meta(event_id, Some(42)),
        bids: vec![
            PriceLevel {
                price: 60_000_000_000_000_000_000_000,
                quantity: 1_250_000_000_000_000_000,
            },
            PriceLevel {
                price: 59_999_000_000_000_000_000_000,
                quantity: 2_000_000_000_000_000_000,
            },
        ],
        asks: vec![
            PriceLevel {
                price: 60_001_000_000_000_000_000_000,
                quantity: 500_000_000_000_000_000,
            },
            PriceLevel {
                price: 60_002_000_000_000_000_000_000,
                quantity: 750_000_000_000_000_000,
            },
        ],
    }
}

fn fixture_trade(event_id: Uuid) -> TradeTick {
    let mut value = meta(event_id, None);
    value.source_stream = "btcusdt@trade".to_owned();
    value.exchange_event_ts_us = None;
    value.exchange_trade_ts_us = Some(1_725_930_000_456_000);
    value.event_ts_precision = TimestampPrecision::Unavailable;
    value.trade_ts_precision = TimestampPrecision::Millisecond;
    TradeTick {
        meta: value,
        trade_id: "9007199254740993".to_owned(),
        price: 60_000_500_000_000_000_000_000,
        quantity: 125_000_000_000_000_000,
        taker_side: TakerSide::Sell,
    }
}

fn ipc_round_trip(batch: &RecordBatch) -> RecordBatch {
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
    }
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
    reader.next().unwrap().unwrap()
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<T>()
        .unwrap()
}

#[test]
fn schemas_are_self_describing_and_lossless() {
    for schema in [book_schema(&context()), trade_schema(&context())] {
        assert_eq!(schema.metadata()["project"], "hft-market-data-collector");
        assert_eq!(schema.metadata()["schema_version"], "1");
        assert_eq!(schema.metadata()["timestamp_unit"], "microsecond");
        assert_eq!(schema.metadata()["decimal_scale"], "18");
        assert_eq!(schema.metadata()["exchange"], "binance");
        assert_eq!(schema.metadata()["market"], "spot");
        assert_eq!(schema.metadata()["symbol"], "BTC/USDT");
        assert_eq!(schema.metadata()["utc_hour"], "2024-09-10T01:00:00Z");
        assert_eq!(
            schema.field_with_name("event_id").unwrap().data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert_eq!(
            schema.field_with_name("price").unwrap().data_type(),
            &DataType::Decimal128(38, 18)
        );
        assert_eq!(
            schema.field_with_name("quantity").unwrap().data_type(),
            &DataType::Decimal128(38, 18)
        );
        assert!(
            schema
                .field_with_name("exchange_event_ts_us")
                .unwrap()
                .is_nullable()
        );
        assert!(
            schema
                .field_with_name("exchange_trade_ts_us")
                .unwrap()
                .is_nullable()
        );
    }
}

#[test]
fn book_batch_expands_snapshots_and_round_trips_all_values() {
    let first_id = Uuid::now_v7();
    let second_id = Uuid::now_v7();
    let first = fixture_book(first_id);
    let mut second = fixture_book(second_id);
    second.meta.exchange_event_ts_us = None;
    second.meta.event_ts_precision = TimestampPrecision::Unavailable;
    second.meta.source_sequence = None;
    second.bids.truncate(1);
    second.asks.truncate(1);

    let mut builder = BookBatchBuilder::new(context());
    builder.push(&first).unwrap();
    builder.push(&second).unwrap();
    assert_eq!(builder.len(), 6);
    let batch = builder.finish().unwrap();
    assert_eq!(builder.len(), 0);
    assert_eq!(batch.num_rows(), 6);

    let decoded = ipc_round_trip(&batch);
    assert_eq!(decoded.schema().metadata(), batch.schema().metadata());
    assert_eq!(
        column::<UInt16Array>(&decoded, "schema_version").values(),
        &[1; 6]
    );
    let ids = column::<FixedSizeBinaryArray>(&decoded, "event_id");
    assert_eq!(ids.value(0), first_id.as_bytes());
    assert_eq!(ids.value(3), first_id.as_bytes());
    assert_eq!(ids.value(4), second_id.as_bytes());
    assert_eq!(ids.value(5), second_id.as_bytes());
    assert_eq!(
        column::<StringArray>(&decoded, "exchange").value(0),
        "binance"
    );
    assert_eq!(column::<StringArray>(&decoded, "market").value(0), "spot");
    assert_eq!(
        column::<StringArray>(&decoded, "symbol").value(0),
        "BTC/USDT"
    );
    assert_eq!(
        column::<StringArray>(&decoded, "source_symbol").value(0),
        "BTCUSDT"
    );
    assert_eq!(
        column::<StringArray>(&decoded, "source_stream").value(0),
        "btcusdt@depth20@100ms"
    );
    let sequence = column::<UInt64Array>(&decoded, "source_sequence");
    assert_eq!(sequence.value(0), 42);
    assert!(sequence.is_null(4));
    let event_ts = column::<Int64Array>(&decoded, "exchange_event_ts_us");
    assert_eq!(event_ts.value(0), 1_725_930_000_123_000);
    assert!(event_ts.is_null(4));
    assert!(column::<Int64Array>(&decoded, "exchange_trade_ts_us").is_null(0));
    assert_eq!(
        column::<Int64Array>(&decoded, "local_recv_ts_us").value(0),
        1_725_930_000_123_456
    );
    assert_eq!(
        column::<UInt8Array>(&decoded, "event_ts_precision").values(),
        &[1, 1, 1, 1, 0, 0]
    );
    assert_eq!(
        column::<UInt8Array>(&decoded, "trade_ts_precision").values(),
        &[0; 6]
    );
    assert_eq!(
        column::<UInt32Array>(&decoded, "raw_size_bytes").values(),
        &[321; 6]
    );
    assert_eq!(
        column::<UInt8Array>(&decoded, "side").values(),
        &[0, 0, 1, 1, 0, 1]
    );
    assert_eq!(
        column::<UInt16Array>(&decoded, "level").values(),
        &[0, 1, 0, 1, 0, 0]
    );
    assert_eq!(
        column::<Decimal128Array>(&decoded, "price").values(),
        &[
            first.bids[0].price,
            first.bids[1].price,
            first.asks[0].price,
            first.asks[1].price,
            second.bids[0].price,
            second.asks[0].price,
        ]
    );
    assert_eq!(
        column::<Decimal128Array>(&decoded, "quantity").values(),
        &[
            first.bids[0].quantity,
            first.bids[1].quantity,
            first.asks[0].quantity,
            first.asks[1].quantity,
            second.bids[0].quantity,
            second.asks[0].quantity,
        ]
    );
}

#[test]
fn trade_batch_round_trips_nulls_decimal_extreme_and_event_identity() {
    let event_id = Uuid::now_v7();
    let mut trade = fixture_trade(event_id);
    trade.price = 99_999_999_999_999_999_999_999_999_999_999_999_999_i128;

    let mut builder = TradeBatchBuilder::new(context());
    builder.push(&trade).unwrap();
    assert_eq!(builder.len(), 1);
    let decoded = ipc_round_trip(&builder.finish().unwrap());

    assert_eq!(decoded.num_rows(), 1);
    assert_eq!(
        column::<FixedSizeBinaryArray>(&decoded, "event_id").value(0),
        event_id.as_bytes()
    );
    assert!(column::<UInt64Array>(&decoded, "source_sequence").is_null(0));
    assert!(column::<Int64Array>(&decoded, "exchange_event_ts_us").is_null(0));
    assert_eq!(
        column::<Int64Array>(&decoded, "exchange_trade_ts_us").value(0),
        1_725_930_000_456_000
    );
    assert_eq!(
        column::<StringArray>(&decoded, "trade_id").value(0),
        "9007199254740993"
    );
    assert_eq!(
        column::<Decimal128Array>(&decoded, "price").value(0),
        trade.price
    );
    assert_eq!(
        column::<Decimal128Array>(&decoded, "quantity").value(0),
        trade.quantity
    );
    assert_eq!(column::<UInt8Array>(&decoded, "taker_side").value(0), 2);
}

#[test]
fn invalid_or_wrong_context_book_is_rejected_without_partial_rows() {
    let mut invalid = fixture_book(Uuid::now_v7());
    invalid.asks[1].price = invalid.asks[0].price;
    let mut builder = BookBatchBuilder::new(context());
    builder.push(&fixture_book(Uuid::now_v7())).unwrap();
    let rows_before_rejection = builder.len();
    assert!(builder.push(&invalid).is_err());
    assert_eq!(builder.len(), rows_before_rejection);

    let mut wrong_context = fixture_book(Uuid::now_v7());
    wrong_context.meta.symbol = CanonicalSymbol::new("ETH", "USDT");
    assert!(builder.push(&wrong_context).is_err());
    assert_eq!(builder.len(), rows_before_rejection);
}

#[test]
fn invalid_trade_is_rejected_without_partial_rows() {
    let mut invalid = fixture_trade(Uuid::now_v7());
    invalid.quantity = 0;
    let mut builder = TradeBatchBuilder::new(context());
    assert!(builder.push(&invalid).is_err());
    assert_eq!(builder.len(), 0);
}

#[test]
fn out_of_decimal_range_is_rejected_before_finish() {
    let mut out_of_decimal_range = fixture_trade(Uuid::now_v7());
    out_of_decimal_range.price = i128::MAX;
    let mut builder = TradeBatchBuilder::new(context());
    assert!(builder.push(&out_of_decimal_range).is_err());
    assert_eq!(builder.len(), 0);
}
