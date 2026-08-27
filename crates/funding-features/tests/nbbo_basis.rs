use funding_core::{
    config::ExactDecimal,
    feature::{
        BasisKind, BookComparisonValidity, BookFeatures, BookInvalidReason,
        EffectiveTimestampSource, ExecutableQuote, ExecutableQuoteSide, FeatureInvalidReason,
        FeatureSource, FeatureValidity, InstrumentKind, NamedPrice, NbboMarketState, NbboSide,
        NbboVenueExclusionReason, PriceKind, QuoteInvalidReason, QuoteValidity,
        StructuralBookValidity,
    },
};
use funding_features::basis::{NbboInput, basis_bps, compute_nbbo};
use md_core::model::{AdapterId, CanonicalSymbol};
use uuid::Uuid;

const ONE: i128 = ExactDecimal::SCALE;

fn exact_scaled(value: i128) -> ExactDecimal {
    ExactDecimal::from_scaled(value).unwrap()
}

fn price(integer: i128) -> ExactDecimal {
    exact_scaled(integer * ONE)
}

fn source(venue: AdapterId, symbol: &CanonicalSymbol, local_recv_ts_us: i64) -> FeatureSource {
    FeatureSource {
        event_id: Uuid::now_v7(),
        adapter: venue,
        symbol: symbol.clone(),
        source_sequence: Some(1),
        exchange_event_ts_us: Some(local_recv_ts_us - 10),
        exchange_trade_ts_us: None,
        local_recv_ts_us,
        effective_ts_us: local_recv_ts_us - 10,
        effective_ts_source: EffectiveTimestampSource::ExchangeEvent,
    }
}

fn quote(
    side: ExecutableQuoteSide,
    requested_base: ExactDecimal,
    available_base: ExactDecimal,
    average_price: ExactDecimal,
) -> ExecutableQuote {
    ExecutableQuote {
        side,
        requested_base,
        available_base,
        average_price: Some(average_price),
        quote_notional: Some(
            average_price
                .checked_mul(
                    requested_base,
                    funding_core::config::DecimalRounding::HalfAwayFromZero,
                )
                .unwrap(),
        ),
        levels_consumed: 2,
        validity: QuoteValidity::Valid,
    }
}

#[allow(clippy::too_many_arguments)]
fn book(
    venue: AdapterId,
    symbol: &CanonicalSymbol,
    local_recv_ts_us: i64,
    requested_base: ExactDecimal,
    sell_price: ExactDecimal,
    sell_available: ExactDecimal,
    buy_price: ExactDecimal,
    buy_available: ExactDecimal,
) -> BookFeatures {
    BookFeatures {
        source: source(venue, symbol, local_recv_ts_us),
        previous_book: None,
        structural_validity: StructuralBookValidity::Valid,
        comparison_validity: BookComparisonValidity::NotRequested,
        sell_into_bids: quote(
            ExecutableQuoteSide::SellIntoBids,
            requested_base,
            sell_available,
            sell_price,
        ),
        buy_from_asks: quote(
            ExecutableQuoteSide::BuyFromAsks,
            requested_base,
            buy_available,
            buy_price,
        ),
        mid: Some(price(100)),
        microprice: Some(price(100)),
        imbalance_1: Some(exact_scaled(0)),
        imbalance_5: Some(exact_scaled(0)),
        imbalance_10: Some(exact_scaled(0)),
        imbalance_20: Some(exact_scaled(0)),
        snapshot_ofi: None,
        depth_delta_bid: None,
        depth_delta_ask: None,
        depth_delta_bids: Vec::new(),
        depth_delta_asks: Vec::new(),
        validity: FeatureValidity::Valid,
    }
}

fn input<'a>(
    venue: AdapterId,
    kind: InstrumentKind,
    symbol: &'a CanonicalSymbol,
    book: &'a BookFeatures,
) -> NbboInput<'a> {
    NbboInput {
        venue,
        instrument_kind: kind,
        symbol,
        book,
    }
}

#[test]
fn executable_nbbo_selects_best_full_fill_wap_and_preserves_evidence() {
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let requested = price(2);
    let binance = book(
        AdapterId::BinanceUsdm,
        &symbol,
        9_900,
        requested,
        price(101),
        price(4),
        price(103),
        price(5),
    );
    let bybit = book(
        AdapterId::BybitLinear,
        &symbol,
        9_950,
        requested,
        price(102),
        price(3),
        exact_scaled(102 * ONE + ONE / 2),
        price(6),
    );

    let result = compute_nbbo(
        &[
            input(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                &symbol,
                &binance,
            ),
            input(
                AdapterId::BybitLinear,
                InstrumentKind::Perpetual,
                &symbol,
                &bybit,
            ),
        ],
        &symbol,
        InstrumentKind::Perpetual,
        requested,
        10_000,
        1_000,
    )
    .unwrap();

    let bid = result.bid.unwrap();
    let ask = result.ask.unwrap();
    assert_eq!(bid.venue, AdapterId::BybitLinear);
    assert_eq!(bid.side, NbboSide::Bid);
    assert_eq!(bid.price, price(102));
    assert_eq!(bid.available_base, price(3));
    assert_eq!(bid.source.event_id, bybit.source.event_id);
    assert_eq!(bid.age_us, 50);
    assert_eq!(ask.venue, AdapterId::BybitLinear);
    assert_eq!(ask.price, exact_scaled(102 * ONE + ONE / 2));
    assert_eq!(result.market_state, NbboMarketState::Normal);
    assert_eq!(result.requested_base, requested);
    assert!(result.exclusions.is_empty());
}

#[test]
fn nbbo_does_not_combine_partial_depth_across_venues() {
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let requested = price(1);
    let mut binance = book(
        AdapterId::BinanceUsdm,
        &symbol,
        100,
        requested,
        price(101),
        exact_scaled(6 * ONE / 10),
        price(103),
        price(2),
    );
    let mut bybit = book(
        AdapterId::BybitLinear,
        &symbol,
        100,
        requested,
        price(102),
        exact_scaled(6 * ONE / 10),
        price(104),
        price(2),
    );
    for quote in [&mut binance.sell_into_bids, &mut bybit.sell_into_bids] {
        quote.average_price = None;
        quote.quote_notional = None;
        quote.validity = QuoteValidity::Invalid(QuoteInvalidReason::InsufficientDepth {
            requested_base: requested,
            available_base: exact_scaled(6 * ONE / 10),
        });
    }

    let result = compute_nbbo(
        &[
            input(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                &symbol,
                &binance,
            ),
            input(
                AdapterId::BybitLinear,
                InstrumentKind::Perpetual,
                &symbol,
                &bybit,
            ),
        ],
        &symbol,
        InstrumentKind::Perpetual,
        requested,
        200,
        1_000,
    )
    .unwrap();

    assert!(result.bid.is_none());
    assert!(result.ask.is_some());
    assert_eq!(result.market_state, NbboMarketState::Incomplete);
    assert_eq!(result.exclusions.len(), 2);
    assert!(result.exclusions.iter().all(|item| {
        item.side == NbboSide::Bid
            && matches!(
                item.reason,
                NbboVenueExclusionReason::QuoteInvalid(
                    QuoteInvalidReason::InsufficientDepth { .. }
                )
            )
    }));
}

#[test]
fn nbbo_ties_keep_caller_order() {
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let requested = price(1);
    let first = book(
        AdapterId::BinanceUsdm,
        &symbol,
        990,
        requested,
        price(101),
        price(2),
        price(102),
        price(2),
    );
    let second = book(
        AdapterId::BybitLinear,
        &symbol,
        995,
        requested,
        price(101),
        price(2),
        price(102),
        price(2),
    );
    let result = compute_nbbo(
        &[
            input(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                &symbol,
                &first,
            ),
            input(
                AdapterId::BybitLinear,
                InstrumentKind::Perpetual,
                &symbol,
                &second,
            ),
        ],
        &symbol,
        InstrumentKind::Perpetual,
        requested,
        1_000,
        10,
    )
    .unwrap();

    assert_eq!(result.bid.unwrap().venue, AdapterId::BinanceUsdm);
    assert_eq!(result.ask.unwrap().venue, AdapterId::BinanceUsdm);
    assert!(result.exclusions.is_empty());
}

#[test]
fn nbbo_accepts_exact_freshness_boundary_and_labels_locked_and_crossed() {
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let requested = price(1);
    let locked_bid = book(
        AdapterId::BinanceUsdm,
        &symbol,
        900,
        requested,
        price(102),
        price(2),
        price(103),
        price(2),
    );
    let locked_ask = book(
        AdapterId::BybitLinear,
        &symbol,
        950,
        requested,
        price(101),
        price(2),
        price(102),
        price(2),
    );
    let locked_result = compute_nbbo(
        &[
            input(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                &symbol,
                &locked_bid,
            ),
            input(
                AdapterId::BybitLinear,
                InstrumentKind::Perpetual,
                &symbol,
                &locked_ask,
            ),
        ],
        &symbol,
        InstrumentKind::Perpetual,
        requested,
        1_000,
        100,
    )
    .unwrap();
    assert_eq!(locked_result.market_state, NbboMarketState::Locked);
    assert_eq!(locked_result.validity, FeatureValidity::Valid);

    let crossed_bid = book(
        AdapterId::BinanceUsdm,
        &symbol,
        950,
        requested,
        price(103),
        price(2),
        price(104),
        price(2),
    );
    let crossed = compute_nbbo(
        &[
            input(
                AdapterId::BybitLinear,
                InstrumentKind::Perpetual,
                &symbol,
                &locked_ask,
            ),
            input(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                &symbol,
                &crossed_bid,
            ),
        ],
        &symbol,
        InstrumentKind::Perpetual,
        requested,
        1_000,
        100,
    )
    .unwrap();
    assert_eq!(crossed.market_state, NbboMarketState::Crossed);
    assert_eq!(crossed.bid.unwrap().price, price(103));
    assert_eq!(crossed.ask.unwrap().price, price(102));
    assert_eq!(crossed.validity, FeatureValidity::Valid);
}

#[test]
fn nbbo_attributes_identity_and_venue_kind_exclusions_per_side() {
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let other_symbol = CanonicalSymbol::new("ETH", "USDT");
    let requested = price(1);
    let good = book(
        AdapterId::BinanceUsdm,
        &symbol,
        900,
        requested,
        price(100),
        price(2),
        price(101),
        price(2),
    );
    let venue_mismatch = book(
        AdapterId::BinanceUsdm,
        &symbol,
        900,
        requested,
        price(110),
        price(2),
        price(90),
        price(2),
    );
    let wrong_symbol = book(
        AdapterId::BybitLinear,
        &other_symbol,
        900,
        requested,
        price(110),
        price(2),
        price(90),
        price(2),
    );
    let venue_kind_mismatch = book(
        AdapterId::UpbitSpot,
        &symbol,
        900,
        requested,
        price(110),
        price(2),
        price(90),
        price(2),
    );

    let result = compute_nbbo(
        &[
            input(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                &symbol,
                &good,
            ),
            input(
                AdapterId::BybitLinear,
                InstrumentKind::Perpetual,
                &symbol,
                &venue_mismatch,
            ),
            input(
                AdapterId::BybitLinear,
                InstrumentKind::Perpetual,
                &other_symbol,
                &wrong_symbol,
            ),
            input(
                AdapterId::UpbitSpot,
                InstrumentKind::Perpetual,
                &symbol,
                &venue_kind_mismatch,
            ),
        ],
        &symbol,
        InstrumentKind::Perpetual,
        requested,
        1_000,
        100,
    )
    .unwrap();

    assert_eq!(result.bid.unwrap().price, price(100));
    assert_eq!(result.ask.unwrap().price, price(101));
    assert_eq!(result.exclusions.len(), 6);
    for side in [NbboSide::Bid, NbboSide::Ask] {
        assert!(result.exclusions.iter().any(|item| {
            item.side == side
                && matches!(
                    item.reason,
                    NbboVenueExclusionReason::VenueSourceMismatch { .. }
                )
        }));
        assert!(result.exclusions.iter().any(|item| {
            item.side == side
                && matches!(item.reason, NbboVenueExclusionReason::SymbolMismatch { .. })
        }));
        assert!(result.exclusions.iter().any(|item| {
            item.side == side
                && matches!(
                    item.reason,
                    NbboVenueExclusionReason::VenueInstrumentKindMismatch { .. }
                )
        }));
    }
}

#[test]
fn nbbo_rejects_duplicate_snapshots_and_internally_invalid_executable_books() {
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let requested = price(1);
    let good = book(
        AdapterId::BinanceUsdm,
        &symbol,
        900,
        requested,
        price(100),
        price(2),
        price(101),
        price(2),
    );
    let duplicate_a = book(
        AdapterId::BybitLinear,
        &symbol,
        910,
        requested,
        price(110),
        price(2),
        price(111),
        price(2),
    );
    let duplicate_b = book(
        AdapterId::BybitLinear,
        &symbol,
        920,
        requested,
        price(90),
        price(2),
        price(91),
        price(2),
    );
    let duplicates = compute_nbbo(
        &[
            input(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                &symbol,
                &good,
            ),
            input(
                AdapterId::BybitLinear,
                InstrumentKind::Perpetual,
                &symbol,
                &duplicate_a,
            ),
            input(
                AdapterId::BybitLinear,
                InstrumentKind::Perpetual,
                &symbol,
                &duplicate_b,
            ),
        ],
        &symbol,
        InstrumentKind::Perpetual,
        requested,
        1_000,
        100,
    )
    .unwrap();
    assert_eq!(duplicates.bid.unwrap().venue, AdapterId::BinanceUsdm);
    assert_eq!(duplicates.ask.unwrap().venue, AdapterId::BinanceUsdm);
    assert_eq!(duplicates.exclusions.len(), 4);
    assert!(
        duplicates
            .exclusions
            .iter()
            .all(|item| matches!(item.reason, NbboVenueExclusionReason::DuplicateMarketInput))
    );

    let mut structurally_invalid = duplicate_a.clone();
    structurally_invalid.structural_validity =
        StructuralBookValidity::Invalid(BookInvalidReason::CrossedBook);
    let structure = compute_nbbo(
        &[
            input(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                &symbol,
                &good,
            ),
            input(
                AdapterId::BybitLinear,
                InstrumentKind::Perpetual,
                &symbol,
                &structurally_invalid,
            ),
        ],
        &symbol,
        InstrumentKind::Perpetual,
        requested,
        1_000,
        100,
    )
    .unwrap();
    assert_eq!(structure.exclusions.len(), 2);
    assert!(structure.exclusions.iter().all(|item| matches!(
        item.reason,
        NbboVenueExclusionReason::StructuralBookInvalid(BookInvalidReason::CrossedBook)
    )));

    let mut inconsistent = duplicate_a;
    inconsistent.sell_into_bids.average_price = Some(price(102));
    inconsistent.buy_from_asks.average_price = Some(price(102));
    let consistency = compute_nbbo(
        &[
            input(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                &symbol,
                &good,
            ),
            input(
                AdapterId::BybitLinear,
                InstrumentKind::Perpetual,
                &symbol,
                &inconsistent,
            ),
        ],
        &symbol,
        InstrumentKind::Perpetual,
        requested,
        1_000,
        100,
    )
    .unwrap();
    assert_eq!(consistency.exclusions.len(), 2);
    assert!(consistency.exclusions.iter().all(|item| matches!(
        item.reason,
        NbboVenueExclusionReason::ExecutableBookLockedOrCrossed { .. }
    )));

    let wrong_request = book(
        AdapterId::BybitLinear,
        &symbol,
        900,
        price(2),
        price(100),
        price(3),
        price(101),
        price(3),
    );
    let quantity = compute_nbbo(
        &[
            input(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                &symbol,
                &good,
            ),
            input(
                AdapterId::BybitLinear,
                InstrumentKind::Perpetual,
                &symbol,
                &wrong_request,
            ),
        ],
        &symbol,
        InstrumentKind::Perpetual,
        requested,
        1_000,
        100,
    )
    .unwrap();
    assert_eq!(quantity.exclusions.len(), 2);
    assert!(quantity.exclusions.iter().all(|item| matches!(
        item.reason,
        NbboVenueExclusionReason::RequestedQuantityMismatch { .. }
    )));
}

#[test]
fn nbbo_rejects_nonpositive_request_and_negative_freshness_limit() {
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    assert!(matches!(
        compute_nbbo(
            &[],
            &symbol,
            InstrumentKind::Spot,
            exact_scaled(0),
            1_000,
            100,
        ),
        Err(FeatureInvalidReason::InvalidQuantity)
    ));
    assert!(matches!(
        compute_nbbo(&[], &symbol, InstrumentKind::Spot, price(1), 1_000, -1,),
        Err(FeatureInvalidReason::InvalidFreshnessLimit { limit_us: -1 })
    ));
}

fn named_price(
    venue: AdapterId,
    instrument_kind: InstrumentKind,
    kind: PriceKind,
    value: ExactDecimal,
    symbol: &CanonicalSymbol,
    local_recv_ts_us: i64,
) -> NamedPrice {
    NamedPrice {
        venue,
        instrument_kind,
        kind,
        value,
        source: source(venue, symbol, local_recv_ts_us),
    }
}

#[test]
fn basis_sign_and_denominator_are_always_compared_minus_spot_over_spot() {
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let spot = named_price(
        AdapterId::BinanceSpot,
        InstrumentKind::Spot,
        PriceKind::SpotMid,
        price(100),
        &symbol,
        900,
    );
    let perp_high = named_price(
        AdapterId::BinanceUsdm,
        InstrumentKind::Perpetual,
        PriceKind::PerpetualMid,
        price(110),
        &symbol,
        910,
    );
    let perp_low = named_price(
        AdapterId::BybitLinear,
        InstrumentKind::Perpetual,
        PriceKind::PerpetualMid,
        price(90),
        &symbol,
        920,
    );

    let positive = basis_bps(spot.clone(), perp_high, 1_000, 100).unwrap();
    let negative = basis_bps(spot, perp_low, 1_000, 100).unwrap();
    assert_eq!(
        positive.kind,
        BasisKind::IndicativePair {
            reference: PriceKind::SpotMid,
            compared: PriceKind::PerpetualMid,
        }
    );
    assert_eq!(positive.signed_price_difference, price(10));
    assert_eq!(positive.basis_bps, price(1_000));
    assert_eq!(negative.signed_price_difference, price(-10));
    assert_eq!(negative.basis_bps, price(-1_000));
    assert_eq!(positive.decision_ts_us, 1_000);
    assert_eq!(positive.freshness_limit_us, 100);
}

#[test]
fn all_ordered_indicative_and_executable_basis_pairs_have_explicit_kinds() {
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let indicative_pairs = [
        (PriceKind::SpotMid, PriceKind::Mark),
        (PriceKind::Mark, PriceKind::SpotMid),
        (PriceKind::PerpetualMid, PriceKind::Mark),
        (PriceKind::Mark, PriceKind::PerpetualMid),
        (PriceKind::PerpetualMid, PriceKind::Index),
        (PriceKind::Index, PriceKind::PerpetualMid),
        (PriceKind::Mark, PriceKind::Index),
        (PriceKind::Index, PriceKind::Mark),
        (PriceKind::PerpetualMid, PriceKind::PerpetualMid),
    ];
    for (reference_kind, compared_kind) in indicative_pairs {
        let (reference_venue, reference_instrument) = if reference_kind == PriceKind::SpotMid {
            (AdapterId::BinanceSpot, InstrumentKind::Spot)
        } else {
            (AdapterId::BinanceUsdm, InstrumentKind::Perpetual)
        };
        let (compared_venue, compared_instrument) = if compared_kind == PriceKind::SpotMid {
            (AdapterId::BinanceSpot, InstrumentKind::Spot)
        } else {
            (AdapterId::BybitLinear, InstrumentKind::Perpetual)
        };
        let result = basis_bps(
            named_price(
                reference_venue,
                reference_instrument,
                reference_kind,
                price(100),
                &symbol,
                990,
            ),
            named_price(
                compared_venue,
                compared_instrument,
                compared_kind,
                price(101),
                &symbol,
                995,
            ),
            1_000,
            10,
        )
        .unwrap();
        assert_eq!(
            result.kind,
            BasisKind::IndicativePair {
                reference: reference_kind,
                compared: compared_kind,
            }
        );
        assert_eq!(result.signed_price_difference, price(1));
        assert_eq!(result.basis_bps, price(100));
    }

    let executable_cases = [
        (
            PriceKind::SpotBuyFromAsks,
            PriceKind::PerpetualSellIntoBids,
            BasisKind::ExecutableEntry,
        ),
        (
            PriceKind::SpotSellIntoBids,
            PriceKind::PerpetualBuyFromAsks,
            BasisKind::ExecutableExit,
        ),
        (
            PriceKind::PerpetualBuyFromAsks,
            PriceKind::PerpetualSellIntoBids,
            BasisKind::ExecutableEntry,
        ),
        (
            PriceKind::PerpetualSellIntoBids,
            PriceKind::PerpetualBuyFromAsks,
            BasisKind::ExecutableExit,
        ),
    ];

    for (reference_kind, compared_kind, expected_kind) in executable_cases {
        let reference_is_spot = matches!(
            reference_kind,
            PriceKind::SpotBuyFromAsks | PriceKind::SpotSellIntoBids
        );
        let reference = named_price(
            if reference_is_spot {
                AdapterId::BinanceSpot
            } else {
                AdapterId::BinanceUsdm
            },
            if reference_is_spot {
                InstrumentKind::Spot
            } else {
                InstrumentKind::Perpetual
            },
            reference_kind,
            price(100),
            &symbol,
            990,
        );
        let compared = named_price(
            AdapterId::BybitLinear,
            InstrumentKind::Perpetual,
            compared_kind,
            price(101),
            &symbol,
            995,
        );
        let result = basis_bps(reference, compared, 1_000, 10).unwrap();
        assert_eq!(result.kind, expected_kind);
        assert_eq!(result.signed_price_difference, price(1));
        assert_eq!(result.basis_bps, price(100));
    }
}

#[test]
fn basis_rejects_invalid_clock_identity_price_pair_and_true_output_overflow() {
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let other_symbol = CanonicalSymbol::new("ETH", "USDT");
    let valid_spot = named_price(
        AdapterId::BinanceSpot,
        InstrumentKind::Spot,
        PriceKind::SpotMid,
        price(100),
        &symbol,
        900,
    );

    assert!(matches!(
        basis_bps(
            valid_spot.clone(),
            named_price(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                PriceKind::PerpetualMid,
                price(101),
                &symbol,
                901,
            ),
            1_000,
            -1,
        ),
        Err(FeatureInvalidReason::InvalidFreshnessLimit { limit_us: -1 })
    ));
    assert!(matches!(
        basis_bps(
            valid_spot.clone(),
            named_price(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                PriceKind::PerpetualMid,
                price(101),
                &symbol,
                1_001,
            ),
            1_000,
            100,
        ),
        Err(FeatureInvalidReason::FutureTimestamp { .. })
    ));
    assert!(matches!(
        basis_bps(
            valid_spot.clone(),
            named_price(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                PriceKind::PerpetualMid,
                price(101),
                &other_symbol,
                901,
            ),
            1_000,
            100,
        ),
        Err(FeatureInvalidReason::SymbolMismatch { .. })
    ));
    assert!(matches!(
        basis_bps(
            valid_spot.clone(),
            named_price(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                PriceKind::Mark,
                exact_scaled(0),
                &symbol,
                901,
            ),
            1_000,
            100,
        ),
        Err(FeatureInvalidReason::NonPositiveValue)
    ));
    assert!(matches!(
        basis_bps(
            valid_spot.clone(),
            named_price(
                AdapterId::BinanceUsdm,
                InstrumentKind::Perpetual,
                PriceKind::PerpetualBuyFromAsks,
                price(101),
                &symbol,
                901,
            ),
            1_000,
            100,
        ),
        Err(FeatureInvalidReason::InvalidBasisPair)
    ));

    let mut venue_mismatch = named_price(
        AdapterId::BinanceUsdm,
        InstrumentKind::Perpetual,
        PriceKind::PerpetualMid,
        price(101),
        &symbol,
        901,
    );
    venue_mismatch.source.adapter = AdapterId::BybitLinear;
    assert!(matches!(
        basis_bps(valid_spot.clone(), venue_mismatch, 1_000, 100),
        Err(FeatureInvalidReason::VenueSourceMismatch { .. })
    ));

    let kind_mismatch = named_price(
        AdapterId::BinanceSpot,
        InstrumentKind::Spot,
        PriceKind::PerpetualMid,
        price(101),
        &symbol,
        901,
    );
    assert!(matches!(
        basis_bps(valid_spot.clone(), kind_mismatch, 1_000, 100),
        Err(FeatureInvalidReason::PriceKindInstrumentMismatch)
    ));

    let same_market_same_kind = named_price(
        AdapterId::BinanceSpot,
        InstrumentKind::Spot,
        PriceKind::SpotMid,
        price(101),
        &symbol,
        901,
    );
    assert!(matches!(
        basis_bps(valid_spot.clone(), same_market_same_kind, 1_000, 100),
        Err(FeatureInvalidReason::InvalidBasisPair)
    ));

    let venue_kind_mismatch = named_price(
        AdapterId::BinanceSpot,
        InstrumentKind::Perpetual,
        PriceKind::PerpetualMid,
        price(101),
        &symbol,
        901,
    );
    assert!(matches!(
        basis_bps(valid_spot.clone(), venue_kind_mismatch, 1_000, 100),
        Err(FeatureInvalidReason::VenueInstrumentKindMismatch { .. })
    ));

    let tiny_spot = named_price(
        AdapterId::BinanceSpot,
        InstrumentKind::Spot,
        PriceKind::SpotMid,
        exact_scaled(1),
        &symbol,
        900,
    );
    let huge_perp = named_price(
        AdapterId::BinanceUsdm,
        InstrumentKind::Perpetual,
        PriceKind::PerpetualMid,
        exact_scaled(ExactDecimal::MAX_COEFFICIENT),
        &symbol,
        900,
    );
    assert!(matches!(
        basis_bps(tiny_spot, huge_perp, 1_000, 100),
        Err(FeatureInvalidReason::ArithmeticOverflow)
    ));
}

#[test]
fn basis_big_integer_intermediate_does_not_false_overflow() {
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let reference_value = exact_scaled(ExactDecimal::MAX_COEFFICIENT - 10_000 * ONE);
    let compared_value = exact_scaled(ExactDecimal::MAX_COEFFICIENT);
    let result = basis_bps(
        named_price(
            AdapterId::BinanceSpot,
            InstrumentKind::Spot,
            PriceKind::SpotMid,
            reference_value,
            &symbol,
            900,
        ),
        named_price(
            AdapterId::BinanceUsdm,
            InstrumentKind::Perpetual,
            PriceKind::PerpetualMid,
            compared_value,
            &symbol,
            900,
        ),
        1_000,
        100,
    )
    .unwrap();
    assert!(result.basis_bps.scaled() > 0);
}
