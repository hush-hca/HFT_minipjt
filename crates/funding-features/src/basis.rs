use funding_core::{
    config::{DecimalRounding, ExactDecimal},
    feature::{
        BasisFeature, BasisKind, BookFeatures, ExecutableQuote, ExecutableQuoteSide,
        FeatureInvalidReason, FeatureValidity, InstrumentKind, NamedPrice, NbboFeature,
        NbboMarketState, NbboQuote, NbboSide, NbboVenueExclusion, NbboVenueExclusionReason,
        PriceKind, QuoteValidity, StructuralBookValidity,
    },
};
use md_core::model::{AdapterId, CanonicalSymbol};
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};

/// One venue's already-computed, full-fill executable book features.
///
/// The explicit identity fields intentionally duplicate `book.source`: callers
/// must state the intended market and mismatches are retained as exclusions.
#[derive(Debug, Clone, Copy)]
pub struct NbboInput<'a> {
    pub venue: AdapterId,
    pub instrument_kind: InstrumentKind,
    pub symbol: &'a CanonicalSymbol,
    pub book: &'a BookFeatures,
}

pub fn compute_nbbo(
    inputs: &[NbboInput<'_>],
    symbol: &CanonicalSymbol,
    instrument_kind: InstrumentKind,
    requested_base: ExactDecimal,
    decision_ts_us: i64,
    freshness_limit_us: i64,
) -> Result<NbboFeature, FeatureInvalidReason> {
    if requested_base.scaled() <= 0 {
        return Err(FeatureInvalidReason::InvalidQuantity);
    }
    if freshness_limit_us < 0 {
        return Err(FeatureInvalidReason::InvalidFreshnessLimit {
            limit_us: freshness_limit_us,
        });
    }

    let mut bid = None;
    let mut ask = None;
    let mut exclusions = Vec::new();
    let duplicate_markets = duplicate_markets(inputs);

    for input in inputs {
        evaluate_side(
            input,
            symbol,
            instrument_kind,
            requested_base,
            decision_ts_us,
            freshness_limit_us,
            NbboSide::Bid,
            duplicate_markets.contains(&market_key(input)),
            &mut bid,
            &mut exclusions,
        );
        evaluate_side(
            input,
            symbol,
            instrument_kind,
            requested_base,
            decision_ts_us,
            freshness_limit_us,
            NbboSide::Ask,
            duplicate_markets.contains(&market_key(input)),
            &mut ask,
            &mut exclusions,
        );
    }

    let market_state = match (&bid, &ask) {
        (Some(best_bid), Some(best_ask)) if best_bid.price > best_ask.price => {
            NbboMarketState::Crossed
        }
        (Some(best_bid), Some(best_ask)) if best_bid.price == best_ask.price => {
            NbboMarketState::Locked
        }
        (Some(_), Some(_)) => NbboMarketState::Normal,
        _ => NbboMarketState::Incomplete,
    };
    let validity = if market_state == NbboMarketState::Incomplete {
        FeatureValidity::Invalid(if inputs.is_empty() {
            FeatureInvalidReason::NoInput
        } else {
            FeatureInvalidReason::MissingBook
        })
    } else {
        FeatureValidity::Valid
    };

    Ok(NbboFeature {
        symbol: symbol.clone(),
        instrument_kind,
        requested_base,
        decision_ts_us,
        freshness_limit_us,
        bid,
        ask,
        market_state,
        exclusions,
        validity,
    })
}

#[allow(clippy::too_many_arguments)]
fn evaluate_side(
    input: &NbboInput<'_>,
    target_symbol: &CanonicalSymbol,
    target_kind: InstrumentKind,
    requested_base: ExactDecimal,
    decision_ts_us: i64,
    freshness_limit_us: i64,
    side: NbboSide,
    duplicate_market: bool,
    winner: &mut Option<NbboQuote>,
    exclusions: &mut Vec<NbboVenueExclusion>,
) {
    let source = &input.book.source;
    let common_reason = common_exclusion(
        input,
        target_symbol,
        target_kind,
        decision_ts_us,
        freshness_limit_us,
        duplicate_market,
    );
    if let Some(reason) = common_reason {
        exclusions.push(exclusion(input, side, reason));
        return;
    }

    let (quote, expected_side) = match side {
        NbboSide::Bid => (
            &input.book.sell_into_bids,
            ExecutableQuoteSide::SellIntoBids,
        ),
        NbboSide::Ask => (&input.book.buy_from_asks, ExecutableQuoteSide::BuyFromAsks),
    };
    if quote.side != expected_side {
        exclusions.push(exclusion(
            input,
            side,
            NbboVenueExclusionReason::QuoteSideMismatch {
                expected: expected_side,
                actual: quote.side,
            },
        ));
        return;
    }
    if quote.requested_base != requested_base {
        exclusions.push(exclusion(
            input,
            side,
            NbboVenueExclusionReason::RequestedQuantityMismatch {
                expected: requested_base,
                actual: quote.requested_base,
            },
        ));
        return;
    }
    if let QuoteValidity::Invalid(reason) = &quote.validity {
        exclusions.push(exclusion(
            input,
            side,
            NbboVenueExclusionReason::QuoteInvalid(reason.clone()),
        ));
        return;
    }
    if quote.available_base < requested_base {
        exclusions.push(exclusion(
            input,
            side,
            NbboVenueExclusionReason::InsufficientDepth {
                requested_base,
                available_base: quote.available_base,
            },
        ));
        return;
    }
    let Some(price) = quote.average_price else {
        exclusions.push(exclusion(
            input,
            side,
            NbboVenueExclusionReason::MissingAveragePrice,
        ));
        return;
    };
    if price.scaled() <= 0 {
        exclusions.push(exclusion(
            input,
            side,
            NbboVenueExclusionReason::NonPositivePrice,
        ));
        return;
    }

    let candidate = selected_quote(
        input,
        side,
        price,
        quote,
        decision_ts_us - source.local_recv_ts_us,
    );
    // Strict comparisons make caller order the stable tie-break; no venue
    // preference is invented inside the feature engine.
    let replace = winner.as_ref().is_none_or(|current| match side {
        NbboSide::Bid => candidate.price > current.price,
        NbboSide::Ask => candidate.price < current.price,
    });
    if replace {
        *winner = Some(candidate);
    }
}

fn common_exclusion(
    input: &NbboInput<'_>,
    target_symbol: &CanonicalSymbol,
    target_kind: InstrumentKind,
    decision_ts_us: i64,
    freshness_limit_us: i64,
    duplicate_market: bool,
) -> Option<NbboVenueExclusionReason> {
    let source = &input.book.source;
    if input.venue != source.adapter {
        return Some(NbboVenueExclusionReason::VenueSourceMismatch {
            declared_venue: input.venue,
            source_venue: source.adapter,
        });
    }
    if input.symbol != &source.symbol {
        return Some(NbboVenueExclusionReason::SourceSymbolMismatch {
            declared_symbol: input.symbol.clone(),
            source_symbol: source.symbol.clone(),
        });
    }
    if input.symbol != target_symbol {
        return Some(NbboVenueExclusionReason::SymbolMismatch {
            expected: target_symbol.clone(),
            actual: input.symbol.clone(),
        });
    }
    if input.instrument_kind != target_kind {
        return Some(NbboVenueExclusionReason::InstrumentKindMismatch {
            expected: target_kind,
            actual: input.instrument_kind,
        });
    }
    if !venue_supports_instrument(input.venue, input.instrument_kind) {
        return Some(NbboVenueExclusionReason::VenueInstrumentKindMismatch {
            venue: input.venue,
            instrument_kind: input.instrument_kind,
        });
    }
    if duplicate_market {
        return Some(NbboVenueExclusionReason::DuplicateMarketInput);
    }
    if let StructuralBookValidity::Invalid(reason) = &input.book.structural_validity {
        return Some(NbboVenueExclusionReason::StructuralBookInvalid(
            reason.clone(),
        ));
    }
    if let FeatureValidity::Invalid(reason) = &input.book.validity {
        return Some(NbboVenueExclusionReason::FeatureInvalid(reason.clone()));
    }
    if let (QuoteValidity::Valid, Some(sell_price), QuoteValidity::Valid, Some(buy_price)) = (
        &input.book.sell_into_bids.validity,
        input.book.sell_into_bids.average_price,
        &input.book.buy_from_asks.validity,
        input.book.buy_from_asks.average_price,
    ) && sell_price >= buy_price
    {
        return Some(NbboVenueExclusionReason::ExecutableBookLockedOrCrossed {
            sell_price,
            buy_price,
        });
    }
    if source.local_recv_ts_us > decision_ts_us {
        return Some(NbboVenueExclusionReason::FutureTimestamp {
            source_ts_us: source.local_recv_ts_us,
            decision_ts_us,
        });
    }
    let age_us = decision_ts_us - source.local_recv_ts_us;
    if age_us > freshness_limit_us {
        return Some(NbboVenueExclusionReason::Stale {
            age_us,
            limit_us: freshness_limit_us,
        });
    }
    None
}

fn duplicate_markets(
    inputs: &[NbboInput<'_>],
) -> HashSet<(AdapterId, InstrumentKind, CanonicalSymbol)> {
    let mut seen = HashSet::with_capacity(inputs.len());
    let mut duplicates = HashSet::new();
    for input in inputs {
        let key = market_key(input);
        if !seen.insert(key.clone()) {
            duplicates.insert(key);
        }
    }
    duplicates
}

fn market_key(input: &NbboInput<'_>) -> (AdapterId, InstrumentKind, CanonicalSymbol) {
    (input.venue, input.instrument_kind, input.symbol.clone())
}

fn exclusion(
    input: &NbboInput<'_>,
    side: NbboSide,
    reason: NbboVenueExclusionReason,
) -> NbboVenueExclusion {
    NbboVenueExclusion {
        venue: input.venue,
        instrument_kind: input.instrument_kind,
        symbol: input.symbol.clone(),
        side,
        source: input.book.source.clone(),
        reason,
    }
}

fn selected_quote(
    input: &NbboInput<'_>,
    side: NbboSide,
    price: ExactDecimal,
    quote: &ExecutableQuote,
    age_us: i64,
) -> NbboQuote {
    NbboQuote {
        venue: input.venue,
        instrument_kind: input.instrument_kind,
        symbol: input.symbol.clone(),
        side,
        price,
        requested_base: quote.requested_base,
        available_base: quote.available_base,
        quote_notional: quote.quote_notional,
        levels_consumed: quote.levels_consumed,
        age_us,
        source: input.book.source.clone(),
    }
}

/// Computes signed basis points as
/// `(compared - reference) * 10_000 / reference`.
///
/// Both values are Decimal128(38,18). The unscaled expression is evaluated
/// with arbitrary-precision intermediates and rounded once, half away from
/// zero, into the scale-18 output.
pub fn basis_bps(
    reference: NamedPrice,
    compared: NamedPrice,
    decision_ts_us: i64,
    freshness_limit_us: i64,
) -> Result<BasisFeature, FeatureInvalidReason> {
    if freshness_limit_us < 0 {
        return Err(FeatureInvalidReason::InvalidFreshnessLimit {
            limit_us: freshness_limit_us,
        });
    }
    validate_named_price(&reference, decision_ts_us, freshness_limit_us)?;
    validate_named_price(&compared, decision_ts_us, freshness_limit_us)?;
    if reference.source.symbol != compared.source.symbol {
        return Err(FeatureInvalidReason::SymbolMismatch {
            expected: reference.source.symbol.clone(),
            actual: compared.source.symbol.clone(),
        });
    }
    if reference.venue == compared.venue && reference.kind == compared.kind {
        return Err(FeatureInvalidReason::InvalidBasisPair);
    }
    let kind =
        basis_kind(reference.kind, compared.kind).ok_or(FeatureInvalidReason::InvalidBasisPair)?;

    let signed_price_difference = compared
        .value
        .checked_sub(reference.value)
        .map_err(|_| FeatureInvalidReason::ArithmeticOverflow)?;
    let difference = BigInt::from(compared.value.scaled()) - reference.value.scaled();
    let numerator = difference * BigInt::from(10_000_i128) * BigInt::from(ExactDecimal::SCALE);
    let basis_bps = exact_ratio(
        numerator,
        BigInt::from(reference.value.scaled()),
        DecimalRounding::HalfAwayFromZero,
    )?;

    Ok(BasisFeature {
        symbol: reference.source.symbol.clone(),
        kind,
        reference,
        compared,
        signed_price_difference,
        basis_bps,
        decision_ts_us,
        freshness_limit_us,
        validity: FeatureValidity::Valid,
    })
}

fn validate_named_price(
    price: &NamedPrice,
    decision_ts_us: i64,
    freshness_limit_us: i64,
) -> Result<(), FeatureInvalidReason> {
    if price.venue != price.source.adapter {
        return Err(FeatureInvalidReason::VenueSourceMismatch {
            declared_venue: price.venue,
            source_venue: price.source.adapter,
        });
    }
    if !venue_supports_instrument(price.venue, price.instrument_kind) {
        return Err(FeatureInvalidReason::VenueInstrumentKindMismatch {
            venue: price.venue,
            instrument_kind: price.instrument_kind,
        });
    }
    if !price_kind_matches_instrument(price.kind, price.instrument_kind) {
        return Err(FeatureInvalidReason::PriceKindInstrumentMismatch);
    }
    if price.value.scaled() <= 0 {
        return Err(FeatureInvalidReason::NonPositiveValue);
    }
    if price.source.local_recv_ts_us > decision_ts_us {
        return Err(FeatureInvalidReason::FutureTimestamp {
            source_ts_us: price.source.local_recv_ts_us,
            decision_ts_us,
        });
    }
    let age_us = decision_ts_us - price.source.local_recv_ts_us;
    if age_us > freshness_limit_us {
        return Err(FeatureInvalidReason::Stale {
            age_us,
            limit_us: freshness_limit_us,
        });
    }
    Ok(())
}

fn price_kind_matches_instrument(kind: PriceKind, instrument: InstrumentKind) -> bool {
    matches!(
        (kind, instrument),
        (
            PriceKind::SpotMid | PriceKind::SpotSellIntoBids | PriceKind::SpotBuyFromAsks,
            InstrumentKind::Spot
        ) | (
            PriceKind::PerpetualMid
                | PriceKind::Mark
                | PriceKind::Index
                | PriceKind::PerpetualSellIntoBids
                | PriceKind::PerpetualBuyFromAsks,
            InstrumentKind::Perpetual
        )
    )
}

fn basis_kind(reference: PriceKind, compared: PriceKind) -> Option<BasisKind> {
    match (reference, compared) {
        (
            PriceKind::SpotBuyFromAsks | PriceKind::PerpetualBuyFromAsks,
            PriceKind::PerpetualSellIntoBids,
        ) => Some(BasisKind::ExecutableEntry),
        (
            PriceKind::SpotSellIntoBids | PriceKind::PerpetualSellIntoBids,
            PriceKind::PerpetualBuyFromAsks,
        ) => Some(BasisKind::ExecutableExit),
        (reference, compared)
            if is_indicative_price(reference) && is_indicative_price(compared) =>
        {
            Some(BasisKind::IndicativePair {
                reference,
                compared,
            })
        }
        _ => None,
    }
}

fn is_indicative_price(kind: PriceKind) -> bool {
    matches!(
        kind,
        PriceKind::SpotMid | PriceKind::PerpetualMid | PriceKind::Mark | PriceKind::Index
    )
}

fn venue_supports_instrument(venue: AdapterId, kind: InstrumentKind) -> bool {
    matches!(
        (venue, kind),
        (
            AdapterId::UpbitSpot | AdapterId::BithumbSpot | AdapterId::BinanceSpot,
            InstrumentKind::Spot
        ) | (
            AdapterId::BinanceUsdm | AdapterId::BybitLinear,
            InstrumentKind::Perpetual
        )
    )
}

fn exact_ratio(
    mut numerator: BigInt,
    mut denominator: BigInt,
    rounding: DecimalRounding,
) -> Result<ExactDecimal, FeatureInvalidReason> {
    if denominator.is_zero() {
        return Err(FeatureInvalidReason::NonPositiveValue);
    }
    if denominator.is_negative() {
        numerator = -numerator;
        denominator = -denominator;
    }
    let quotient = &numerator / &denominator;
    let remainder = &numerator % &denominator;
    let rounded = if remainder.is_zero() {
        quotient
    } else {
        let direction = if numerator.is_negative() { -1 } else { 1 };
        match rounding {
            DecimalRounding::TowardZero => quotient,
            DecimalRounding::Floor if numerator.is_negative() => quotient - 1,
            DecimalRounding::Floor => quotient,
            DecimalRounding::Ceiling if numerator.is_positive() => quotient + 1,
            DecimalRounding::Ceiling => quotient,
            DecimalRounding::HalfAwayFromZero
                if remainder.abs() * BigInt::from(2) >= denominator.abs() =>
            {
                quotient + direction
            }
            DecimalRounding::HalfAwayFromZero => quotient,
        }
    };
    rounded
        .to_i128()
        .ok_or(FeatureInvalidReason::ArithmeticOverflow)
        .and_then(|scaled| {
            ExactDecimal::from_scaled(scaled).map_err(|_| FeatureInvalidReason::ArithmeticOverflow)
        })
}
use std::collections::HashSet;
