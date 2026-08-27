use std::collections::BTreeMap;

use funding_core::{
    config::{DecimalRounding, ExactDecimal},
    feature::{
        BookComparisonValidity, BookFeatures, BookIdentity, BookInvalidReason, BookLevelSide,
        DepthDeltaLevel, EffectiveTimestampSource, ExecutableQuote, ExecutableQuoteSide,
        FeatureInvalidReason, FeatureSource, FeatureValidity, QuoteInvalidReason, QuoteValidity,
        StructuralBookValidity,
    },
};
use md_core::{
    model::{BookSnapshot, NormalizedEvent, PriceLevel},
    validation::{BookSide, TimestampField, ValidationError, validate_book, validate_event},
};
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};

const DEPTH_LIMIT: usize = 20;

pub fn compute_book_features(
    previous: Option<&BookSnapshot>,
    current: &BookSnapshot,
    requested_base: ExactDecimal,
    decision_ts_us: i64,
    stale_after_us: i64,
) -> BookFeatures {
    let source = feature_source(current);
    let previous_book = previous.map(book_identity);

    if let Some(reason) = structural_invalid_reason(current) {
        let structural_validity = StructuralBookValidity::Invalid(reason.clone());
        let quote_reason = quote_reason_for_book_invalid(&reason);
        return BookFeatures::invalid(
            source,
            previous_book,
            structural_validity,
            invalid_quote(
                ExecutableQuoteSide::SellIntoBids,
                requested_base,
                quote_reason.clone(),
            ),
            invalid_quote(
                ExecutableQuoteSide::BuyFromAsks,
                requested_base,
                quote_reason,
            ),
            feature_reason_for_book_invalid(&reason),
        );
    }

    if stale_after_us < 0 {
        let reason = FeatureInvalidReason::InvalidFreshnessLimit {
            limit_us: stale_after_us,
        };
        return BookFeatures::invalid(
            source,
            previous_book,
            StructuralBookValidity::Valid,
            invalid_quote(
                ExecutableQuoteSide::SellIntoBids,
                requested_base,
                QuoteInvalidReason::InvalidFreshnessLimit {
                    limit_us: stale_after_us,
                },
            ),
            invalid_quote(
                ExecutableQuoteSide::BuyFromAsks,
                requested_base,
                QuoteInvalidReason::InvalidFreshnessLimit {
                    limit_us: stale_after_us,
                },
            ),
            reason,
        );
    }

    let causal_ts_us = current.meta.local_recv_ts_us;
    if causal_ts_us > decision_ts_us {
        let reason = BookInvalidReason::FutureTimestamp {
            source_ts_us: causal_ts_us,
            decision_ts_us,
        };
        return BookFeatures::invalid(
            source,
            previous_book,
            StructuralBookValidity::Invalid(reason),
            invalid_quote(
                ExecutableQuoteSide::SellIntoBids,
                requested_base,
                QuoteInvalidReason::FutureTimestamp {
                    source_ts_us: causal_ts_us,
                    decision_ts_us,
                },
            ),
            invalid_quote(
                ExecutableQuoteSide::BuyFromAsks,
                requested_base,
                QuoteInvalidReason::FutureTimestamp {
                    source_ts_us: causal_ts_us,
                    decision_ts_us,
                },
            ),
            FeatureInvalidReason::FutureTimestamp {
                source_ts_us: causal_ts_us,
                decision_ts_us,
            },
        );
    }

    let age_us = decision_ts_us - causal_ts_us;
    if age_us > stale_after_us {
        let reason = FeatureInvalidReason::Stale {
            age_us,
            limit_us: stale_after_us,
        };
        let quote_reason = QuoteInvalidReason::Stale {
            age_us,
            limit_us: stale_after_us,
        };
        return BookFeatures::invalid(
            source,
            previous_book,
            StructuralBookValidity::Valid,
            invalid_quote(
                ExecutableQuoteSide::SellIntoBids,
                requested_base,
                quote_reason.clone(),
            ),
            invalid_quote(
                ExecutableQuoteSide::BuyFromAsks,
                requested_base,
                quote_reason,
            ),
            reason,
        );
    }

    if let Err(error) = validate_event(&NormalizedEvent::Book(current.clone())) {
        let reason = map_event_error(error, current);
        return BookFeatures::invalid(
            source,
            previous_book,
            StructuralBookValidity::Valid,
            invalid_quote(
                ExecutableQuoteSide::SellIntoBids,
                requested_base,
                quote_reason_for_feature_invalid(&reason),
            ),
            invalid_quote(
                ExecutableQuoteSide::BuyFromAsks,
                requested_base,
                quote_reason_for_feature_invalid(&reason),
            ),
            reason,
        );
    }

    if !book_values_fit_decimal(current) {
        return arithmetic_invalid(source, previous_book, requested_base);
    }

    let current_values = match compute_current_values(current) {
        Ok(values) => values,
        Err(()) => return arithmetic_invalid(source, previous_book, requested_base),
    };
    let sell_into_bids = executable_quote(
        &current.bids,
        ExecutableQuoteSide::SellIntoBids,
        requested_base,
    );
    let buy_from_asks = executable_quote(
        &current.asks,
        ExecutableQuoteSide::BuyFromAsks,
        requested_base,
    );

    let mut comparison_validity = BookComparisonValidity::NotRequested;
    let mut snapshot_ofi = None;
    let mut depth_delta_bid = None;
    let mut depth_delta_ask = None;
    let mut depth_delta_bids = Vec::new();
    let mut depth_delta_asks = Vec::new();

    if let Some(previous) = previous {
        if previous.meta.adapter != current.meta.adapter
            || previous.meta.symbol != current.meta.symbol
        {
            comparison_validity = BookComparisonValidity::Invalid(
                FeatureInvalidReason::PreviousBookIdentityMismatch {
                    previous_adapter: previous.meta.adapter,
                    previous_symbol: previous.meta.symbol.clone(),
                    current_adapter: current.meta.adapter,
                    current_symbol: current.meta.symbol.clone(),
                },
            );
        } else if let (Some(previous_sequence), Some(current_sequence)) =
            (previous.meta.source_sequence, current.meta.source_sequence)
            && current_sequence < previous_sequence
        {
            comparison_validity =
                BookComparisonValidity::Invalid(FeatureInvalidReason::RegressingSourceSequence {
                    previous_sequence,
                    current_sequence,
                });
        } else if let (Some(previous_sequence), Some(current_sequence)) =
            (previous.meta.source_sequence, current.meta.source_sequence)
            && current_sequence == previous_sequence
            && !same_book_payload(previous, current)
        {
            comparison_validity =
                BookComparisonValidity::Invalid(FeatureInvalidReason::SourceSequenceConflict {
                    sequence: current_sequence,
                });
        } else if previous.meta.local_recv_ts_us > current.meta.local_recv_ts_us {
            comparison_validity = BookComparisonValidity::Invalid(
                FeatureInvalidReason::PreviousBookReceiveOrderInvalid {
                    previous_local_recv_ts_us: previous.meta.local_recv_ts_us,
                    current_local_recv_ts_us: current.meta.local_recv_ts_us,
                },
            );
        } else if previous.meta.local_recv_ts_us > decision_ts_us {
            comparison_validity =
                BookComparisonValidity::Invalid(FeatureInvalidReason::FutureTimestamp {
                    source_ts_us: previous.meta.local_recv_ts_us,
                    decision_ts_us,
                });
        } else if decision_ts_us - previous.meta.local_recv_ts_us > stale_after_us {
            comparison_validity = BookComparisonValidity::Invalid(FeatureInvalidReason::Stale {
                age_us: decision_ts_us - previous.meta.local_recv_ts_us,
                limit_us: stale_after_us,
            });
        } else if let Some(reason) = structural_invalid_reason(previous) {
            comparison_validity =
                BookComparisonValidity::Invalid(FeatureInvalidReason::PreviousBookInvalid {
                    reason,
                });
        } else if let Err(error) = validate_event(&NormalizedEvent::Book(previous.clone())) {
            comparison_validity = BookComparisonValidity::Invalid(map_event_error(error, previous));
        } else {
            let source_ts_us = book_timestamp(current);
            let previous_ts_us = book_timestamp(previous);
            if source_ts_us < previous_ts_us {
                comparison_validity =
                    BookComparisonValidity::Invalid(FeatureInvalidReason::RegressingTimestamp {
                        previous_ts_us,
                        current_ts_us: source_ts_us,
                    });
            } else if !book_values_fit_decimal(previous) {
                comparison_validity =
                    BookComparisonValidity::Invalid(FeatureInvalidReason::ArithmeticOverflow);
            } else {
                match compute_comparison(previous, current) {
                    Ok(value) => {
                        comparison_validity = BookComparisonValidity::Valid;
                        snapshot_ofi = Some(value.snapshot_ofi);
                        depth_delta_bid = Some(value.depth_delta_bid);
                        depth_delta_ask = Some(value.depth_delta_ask);
                        depth_delta_bids = value.depth_delta_bids;
                        depth_delta_asks = value.depth_delta_asks;
                    }
                    Err(()) => {
                        comparison_validity = BookComparisonValidity::Invalid(
                            FeatureInvalidReason::ArithmeticOverflow,
                        );
                    }
                }
            }
        }
    }

    BookFeatures {
        source,
        previous_book,
        structural_validity: StructuralBookValidity::Valid,
        comparison_validity,
        sell_into_bids,
        buy_from_asks,
        mid: Some(current_values.mid),
        microprice: Some(current_values.microprice),
        imbalance_1: Some(current_values.imbalances[0]),
        imbalance_5: Some(current_values.imbalances[1]),
        imbalance_10: Some(current_values.imbalances[2]),
        imbalance_20: Some(current_values.imbalances[3]),
        snapshot_ofi,
        depth_delta_bid,
        depth_delta_ask,
        depth_delta_bids,
        depth_delta_asks,
        validity: FeatureValidity::Valid,
    }
}

struct CurrentValues {
    mid: ExactDecimal,
    microprice: ExactDecimal,
    imbalances: [ExactDecimal; 4],
}

fn compute_current_values(book: &BookSnapshot) -> Result<CurrentValues, ()> {
    let best_bid = BigInt::from(book.bids[0].price);
    let best_ask = BigInt::from(book.asks[0].price);
    let bid_quantity = BigInt::from(book.bids[0].quantity);
    let ask_quantity = BigInt::from(book.asks[0].quantity);

    let mid = exact_ratio(
        best_bid.clone() + &best_ask,
        BigInt::from(2),
        DecimalRounding::HalfAwayFromZero,
    )?;
    let microprice = exact_ratio(
        &best_ask * &bid_quantity + &best_bid * &ask_quantity,
        &bid_quantity + &ask_quantity,
        DecimalRounding::HalfAwayFromZero,
    )?;
    let mut imbalances = [zero(); 4];
    for (index, depth) in [1, 5, 10, 20].into_iter().enumerate() {
        imbalances[index] = depth_imbalance(book, depth)?;
    }
    Ok(CurrentValues {
        mid,
        microprice,
        imbalances,
    })
}

fn depth_imbalance(book: &BookSnapshot, depth: usize) -> Result<ExactDecimal, ()> {
    let bid_quantity = sum_quantities(book.bids.iter().take(depth));
    let ask_quantity = sum_quantities(book.asks.iter().take(depth));
    exact_ratio(
        (&bid_quantity - &ask_quantity) * BigInt::from(ExactDecimal::SCALE),
        bid_quantity + ask_quantity,
        DecimalRounding::HalfAwayFromZero,
    )
}

fn executable_quote(
    levels: &[PriceLevel],
    side: ExecutableQuoteSide,
    requested_base: ExactDecimal,
) -> ExecutableQuote {
    if requested_base.scaled() <= 0 {
        return invalid_quote(side, requested_base, QuoteInvalidReason::InvalidQuantity);
    }

    let available_raw = sum_quantities(levels.iter());
    let Some(available_base) = big_to_exact(&available_raw) else {
        return invalid_quote(side, requested_base, QuoteInvalidReason::ArithmeticOverflow);
    };
    let requested_raw = BigInt::from(requested_base.scaled());
    if available_raw < requested_raw {
        return ExecutableQuote {
            side,
            requested_base,
            available_base,
            average_price: None,
            quote_notional: None,
            levels_consumed: u16::try_from(levels.len()).unwrap_or(u16::MAX),
            validity: QuoteValidity::Invalid(QuoteInvalidReason::InsufficientDepth {
                requested_base,
                available_base,
            }),
        };
    }

    let mut remaining = requested_raw.clone();
    let mut raw_notional = BigInt::zero();
    let mut levels_consumed = 0_u16;
    for level in levels {
        if remaining.is_zero() {
            break;
        }
        let level_quantity = BigInt::from(level.quantity);
        let fill = if remaining < level_quantity {
            remaining.clone()
        } else {
            level_quantity
        };
        raw_notional += BigInt::from(level.price) * &fill;
        remaining -= fill;
        let Some(next) = levels_consumed.checked_add(1) else {
            return invalid_quote(side, requested_base, QuoteInvalidReason::ArithmeticOverflow);
        };
        levels_consumed = next;
    }

    let rounding = match side {
        ExecutableQuoteSide::SellIntoBids => DecimalRounding::Floor,
        ExecutableQuoteSide::BuyFromAsks => DecimalRounding::Ceiling,
    };
    let Ok(average_price) = exact_ratio(raw_notional.clone(), requested_raw, rounding) else {
        return invalid_quote(side, requested_base, QuoteInvalidReason::ArithmeticOverflow);
    };
    let Ok(quote_notional) = exact_ratio(raw_notional, BigInt::from(ExactDecimal::SCALE), rounding)
    else {
        return invalid_quote(side, requested_base, QuoteInvalidReason::ArithmeticOverflow);
    };

    ExecutableQuote {
        side,
        requested_base,
        available_base,
        average_price: Some(average_price),
        quote_notional: Some(quote_notional),
        levels_consumed,
        validity: QuoteValidity::Valid,
    }
}

struct ComparisonValues {
    snapshot_ofi: ExactDecimal,
    depth_delta_bid: ExactDecimal,
    depth_delta_ask: ExactDecimal,
    depth_delta_bids: Vec<DepthDeltaLevel>,
    depth_delta_asks: Vec<DepthDeltaLevel>,
}

fn compute_comparison(
    previous: &BookSnapshot,
    current: &BookSnapshot,
) -> Result<ComparisonValues, ()> {
    let snapshot_ofi = best_level_ofi(previous, current)?;
    let depth_delta_bids = depth_delta_levels(&previous.bids, &current.bids, BookLevelSide::Bid)?;
    let depth_delta_asks = depth_delta_levels(&previous.asks, &current.asks, BookLevelSide::Ask)?;
    let depth_delta_bid = sum_depth_deltas(&depth_delta_bids)?;
    let depth_delta_ask = sum_depth_deltas(&depth_delta_asks)?;
    Ok(ComparisonValues {
        snapshot_ofi,
        depth_delta_bid,
        depth_delta_ask,
        depth_delta_bids,
        depth_delta_asks,
    })
}

fn best_level_ofi(previous: &BookSnapshot, current: &BookSnapshot) -> Result<ExactDecimal, ()> {
    let previous_bid = &previous.bids[0];
    let current_bid = &current.bids[0];
    let previous_ask = &previous.asks[0];
    let current_ask = &current.asks[0];

    let bid_component = match current_bid.price.cmp(&previous_bid.price) {
        std::cmp::Ordering::Greater => BigInt::from(current_bid.quantity),
        std::cmp::Ordering::Equal => {
            BigInt::from(current_bid.quantity) - BigInt::from(previous_bid.quantity)
        }
        std::cmp::Ordering::Less => -BigInt::from(previous_bid.quantity),
    };
    let ask_component = match current_ask.price.cmp(&previous_ask.price) {
        std::cmp::Ordering::Less => -BigInt::from(current_ask.quantity),
        std::cmp::Ordering::Equal => {
            BigInt::from(previous_ask.quantity) - BigInt::from(current_ask.quantity)
        }
        std::cmp::Ordering::Greater => BigInt::from(previous_ask.quantity),
    };
    big_to_exact(&(bid_component + ask_component)).ok_or(())
}

fn depth_delta_levels(
    previous: &[PriceLevel],
    current: &[PriceLevel],
    side: BookLevelSide,
) -> Result<Vec<DepthDeltaLevel>, ()> {
    let mut by_price: BTreeMap<i128, (i128, i128)> = BTreeMap::new();
    for level in previous.iter().take(DEPTH_LIMIT) {
        by_price.entry(level.price).or_default().0 = level.quantity;
    }
    for level in current.iter().take(DEPTH_LIMIT) {
        by_price.entry(level.price).or_default().1 = level.quantity;
    }

    let make_level = |(price, (previous_base, current_base)): (i128, (i128, i128))| {
        Some(DepthDeltaLevel {
            price: ExactDecimal::from_scaled(price).ok()?,
            previous_base: ExactDecimal::from_scaled(previous_base).ok()?,
            current_base: ExactDecimal::from_scaled(current_base).ok()?,
            delta_base: big_to_exact(&(BigInt::from(current_base) - BigInt::from(previous_base)))?,
        })
    };

    match side {
        BookLevelSide::Bid => by_price
            .into_iter()
            .rev()
            .map(make_level)
            .collect::<Option<Vec<_>>>()
            .ok_or(()),
        BookLevelSide::Ask => by_price
            .into_iter()
            .map(make_level)
            .collect::<Option<Vec<_>>>()
            .ok_or(()),
    }
}

fn sum_depth_deltas(levels: &[DepthDeltaLevel]) -> Result<ExactDecimal, ()> {
    let sum: BigInt = levels
        .iter()
        .map(|level| BigInt::from(level.delta_base.scaled()))
        .sum();
    big_to_exact(&sum).ok_or(())
}

fn structural_invalid_reason(book: &BookSnapshot) -> Option<BookInvalidReason> {
    match validate_book(book) {
        Ok(()) => None,
        Err(ValidationError::EmptyBookSide { .. }) => Some(BookInvalidReason::EmptyBook),
        Err(ValidationError::NonPositiveBookPrice { .. }) => {
            Some(BookInvalidReason::NonPositivePrice)
        }
        Err(ValidationError::NonPositiveBookQuantity { .. }) => {
            Some(BookInvalidReason::NonPositiveQuantity)
        }
        Err(ValidationError::UnsortedBook {
            side,
            previous_level,
            level,
        }) => {
            let levels = match side {
                BookSide::Bid => &book.bids,
                BookSide::Ask => &book.asks,
            };
            let previous_price = ExactDecimal::from_scaled(levels[previous_level].price).ok()?;
            let current_price = ExactDecimal::from_scaled(levels[level].price).ok()?;
            let side = match side {
                BookSide::Bid => BookLevelSide::Bid,
                BookSide::Ask => BookLevelSide::Ask,
            };
            if previous_price == current_price {
                Some(BookInvalidReason::DuplicatePriceLevel {
                    side,
                    price: current_price,
                })
            } else {
                Some(BookInvalidReason::UnsortedLevels {
                    side,
                    previous_price,
                    current_price,
                })
            }
        }
        Err(ValidationError::CrossedBook { best_bid, best_ask }) if best_bid == best_ask => {
            Some(BookInvalidReason::LockedBook)
        }
        Err(ValidationError::CrossedBook { .. }) => Some(BookInvalidReason::CrossedBook),
        Err(
            ValidationError::NonPositiveLocalTimestamp { .. }
            | ValidationError::SourceTimestampOutOfRange { .. }
            | ValidationError::NonPositiveTradePrice { .. }
            | ValidationError::NonPositiveTradeQuantity { .. },
        ) => None,
    }
}

fn map_event_error(error: ValidationError, book: &BookSnapshot) -> FeatureInvalidReason {
    match error {
        ValidationError::NonPositiveLocalTimestamp { .. } => FeatureInvalidReason::NonPositiveValue,
        ValidationError::SourceTimestampOutOfRange { field, value, .. } => {
            let source_ts_us = match field {
                TimestampField::ExchangeEvent => value,
                TimestampField::ExchangeTrade => book_timestamp(book),
            };
            FeatureInvalidReason::SourceTimestampOutOfRange {
                source_ts_us,
                local_recv_ts_us: book.meta.local_recv_ts_us,
            }
        }
        _ => FeatureInvalidReason::NonPositiveValue,
    }
}

fn book_values_fit_decimal(book: &BookSnapshot) -> bool {
    book.bids.iter().chain(&book.asks).all(|level| {
        ExactDecimal::from_scaled(level.price).is_ok()
            && ExactDecimal::from_scaled(level.quantity).is_ok()
    })
}

fn feature_source(book: &BookSnapshot) -> FeatureSource {
    let (effective_ts_us, effective_ts_source) = book.meta.exchange_event_ts_us.map_or(
        (
            book.meta.local_recv_ts_us,
            EffectiveTimestampSource::LocalReceive,
        ),
        |value| (value, EffectiveTimestampSource::ExchangeEvent),
    );
    FeatureSource {
        event_id: book.meta.event_id,
        adapter: book.meta.adapter,
        symbol: book.meta.symbol.clone(),
        source_sequence: book.meta.source_sequence,
        exchange_event_ts_us: book.meta.exchange_event_ts_us,
        exchange_trade_ts_us: None,
        local_recv_ts_us: book.meta.local_recv_ts_us,
        effective_ts_us,
        effective_ts_source,
    }
}

fn book_identity(book: &BookSnapshot) -> BookIdentity {
    BookIdentity {
        event_id: book.meta.event_id,
        adapter: book.meta.adapter,
        symbol: book.meta.symbol.clone(),
        source_sequence: book.meta.source_sequence,
        exchange_event_ts_us: book.meta.exchange_event_ts_us,
        local_recv_ts_us: book.meta.local_recv_ts_us,
    }
}

fn book_timestamp(book: &BookSnapshot) -> i64 {
    book.meta
        .exchange_event_ts_us
        .unwrap_or(book.meta.local_recv_ts_us)
}

fn same_book_payload(previous: &BookSnapshot, current: &BookSnapshot) -> bool {
    previous.meta.adapter == current.meta.adapter
        && previous.meta.symbol == current.meta.symbol
        && previous.meta.exchange_event_ts_us == current.meta.exchange_event_ts_us
        && previous.bids == current.bids
        && previous.asks == current.asks
}

fn sum_quantities<'a>(levels: impl Iterator<Item = &'a PriceLevel>) -> BigInt {
    levels.map(|level| BigInt::from(level.quantity)).sum()
}

fn exact_ratio(
    mut numerator: BigInt,
    mut denominator: BigInt,
    rounding: DecimalRounding,
) -> Result<ExactDecimal, ()> {
    if denominator.is_zero() {
        return Err(());
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
    big_to_exact(&rounded).ok_or(())
}

fn big_to_exact(value: &BigInt) -> Option<ExactDecimal> {
    value
        .to_i128()
        .and_then(|scaled| ExactDecimal::from_scaled(scaled).ok())
}

fn zero() -> ExactDecimal {
    ExactDecimal::from_scaled(0).expect("zero is Decimal128-representable")
}

fn invalid_quote(
    side: ExecutableQuoteSide,
    requested_base: ExactDecimal,
    reason: QuoteInvalidReason,
) -> ExecutableQuote {
    ExecutableQuote::invalid(side, requested_base, zero(), reason)
}

fn arithmetic_invalid(
    source: FeatureSource,
    previous_book: Option<BookIdentity>,
    requested_base: ExactDecimal,
) -> BookFeatures {
    BookFeatures::invalid(
        source,
        previous_book,
        StructuralBookValidity::Valid,
        invalid_quote(
            ExecutableQuoteSide::SellIntoBids,
            requested_base,
            QuoteInvalidReason::ArithmeticOverflow,
        ),
        invalid_quote(
            ExecutableQuoteSide::BuyFromAsks,
            requested_base,
            QuoteInvalidReason::ArithmeticOverflow,
        ),
        FeatureInvalidReason::ArithmeticOverflow,
    )
}

fn quote_reason_for_book_invalid(reason: &BookInvalidReason) -> QuoteInvalidReason {
    match reason {
        BookInvalidReason::FutureTimestamp {
            source_ts_us,
            decision_ts_us,
        } => QuoteInvalidReason::FutureTimestamp {
            source_ts_us: *source_ts_us,
            decision_ts_us: *decision_ts_us,
        },
        BookInvalidReason::RegressingTimestamp {
            previous_ts_us,
            current_ts_us,
        } => QuoteInvalidReason::RegressingTimestamp {
            previous_ts_us: *previous_ts_us,
            current_ts_us: *current_ts_us,
        },
        _ => QuoteInvalidReason::StructuralBookInvalid,
    }
}

fn feature_reason_for_book_invalid(reason: &BookInvalidReason) -> FeatureInvalidReason {
    match reason {
        BookInvalidReason::FutureTimestamp {
            source_ts_us,
            decision_ts_us,
        } => FeatureInvalidReason::FutureTimestamp {
            source_ts_us: *source_ts_us,
            decision_ts_us: *decision_ts_us,
        },
        BookInvalidReason::RegressingTimestamp {
            previous_ts_us,
            current_ts_us,
        } => FeatureInvalidReason::RegressingTimestamp {
            previous_ts_us: *previous_ts_us,
            current_ts_us: *current_ts_us,
        },
        _ => FeatureInvalidReason::StructuralBookInvalid {
            reason: reason.clone(),
        },
    }
}

fn quote_reason_for_feature_invalid(reason: &FeatureInvalidReason) -> QuoteInvalidReason {
    match reason {
        FeatureInvalidReason::FutureTimestamp {
            source_ts_us,
            decision_ts_us,
        } => QuoteInvalidReason::FutureTimestamp {
            source_ts_us: *source_ts_us,
            decision_ts_us: *decision_ts_us,
        },
        FeatureInvalidReason::RegressingTimestamp {
            previous_ts_us,
            current_ts_us,
        } => QuoteInvalidReason::RegressingTimestamp {
            previous_ts_us: *previous_ts_us,
            current_ts_us: *current_ts_us,
        },
        FeatureInvalidReason::Stale { age_us, limit_us } => QuoteInvalidReason::Stale {
            age_us: *age_us,
            limit_us: *limit_us,
        },
        FeatureInvalidReason::ArithmeticOverflow => QuoteInvalidReason::ArithmeticOverflow,
        FeatureInvalidReason::InvalidQuantity => QuoteInvalidReason::InvalidQuantity,
        FeatureInvalidReason::InvalidFreshnessLimit { limit_us } => {
            QuoteInvalidReason::InvalidFreshnessLimit {
                limit_us: *limit_us,
            }
        }
        _ => QuoteInvalidReason::StructuralBookInvalid,
    }
}
