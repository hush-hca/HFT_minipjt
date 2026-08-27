use std::collections::HashMap;

use funding_core::{
    config::{CostConfig, DecimalRounding, ExactDecimal},
    feature::{
        EffectiveTimestampSource, FeatureSource, InstrumentKind, NamedPrice, NbboSide, PriceKind,
    },
    metadata::{FundingMetadataFeature, MetadataInvalidReason},
    opportunity::{
        CandidateEvaluation, CapacityEvidence, CapacityEvidenceValidity, CapacityLeg, CostModel,
        FeeAssumption, FeeLiquidity, FeeSource, OpportunityRejectionReason, VenueCostModel,
    },
    public::{DerivativeEvent, MarkIndexSnapshot},
};
use funding_features::{
    basis::{NbboInput, basis_bps, compute_nbbo},
    book::compute_book_features,
    metadata::MetadataAligner,
    opportunity::{CandidateInput, MarkPriceInput, evaluate_candidate},
};
use md_core::model::{AdapterId, BookSnapshot, CanonicalSymbol};

use super::model::OpportunityRow;

const LIVE_FRESHNESS_US: i64 = 2_000_000;
const MICROS_PER_EXACT_UNIT: i128 = ExactDecimal::SCALE / 1_000_000;
const SCALED_PER_PPM: i128 = ExactDecimal::SCALE / 1_000_000;
const SCALED_PER_BPS: i128 = ExactDecimal::SCALE / 10_000;

pub struct LiveOpportunityEngine {
    config: CostConfig,
    cost_model: CostModel,
    metadata: MetadataAligner,
    marks: HashMap<(AdapterId, CanonicalSymbol), MarkIndexSnapshot>,
}

impl LiveOpportunityEngine {
    pub fn new(config: CostConfig) -> Self {
        let cost_model = cost_model(&config);
        Self {
            config,
            cost_model,
            metadata: MetadataAligner::new(),
            marks: HashMap::new(),
        }
    }

    pub fn observe(&mut self, event: &DerivativeEvent) {
        match event {
            DerivativeEvent::FundingEstimate(value) => {
                let _ = self.metadata.observe_funding(value.clone());
            }
            DerivativeEvent::MarkIndex(value) => {
                self.marks
                    .insert((value.meta.venue, value.meta.symbol.clone()), value.clone());
            }
            _ => {}
        }
    }

    pub fn evaluate(
        &self,
        symbol: &CanonicalSymbol,
        binance_book: Option<&BookSnapshot>,
        bybit_book: Option<&BookSnapshot>,
        decision_ts_us: i64,
    ) -> OpportunityRow {
        let binance_funding = self.metadata.funding_feature(
            AdapterId::BinanceUsdm,
            symbol,
            decision_ts_us,
            LIVE_FRESHNESS_US,
        );
        let bybit_funding = self.metadata.funding_feature(
            AdapterId::BybitLinear,
            symbol,
            decision_ts_us,
            LIVE_FRESHNESS_US,
        );
        let (binance_funding, bybit_funding) = match (binance_funding, bybit_funding) {
            (Ok(binance), Ok(bybit)) => (binance, bybit),
            (Err(error), _) | (_, Err(error)) => {
                return unavailable_row(symbol, metadata_code(&error));
            }
        };
        let (short_funding, long_funding) =
            if binance_funding.hourly_linear_rate >= bybit_funding.hourly_linear_rate {
                (&binance_funding, &bybit_funding)
            } else {
                (&bybit_funding, &binance_funding)
            };
        let mut row = funding_row(symbol, short_funding, long_funding);
        if short_funding.hourly_linear_rate <= long_funding.hourly_linear_rate {
            row.exclusion = Some("NO_POSITIVE_FUNDING_GAP".into());
            return row;
        }

        let Some(binance_book) = binance_book else {
            row.exclusion = Some("MISSING_BINANCE_BOOK".into());
            return row;
        };
        let Some(bybit_book) = bybit_book else {
            row.exclusion = Some("MISSING_BYBIT_BOOK".into());
            return row;
        };
        let (short_book, long_book) = if short_funding.source.adapter == AdapterId::BinanceUsdm {
            (binance_book, bybit_book)
        } else {
            (bybit_book, binance_book)
        };
        let Some(short_bid) = short_book.bids.first().map(|level| level.price) else {
            row.exclusion = Some("MISSING_SHORT_BID".into());
            return row;
        };
        let Some(long_ask) = long_book.asks.first().map(|level| level.price) else {
            row.exclusion = Some("MISSING_LONG_ASK".into());
            return row;
        };
        let conservative_price = match ExactDecimal::from_scaled(short_bid.max(long_ask)) {
            Ok(value) if value.scaled() > 0 => value,
            _ => {
                row.exclusion = Some("INVALID_ENTRY_PRICE".into());
                return row;
            }
        };
        let requested_base = match self
            .config
            .research_quote_per_leg
            .checked_div(conservative_price, DecimalRounding::Floor)
        {
            Ok(value) if value.scaled() > 0 => value,
            _ => {
                row.exclusion = Some("RESEARCH_SIZE_NOT_REPRESENTABLE".into());
                return row;
            }
        };

        let long_features = compute_book_features(
            None,
            long_book,
            requested_base,
            decision_ts_us,
            LIVE_FRESHNESS_US,
        );
        let short_features = compute_book_features(
            None,
            short_book,
            requested_base,
            decision_ts_us,
            LIVE_FRESHNESS_US,
        );
        let long_nbbo = match compute_nbbo(
            &[NbboInput {
                venue: long_book.meta.adapter,
                instrument_kind: InstrumentKind::Perpetual,
                symbol,
                book: &long_features,
            }],
            symbol,
            InstrumentKind::Perpetual,
            requested_base,
            decision_ts_us,
            LIVE_FRESHNESS_US,
        ) {
            Ok(value) => value,
            Err(_) => {
                row.exclusion = Some("INVALID_LONG_BOOK".into());
                return row;
            }
        };
        let short_nbbo = match compute_nbbo(
            &[NbboInput {
                venue: short_book.meta.adapter,
                instrument_kind: InstrumentKind::Perpetual,
                symbol,
                book: &short_features,
            }],
            symbol,
            InstrumentKind::Perpetual,
            requested_base,
            decision_ts_us,
            LIVE_FRESHNESS_US,
        ) {
            Ok(value) => value,
            Err(_) => {
                row.exclusion = Some("INVALID_SHORT_BOOK".into());
                return row;
            }
        };
        let Some(long_quote) = long_nbbo.ask else {
            row.exclusion = Some("LONG_DEPTH_UNAVAILABLE".into());
            return row;
        };
        let Some(short_quote) = short_nbbo.bid else {
            row.exclusion = Some("SHORT_DEPTH_UNAVAILABLE".into());
            return row;
        };
        if long_quote.side != NbboSide::Ask || short_quote.side != NbboSide::Bid {
            row.exclusion = Some("ENTRY_SIDE_MISMATCH".into());
            return row;
        }

        let entry_basis = match basis_bps(
            named_entry(&long_quote, PriceKind::PerpetualBuyFromAsks),
            named_entry(&short_quote, PriceKind::PerpetualSellIntoBids),
            decision_ts_us,
            LIVE_FRESHNESS_US,
        ) {
            Ok(value) => value,
            Err(_) => {
                row.exclusion = Some("INVALID_ENTRY_BASIS".into());
                return row;
            }
        };
        let long_mark = match self.mark(long_quote.venue, symbol) {
            Some(value) => value,
            None => {
                row.exclusion = Some("MISSING_LONG_MARK".into());
                return row;
            }
        };
        let short_mark = match self.mark(short_quote.venue, symbol) {
            Some(value) => value,
            None => {
                row.exclusion = Some("MISSING_SHORT_MARK".into());
                return row;
            }
        };
        let oldest_age_us = [
            short_funding.age_us,
            long_funding.age_us,
            decision_ts_us.saturating_sub(short_book.meta.local_recv_ts_us),
            decision_ts_us.saturating_sub(long_book.meta.local_recv_ts_us),
            decision_ts_us.saturating_sub(short_mark.source.local_recv_ts_us),
            decision_ts_us.saturating_sub(long_mark.source.local_recv_ts_us),
        ]
        .into_iter()
        .max()
        .unwrap_or(0)
        .max(0);
        row.freshness_ms = u64::try_from(oldest_age_us / 1_000).unwrap_or(u64::MAX);
        let cap = CapacityEvidence {
            source: self.config.capacity_source,
            venue: None,
            leg: CapacityLeg::Pair,
            symbol: Some(symbol.clone()),
            capacity_base: None,
            capacity_quote: Some(self.config.research_quote_per_leg),
            source_event_id: None,
            source_ts_us: Some(decision_ts_us),
            validity: CapacityEvidenceValidity::Available,
        };
        let holding_end_ts_us = short_funding
            .next_settlement_ts_us
            .max(long_funding.next_settlement_ts_us);
        match evaluate_candidate(CandidateInput {
            entry_basis: &entry_basis,
            long_quote: &long_quote,
            short_quote: &short_quote,
            long_funding,
            short_funding,
            long_mark: MarkPriceInput {
                price: &long_mark,
                freshness_limit_us: LIVE_FRESHNESS_US,
            },
            short_mark: MarkPriceInput {
                price: &short_mark,
                freshness_limit_us: LIVE_FRESHNESS_US,
            },
            cost_model: &self.cost_model,
            minimum_net_bps: ExactDecimal::from_scaled(0).expect("zero is representable"),
            holding_end_ts_us,
            caps: &[cap],
        }) {
            CandidateEvaluation::Eligible(value) => {
                row.conservative_net_usd_micros =
                    Some(value.expected_net_pnl / MICROS_PER_EXACT_UNIT);
                row.capacity_usd_micros =
                    Some(value.capacity.capacity_quote.scaled() / MICROS_PER_EXACT_UNIT);
                row.exclusion = None;
            }
            CandidateEvaluation::Rejected(value) => {
                row.exclusion = Some(rejection_code(&value.reason).into());
            }
        }
        row
    }

    fn mark(&self, venue: AdapterId, symbol: &CanonicalSymbol) -> Option<NamedPrice> {
        let value = self.marks.get(&(venue, symbol.clone()))?;
        let price = ExactDecimal::from_scaled(value.mark_price).ok()?;
        Some(NamedPrice {
            venue,
            instrument_kind: InstrumentKind::Perpetual,
            kind: PriceKind::Mark,
            value: price,
            source: derivative_source(&value.meta),
        })
    }
}

fn funding_row(
    symbol: &CanonicalSymbol,
    short: &FundingMetadataFeature,
    long: &FundingMetadataFeature,
) -> OpportunityRow {
    let raw_gap = short
        .raw_rate
        .scaled()
        .saturating_sub(long.raw_rate.scaled());
    let hourly_gap = short
        .hourly_linear_rate
        .scaled()
        .saturating_sub(long.hourly_linear_rate.scaled());
    let apr_scaled = hourly_gap.saturating_mul(24 * 365);
    OpportunityRow {
        symbol: format!("{}/{}", symbol.base, symbol.quote),
        short_venue: venue_name(short.source.adapter).into(),
        short_rate_ppm: short.raw_rate.scaled() / SCALED_PER_PPM,
        short_interval_secs: short.interval_secs,
        long_venue: venue_name(long.source.adapter).into(),
        long_rate_ppm: long.raw_rate.scaled() / SCALED_PER_PPM,
        long_interval_secs: long.interval_secs,
        raw_gap_ppm: raw_gap / SCALED_PER_PPM,
        indicative_apr_bps: apr_scaled / SCALED_PER_BPS,
        conservative_net_usd_micros: None,
        capacity_usd_micros: None,
        freshness_ms: u64::try_from(short.age_us.max(long.age_us).max(0) / 1_000)
            .unwrap_or(u64::MAX),
        exclusion: Some("EVALUATION_PENDING".into()),
    }
}

fn unavailable_row(symbol: &CanonicalSymbol, code: &str) -> OpportunityRow {
    OpportunityRow {
        symbol: format!("{}/{}", symbol.base, symbol.quote),
        short_venue: "UNAVAILABLE".into(),
        short_rate_ppm: 0,
        short_interval_secs: 0,
        long_venue: "UNAVAILABLE".into(),
        long_rate_ppm: 0,
        long_interval_secs: 0,
        raw_gap_ppm: 0,
        indicative_apr_bps: 0,
        conservative_net_usd_micros: None,
        capacity_usd_micros: None,
        freshness_ms: u64::MAX,
        exclusion: Some(code.into()),
    }
}

fn named_entry(quote: &funding_core::feature::NbboQuote, kind: PriceKind) -> NamedPrice {
    NamedPrice {
        venue: quote.venue,
        instrument_kind: InstrumentKind::Perpetual,
        kind,
        value: quote.price,
        source: quote.source.clone(),
    }
}

fn derivative_source(meta: &funding_core::meta::DerivativeMeta) -> FeatureSource {
    let effective_ts_us = meta.source_ts_us.unwrap_or(meta.local_recv_ts_us);
    FeatureSource {
        event_id: meta.event_id,
        adapter: meta.venue,
        symbol: meta.symbol.clone(),
        source_sequence: None,
        exchange_event_ts_us: meta.source_ts_us,
        exchange_trade_ts_us: None,
        local_recv_ts_us: meta.local_recv_ts_us,
        effective_ts_us,
        effective_ts_source: if meta.source_ts_us.is_some() {
            EffectiveTimestampSource::ExchangeEvent
        } else {
            EffectiveTimestampSource::LocalReceive
        },
    }
}

fn cost_model(config: &CostConfig) -> CostModel {
    let venue = |rate| VenueCostModel {
        entry_fee: FeeAssumption {
            rate,
            source: FeeSource::ExplicitConfig,
            liquidity: FeeLiquidity::Taker,
        },
        exit_fee: FeeAssumption {
            rate,
            source: FeeSource::ExplicitConfig,
            liquidity: FeeLiquidity::Taker,
        },
        entry_slippage_bps: config.entry_slippage_bps,
        exit_slippage_bps: config.exit_slippage_bps,
        entry_book_impact_bps: config.entry_book_impact_bps,
        exit_book_impact_bps: config.exit_book_impact_bps,
    };
    CostModel {
        binance: venue(config.binance_taker_rate),
        bybit: venue(config.bybit_taker_rate),
        basis_risk_buffer_bps: config.basis_risk_buffer_bps,
        funding_error_buffer_bps: config.funding_error_buffer_bps,
        leg_risk_buffer_bps: config.leg_risk_buffer_bps,
    }
}

fn metadata_code(error: &MetadataInvalidReason) -> &'static str {
    match error {
        MetadataInvalidReason::MissingFunding { .. } => "MISSING_FUNDING",
        MetadataInvalidReason::Stale { .. } => "STALE_FUNDING",
        MetadataInvalidReason::NextSettlementNotFuture { .. } => "SETTLEMENT_NOT_FUTURE",
        _ => "INVALID_FUNDING_METADATA",
    }
}

fn rejection_code(reason: &OpportunityRejectionReason) -> &'static str {
    match reason {
        OpportunityRejectionReason::InvalidHoldingWindow => "INVALID_HOLDING_WINDOW",
        OpportunityRejectionReason::InvalidEntryBasis => "INVALID_ENTRY_BASIS",
        OpportunityRejectionReason::IdentityMismatch => "IDENTITY_MISMATCH",
        OpportunityRejectionReason::NonPositiveRequestedBase => "NON_POSITIVE_SIZE",
        OpportunityRejectionReason::RequestedQuantityMismatch => "SIZE_MISMATCH",
        OpportunityRejectionReason::MissingNotionalEvidence => "MISSING_NOTIONAL",
        OpportunityRejectionReason::FundingEvidenceMismatch => "FUNDING_EVIDENCE_MISMATCH",
        OpportunityRejectionReason::InvalidCostModel => "INVALID_COST_MODEL",
        OpportunityRejectionReason::FutureEvidence { .. } => "FUTURE_EVIDENCE",
        OpportunityRejectionReason::StaleEvidence { .. } => "STALE_EVIDENCE",
        OpportunityRejectionReason::InsufficientCapacity { .. } => "INSUFFICIENT_CAPACITY",
        OpportunityRejectionReason::NoAnnouncedSettlementInWindow => "NO_SETTLEMENT_IN_WINDOW",
        OpportunityRejectionReason::ArithmeticOverflow => "ARITHMETIC_OVERFLOW",
        OpportunityRejectionReason::NetEdgeNotPositive => "NET_EDGE_NOT_POSITIVE",
        OpportunityRejectionReason::NetEdgeBelowMinimum { .. } => "NET_EDGE_BELOW_MINIMUM",
    }
}

fn venue_name(venue: AdapterId) -> &'static str {
    match venue {
        AdapterId::BinanceUsdm => "Binance USD-M",
        AdapterId::BybitLinear => "Bybit Linear",
        _ => "Unsupported venue",
    }
}
