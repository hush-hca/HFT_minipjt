//! Deterministic, causal, in-memory replay for the public feature evaluator.
//!
//! Equal availability timestamps are inclusive: the fixed family rank applies
//! all evidence before a decision. Exchange timestamps never drive replay time.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use funding_core::{
    config::ExactDecimal,
    feature::{
        EffectiveTimestampSource, FeatureSource, InstrumentKind, NamedPrice, NbboQuote, NbboSide,
        PriceKind, QuoteValidity,
    },
    meta::DerivativeMeta,
    metadata::MetadataInvalidReason,
    opportunity::{
        CandidateEvaluation, CapacityEvidence, CapacityEvidenceValidity, CapacityLeg,
        CapacitySource, CostModel, FeeLiquidity, FeeSource, OpportunityRejectionReason,
        VenueCostModel,
    },
    public::{DerivativeEvent, MarkIndexSnapshot, TraderMetricKind},
    replay::{
        AlignedOpenInterest, AlignedTraderRatio, DecisionEvent, ReplayConfig,
        ReplayDecisionOutcome, ReplayDecisionRecord, ReplayEventFamily, ReplayReconciliation,
        ReplayRejection, ReplayRejectionReason, ReplayReport,
    },
};
use md_core::{
    model::{AdapterId, BookSnapshot, CanonicalSymbol, NormalizedEvent},
    validation::validate_event,
};
use ring::digest::{SHA256, digest};
use uuid::Uuid;

use crate::{
    basis::basis_bps,
    book::compute_book_features,
    flow::{TradePushOutcome, TradeWindow},
    metadata::MetadataAligner,
    opportunity::{CandidateInput, MarkPriceInput, evaluate_candidate},
};

pub const CANONICAL_ENCODING_VERSION: u16 = 1;
pub const SAME_RECEIVE_POLICY: &str = "evidence_before_decision_at_equal_local_receive_timestamp";

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ReplayEvent {
    Market(NormalizedEvent),
    Derivative(DerivativeEvent),
    Decision(DecisionEvent),
}

impl ReplayEvent {
    pub fn event_id(&self) -> Uuid {
        match self {
            Self::Market(value) => value.meta().event_id,
            Self::Derivative(value) => value.meta().event_id,
            Self::Decision(value) => value.event_id,
        }
    }

    pub fn local_recv_ts_us(&self) -> i64 {
        match self {
            Self::Market(value) => value.meta().local_recv_ts_us,
            Self::Derivative(value) => value.meta().local_recv_ts_us,
            Self::Decision(value) => value.local_recv_ts_us,
        }
    }

    pub fn family(&self) -> ReplayEventFamily {
        match self {
            Self::Market(NormalizedEvent::Book(_)) => ReplayEventFamily::Book,
            Self::Market(NormalizedEvent::Trade(_)) => ReplayEventFamily::Trade,
            Self::Derivative(DerivativeEvent::Instrument(_)) => ReplayEventFamily::Instrument,
            Self::Derivative(DerivativeEvent::MarkIndex(_)) => ReplayEventFamily::MarkIndex,
            Self::Derivative(DerivativeEvent::FundingEstimate(_)) => {
                ReplayEventFamily::FundingEstimate
            }
            Self::Derivative(DerivativeEvent::FundingSettlement(_)) => {
                ReplayEventFamily::FundingSettlement
            }
            Self::Derivative(DerivativeEvent::OpenInterest(_)) => ReplayEventFamily::OpenInterest,
            Self::Derivative(DerivativeEvent::TraderRatio(_)) => ReplayEventFamily::TraderRatio,
            Self::Derivative(DerivativeEvent::QuoteConversion(_)) => {
                ReplayEventFamily::QuoteConversion
            }
            Self::Decision(_) => ReplayEventFamily::Decision,
        }
    }

    fn venue(&self) -> AdapterId {
        match self {
            Self::Market(value) => value.meta().adapter,
            Self::Derivative(value) => value.meta().venue,
            Self::Decision(value) => value.long_venue,
        }
    }

    fn symbol(&self) -> &CanonicalSymbol {
        match self {
            Self::Market(value) => &value.meta().symbol,
            Self::Derivative(value) => &value.meta().symbol,
            Self::Decision(value) => &value.symbol,
        }
    }

    fn source_ts_us(&self) -> Option<i64> {
        match self {
            Self::Market(value) => value
                .meta()
                .exchange_trade_ts_us
                .or(value.meta().exchange_event_ts_us),
            Self::Derivative(value) => value.meta().source_ts_us,
            Self::Decision(_) => None,
        }
    }

    fn source_sequence(&self) -> Option<u64> {
        match self {
            Self::Market(value) => value.meta().source_sequence,
            Self::Derivative(_) | Self::Decision(_) => None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ReplayKey {
    available_us: i64,
    family_rank: u8,
    venue_rank: u8,
    base: String,
    quote: String,
    source_ts_us: Option<i64>,
    source_sequence: Option<u64>,
    event_id: Uuid,
}

impl Ord for ReplayKey {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            self.available_us,
            self.family_rank,
            self.venue_rank,
            &self.base,
            &self.quote,
            self.source_sequence,
            self.source_ts_us,
            self.event_id.as_bytes(),
        )
            .cmp(&(
                other.available_us,
                other.family_rank,
                other.venue_rank,
                &other.base,
                &other.quote,
                other.source_sequence,
                other.source_ts_us,
                other.event_id.as_bytes(),
            ))
    }
}

impl PartialOrd for ReplayKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

type MarketKey = (AdapterId, CanonicalSymbol);
type FamilyMarketKey = (ReplayEventFamily, AdapterId, CanonicalSymbol, u8);

struct ReplayState {
    config: ReplayConfig,
    books: HashMap<MarketKey, (Option<BookSnapshot>, BookSnapshot)>,
    flows: HashMap<MarketKey, TradeWindow>,
    marks: HashMap<MarketKey, MarkIndexSnapshot>,
    metadata: MetadataAligner,
    source_watermarks: HashMap<FamilyMarketKey, i64>,
    flow_evidence: HashMap<MarketKey, Vec<(i64, Uuid)>>,
}

impl ReplayState {
    fn new(config: ReplayConfig) -> Result<Self, ReplayRejectionReason> {
        for (field, valid) in [
            ("book_freshness_us", config.book_freshness_us >= 0),
            ("metadata_freshness_us", config.metadata_freshness_us >= 0),
            ("mark_freshness_us", config.mark_freshness_us >= 0),
            ("flow_window_us", config.flow_window_us > 0),
            ("dedupe_capacity", config.dedupe_capacity > 0),
        ] {
            if !valid {
                return Err(ReplayRejectionReason::InvalidConfig {
                    field: field.to_owned(),
                });
            }
        }
        Ok(Self {
            config,
            books: HashMap::new(),
            flows: HashMap::new(),
            marks: HashMap::new(),
            metadata: MetadataAligner::with_dedupe_capacity(config.dedupe_capacity)
                .map_err(|reason| ReplayRejectionReason::MetadataUnavailable { reason })?,
            source_watermarks: HashMap::new(),
            flow_evidence: HashMap::new(),
        })
    }

    fn apply(
        &mut self,
        event: &ReplayEvent,
    ) -> Result<Option<ReplayDecisionRecord>, ReplayRejectionReason> {
        validate_causal_clock(event)?;
        match event {
            ReplayEvent::Market(value) => {
                validate_event(value).map_err(|error| ReplayRejectionReason::InvalidInput {
                    detail: error.to_string(),
                })?;
                self.check_regression(event, false)?;
                match value {
                    NormalizedEvent::Book(book) => {
                        let key = (book.meta.adapter, book.meta.symbol.clone());
                        let source = market_source(&book.meta, false);
                        self.flow(&key)?
                            .observe_watermark(source)
                            .map_err(feature_error)?;
                        let previous = self.books.remove(&key).map(|(_, current)| current);
                        self.books.insert(key.clone(), (previous, book.clone()));
                    }
                    NormalizedEvent::Trade(trade) => {
                        let key = (trade.meta.adapter, trade.meta.symbol.clone());
                        match self.flow(&key)?.push(trade).map_err(feature_error)? {
                            TradePushOutcome::Accepted => {
                                self.flow_evidence.entry(key).or_default().push((
                                    trade
                                        .meta
                                        .exchange_trade_ts_us
                                        .or(trade.meta.exchange_event_ts_us)
                                        .unwrap_or(trade.meta.local_recv_ts_us),
                                    trade.meta.event_id,
                                ));
                            }
                            TradePushOutcome::Duplicate => {}
                            TradePushOutcome::RejectedOutOfOrder {
                                previous_ts_us,
                                current_ts_us,
                            } => {
                                return Err(ReplayRejectionReason::RegressingInput {
                                    previous_ts_us,
                                    current_ts_us,
                                });
                            }
                        }
                    }
                }
                self.commit_watermark(event);
                Ok(None)
            }
            ReplayEvent::Derivative(value) => {
                self.check_regression(event, true)?;
                match value {
                    DerivativeEvent::MarkIndex(mark) => {
                        if mark.mark_price <= 0 || mark.index_price <= 0 {
                            return Err(ReplayRejectionReason::InvalidInput {
                                detail: "mark and index prices must be positive".into(),
                            });
                        }
                        self.marks
                            .insert((mark.meta.venue, mark.meta.symbol.clone()), mark.clone());
                    }
                    DerivativeEvent::FundingEstimate(value) => {
                        let _ = self
                            .metadata
                            .observe_funding(value.clone())
                            .map_err(metadata_error)?;
                    }
                    DerivativeEvent::OpenInterest(value) => {
                        let _ = self
                            .metadata
                            .observe_open_interest(value.clone())
                            .map_err(metadata_error)?;
                    }
                    DerivativeEvent::TraderRatio(value) => {
                        let _ = self
                            .metadata
                            .observe_trader_ratio(value.clone())
                            .map_err(metadata_error)?;
                    }
                    DerivativeEvent::FundingSettlement(value) => {
                        if value.settlement_ts_us <= 0 {
                            return Err(ReplayRejectionReason::InvalidInput {
                                detail: "settlement timestamp must be positive".into(),
                            });
                        }
                    }
                    DerivativeEvent::Instrument(_) | DerivativeEvent::QuoteConversion(_) => {}
                }
                self.commit_watermark(event);
                Ok(None)
            }
            ReplayEvent::Decision(value) => {
                validate_decision(value)?;
                Ok(Some(self.decide(value.clone())))
            }
        }
    }

    fn flow(&mut self, key: &MarketKey) -> Result<&mut TradeWindow, ReplayRejectionReason> {
        if !self.flows.contains_key(key) {
            self.flows.insert(
                key.clone(),
                TradeWindow::new(self.config.flow_window_us).map_err(feature_error)?,
            );
        }
        Ok(self.flows.get_mut(key).expect("inserted above"))
    }

    fn check_regression(
        &self,
        event: &ReplayEvent,
        equal_is_conflict: bool,
    ) -> Result<(), ReplayRejectionReason> {
        let Some(current_ts_us) = event.source_ts_us() else {
            return Ok(());
        };
        let key = (
            event.family(),
            event.venue(),
            event.symbol().clone(),
            subfamily_rank(event),
        );
        if let Some(previous_ts_us) = self.source_watermarks.get(&key) {
            if current_ts_us < *previous_ts_us {
                return Err(ReplayRejectionReason::RegressingInput {
                    previous_ts_us: *previous_ts_us,
                    current_ts_us,
                });
            }
            if equal_is_conflict && current_ts_us == *previous_ts_us {
                return Err(ReplayRejectionReason::TimestampConflict {
                    timestamp_us: current_ts_us,
                });
            }
        }
        Ok(())
    }

    fn commit_watermark(&mut self, event: &ReplayEvent) {
        if let Some(timestamp) = event.source_ts_us() {
            self.source_watermarks.insert(
                (
                    event.family(),
                    event.venue(),
                    event.symbol().clone(),
                    subfamily_rank(event),
                ),
                timestamp,
            );
        }
    }

    fn decide(&mut self, decision: DecisionEvent) -> ReplayDecisionRecord {
        let venues = [decision.long_venue, decision.short_venue];
        let mut books = Vec::new();
        let mut flows = Vec::new();
        let mut evidence = Vec::new();
        let mut unavailable = None;
        for venue in venues {
            let key = (venue, decision.symbol.clone());
            match self.books.get(&key) {
                Some((previous, current)) => {
                    let feature = compute_book_features(
                        previous.as_ref(),
                        current,
                        decision.requested_base,
                        decision.local_recv_ts_us,
                        self.config.book_freshness_us,
                    );
                    evidence.push(current.meta.event_id);
                    if let Some(previous) = previous {
                        evidence.push(previous.meta.event_id);
                    }
                    books.push(feature);
                }
                None => {
                    unavailable.get_or_insert(ReplayRejectionReason::MissingBook { venue });
                }
            };
            match self.flow(&key) {
                Ok(flow) => flows.push(flow.snapshot(decision.local_recv_ts_us)),
                Err(reason) => {
                    unavailable.get_or_insert(reason);
                }
            }
            if let Some(items) = self.flow_evidence.get(&key) {
                let start = decision
                    .local_recv_ts_us
                    .saturating_sub(self.config.flow_window_us);
                evidence.extend(
                    items
                        .iter()
                        .filter(|(ts, _)| *ts >= start && *ts <= decision.local_recv_ts_us)
                        .map(|(_, id)| *id),
                );
            }
        }
        let open_interest = venues
            .into_iter()
            .map(|venue| {
                match self.metadata.open_interest_feature(
                    venue,
                    &decision.symbol,
                    decision.local_recv_ts_us,
                    self.config.metadata_freshness_us,
                ) {
                    Ok(feature) => {
                        evidence.push(feature.source.event_id);
                        AlignedOpenInterest {
                            venue,
                            feature: Some(feature),
                            rejection: None,
                        }
                    }
                    Err(reason) => AlignedOpenInterest {
                        venue,
                        feature: None,
                        rejection: Some(reason),
                    },
                }
            })
            .collect();
        let ratio_specs = [
            (
                AdapterId::BinanceUsdm,
                TraderMetricKind::BinanceTopAccountRatio,
            ),
            (
                AdapterId::BinanceUsdm,
                TraderMetricKind::BinanceTopPositionRatio,
            ),
            (
                AdapterId::BybitLinear,
                TraderMetricKind::BybitLongShortRatio,
            ),
        ];
        let trader_ratios = ratio_specs
            .into_iter()
            .map(|(venue, metric_kind)| {
                match self.metadata.trader_ratio_feature(
                    venue,
                    &decision.symbol,
                    metric_kind,
                    decision.local_recv_ts_us,
                    self.config.metadata_freshness_us,
                ) {
                    Ok(feature) => {
                        evidence.push(feature.source.event_id);
                        AlignedTraderRatio {
                            venue,
                            metric_kind,
                            feature: Some(feature),
                            rejection: None,
                        }
                    }
                    Err(reason) => AlignedTraderRatio {
                        venue,
                        metric_kind,
                        feature: None,
                        rejection: Some(reason),
                    },
                }
            })
            .collect();

        let outcome = unavailable.map_or_else(
            || self.evaluate(&decision, &books, &mut evidence),
            ReplayDecisionOutcome::Unavailable,
        );
        evidence.sort_unstable_by_key(|id| *id.as_bytes());
        evidence.dedup();
        ReplayDecisionRecord {
            decision,
            book_features: books,
            flow_features: flows,
            open_interest,
            trader_ratios,
            evidence_event_ids: evidence,
            outcome,
        }
    }

    fn evaluate(
        &self,
        decision: &DecisionEvent,
        books: &[funding_core::feature::BookFeatures],
        evidence: &mut Vec<Uuid>,
    ) -> ReplayDecisionOutcome {
        let Some(long_book) = books
            .iter()
            .find(|value| value.source.adapter == decision.long_venue)
        else {
            return ReplayDecisionOutcome::Unavailable(ReplayRejectionReason::MissingBook {
                venue: decision.long_venue,
            });
        };
        let Some(short_book) = books
            .iter()
            .find(|value| value.source.adapter == decision.short_venue)
        else {
            return ReplayDecisionOutcome::Unavailable(ReplayRejectionReason::MissingBook {
                venue: decision.short_venue,
            });
        };
        let long_quote = match replay_quote(long_book, NbboSide::Ask, decision.local_recv_ts_us) {
            Ok(value) => value,
            Err(reason) => return ReplayDecisionOutcome::Unavailable(reason),
        };
        let short_quote = match replay_quote(short_book, NbboSide::Bid, decision.local_recv_ts_us) {
            Ok(value) => value,
            Err(reason) => return ReplayDecisionOutcome::Unavailable(reason),
        };
        let long_price = named_from_quote(&long_quote, PriceKind::PerpetualBuyFromAsks);
        let short_price = named_from_quote(&short_quote, PriceKind::PerpetualSellIntoBids);
        let entry_basis = match basis_bps(
            long_price,
            short_price,
            decision.local_recv_ts_us,
            self.config.book_freshness_us,
        ) {
            Ok(value) => value,
            Err(error) => return ReplayDecisionOutcome::Unavailable(feature_error(error)),
        };
        let long_mark = match self.mark(
            decision.long_venue,
            &decision.symbol,
            decision.local_recv_ts_us,
        ) {
            Ok(value) => value,
            Err(reason) => return ReplayDecisionOutcome::Unavailable(reason),
        };
        let short_mark = match self.mark(
            decision.short_venue,
            &decision.symbol,
            decision.local_recv_ts_us,
        ) {
            Ok(value) => value,
            Err(reason) => return ReplayDecisionOutcome::Unavailable(reason),
        };
        let long_funding = match self.metadata.funding_feature(
            decision.long_venue,
            &decision.symbol,
            decision.local_recv_ts_us,
            self.config.metadata_freshness_us,
        ) {
            Ok(value) => value,
            Err(reason) => return ReplayDecisionOutcome::Unavailable(metadata_error(reason)),
        };
        let short_funding = match self.metadata.funding_feature(
            decision.short_venue,
            &decision.symbol,
            decision.local_recv_ts_us,
            self.config.metadata_freshness_us,
        ) {
            Ok(value) => value,
            Err(reason) => return ReplayDecisionOutcome::Unavailable(metadata_error(reason)),
        };
        evidence.extend([
            long_mark.source.event_id,
            short_mark.source.event_id,
            long_funding.source.event_id,
            short_funding.source.event_id,
        ]);
        ReplayDecisionOutcome::Evaluated(evaluate_candidate(CandidateInput {
            entry_basis: &entry_basis,
            long_quote: &long_quote,
            short_quote: &short_quote,
            long_funding: &long_funding,
            short_funding: &short_funding,
            long_mark: MarkPriceInput {
                price: &long_mark,
                freshness_limit_us: self.config.mark_freshness_us,
            },
            short_mark: MarkPriceInput {
                price: &short_mark,
                freshness_limit_us: self.config.mark_freshness_us,
            },
            cost_model: &decision.cost_model,
            minimum_net_bps: decision.minimum_net_bps,
            holding_end_ts_us: decision.holding_end_ts_us,
            caps: &decision.capacity_evidence,
        }))
    }

    fn mark(
        &self,
        venue: AdapterId,
        symbol: &CanonicalSymbol,
        decision_ts_us: i64,
    ) -> Result<NamedPrice, ReplayRejectionReason> {
        let mark = self
            .marks
            .get(&(venue, symbol.clone()))
            .ok_or(ReplayRejectionReason::MissingMark { venue })?;
        if mark.meta.local_recv_ts_us > decision_ts_us {
            return Err(ReplayRejectionReason::SourceAfterAvailability {
                source_ts_us: mark.meta.local_recv_ts_us,
                local_recv_ts_us: decision_ts_us,
            });
        }
        let age = decision_ts_us - mark.meta.local_recv_ts_us;
        if age > self.config.mark_freshness_us {
            return Err(ReplayRejectionReason::FeatureUnavailable {
                detail: format!("stale mark: age_us={age}"),
            });
        }
        evidence_price(mark, PriceKind::Mark, mark.mark_price)
    }
}

pub fn run_replay(
    config: ReplayConfig,
    mut events: Vec<ReplayEvent>,
) -> Result<ReplayReport, ReplayRejectionReason> {
    let mut state = ReplayState::new(config)?;
    let input_events = events.len() as u64;
    events.sort_by(|left, right| {
        replay_key(left)
            .cmp(&replay_key(right))
            .then_with(|| canonical_event_bytes(left).cmp(&canonical_event_bytes(right)))
    });
    let mut identities: HashMap<Uuid, Vec<Vec<u8>>> = HashMap::new();
    for event in &events {
        identities
            .entry(event.event_id())
            .or_default()
            .push(canonical_event_bytes(event));
    }
    let conflicted: std::collections::HashSet<Uuid> = identities
        .into_iter()
        .filter_map(|(id, payloads)| {
            payloads
                .windows(2)
                .any(|pair| pair[0] != pair[1])
                .then_some(id)
        })
        .collect();
    let mut seen: HashMap<Uuid, ReplayEvent> = HashMap::new();
    let mut canonical = Vec::new();
    put(&mut canonical, b"funding-replay-input-v1");
    for event in &events {
        canonical_event(&mut canonical, event);
    }
    let mut report = ReplayReport {
        canonical_encoding_version: CANONICAL_ENCODING_VERSION,
        digest_algorithm: "SHA-256".into(),
        event_digest_hex: String::new(),
        config,
        simulation_enabled: false,
        paper_validation_only: true,
        first_clock_us: None,
        last_clock_us: None,
        event_counts: BTreeMap::new(),
        rejection_counts: BTreeMap::new(),
        causality_violations: 0,
        decisions: Vec::new(),
        rejections: Vec::new(),
        reconciliation: ReplayReconciliation {
            input_events,
            applied_events: 0,
            duplicate_events: 0,
            rejected_events: 0,
            decisions_recorded: 0,
            candidate_evaluations: 0,
            eligible_candidates: 0,
            rejected_candidates: 0,
        },
    };
    for event in events {
        let id = event.event_id();
        report
            .first_clock_us
            .get_or_insert(event.local_recv_ts_us());
        report.last_clock_us = Some(event.local_recv_ts_us());
        if conflicted.contains(&id) {
            reject(
                &mut report,
                &event,
                ReplayRejectionReason::DuplicateEventIdConflict { event_id: id },
            );
            continue;
        }
        if let Some(previous) = seen.get(&id) {
            if previous == &event {
                report.reconciliation.duplicate_events += 1;
                continue;
            }
            reject(
                &mut report,
                &event,
                ReplayRejectionReason::DuplicateEventIdConflict { event_id: id },
            );
            continue;
        }
        seen.insert(id, event.clone());
        match state.apply(&event) {
            Ok(record) => {
                report.reconciliation.applied_events += 1;
                *report
                    .event_counts
                    .entry(event.family().code().into())
                    .or_default() += 1;
                if let Some(record) = record {
                    report.reconciliation.decisions_recorded += 1;
                    if let ReplayDecisionOutcome::Evaluated(ref evaluation) = record.outcome {
                        report.reconciliation.candidate_evaluations += 1;
                        match evaluation {
                            CandidateEvaluation::Eligible(_) => {
                                report.reconciliation.eligible_candidates += 1
                            }
                            CandidateEvaluation::Rejected(value) => {
                                report.reconciliation.rejected_candidates += 1;
                                *report
                                    .rejection_counts
                                    .entry(opportunity_reason_code(&value.reason).into())
                                    .or_default() += 1;
                            }
                        }
                    } else if let ReplayDecisionOutcome::Unavailable(ref reason) = record.outcome {
                        *report
                            .rejection_counts
                            .entry(reason.code().into())
                            .or_default() += 1;
                    }
                    report.decisions.push(record);
                }
            }
            Err(reason) => reject(&mut report, &event, reason),
        }
    }
    report.event_digest_hex = hex(digest(&SHA256, &canonical).as_ref());
    if !report.reconciliation.input_identity_holds()
        || !report.reconciliation.candidate_identity_holds()
    {
        return Err(ReplayRejectionReason::ReconciliationFailure);
    }
    Ok(report)
}

fn reject(report: &mut ReplayReport, event: &ReplayEvent, reason: ReplayRejectionReason) {
    if matches!(
        reason,
        ReplayRejectionReason::SourceAfterAvailability { .. }
            | ReplayRejectionReason::RegressingInput { .. }
            | ReplayRejectionReason::TimestampConflict { .. }
            | ReplayRejectionReason::DuplicateEventIdConflict { .. }
    ) {
        report.causality_violations += 1;
    }
    *report
        .rejection_counts
        .entry(reason.code().into())
        .or_default() += 1;
    report.reconciliation.rejected_events += 1;
    report.rejections.push(ReplayRejection {
        event_id: event.event_id(),
        family: event.family(),
        local_recv_ts_us: event.local_recv_ts_us(),
        reason,
    });
}

fn replay_key(event: &ReplayEvent) -> ReplayKey {
    ReplayKey {
        available_us: event.local_recv_ts_us(),
        family_rank: family_rank(event.family()),
        venue_rank: venue_rank(event.venue()),
        base: event.symbol().base.clone(),
        quote: event.symbol().quote.clone(),
        source_ts_us: event.source_ts_us(),
        source_sequence: event.source_sequence(),
        event_id: event.event_id(),
    }
}

const fn family_rank(value: ReplayEventFamily) -> u8 {
    match value {
        ReplayEventFamily::Instrument => 0,
        ReplayEventFamily::Book => 1,
        ReplayEventFamily::Trade => 2,
        ReplayEventFamily::MarkIndex => 3,
        ReplayEventFamily::FundingEstimate => 4,
        ReplayEventFamily::FundingSettlement => 5,
        ReplayEventFamily::OpenInterest => 6,
        ReplayEventFamily::TraderRatio => 7,
        ReplayEventFamily::QuoteConversion => 8,
        ReplayEventFamily::Decision => 255,
    }
}

const fn venue_rank(value: AdapterId) -> u8 {
    match value {
        AdapterId::BinanceUsdm => 0,
        AdapterId::BybitLinear => 1,
        AdapterId::BinanceSpot => 2,
        AdapterId::UpbitSpot => 3,
        AdapterId::BithumbSpot => 4,
    }
}

fn subfamily_rank(event: &ReplayEvent) -> u8 {
    match event {
        ReplayEvent::Derivative(DerivativeEvent::TraderRatio(value)) => {
            trader_kind(value.metric_kind)
        }
        ReplayEvent::Derivative(DerivativeEvent::QuoteConversion(value)) => match value.side {
            funding_core::public::QuoteSide::Bid => 0,
            funding_core::public::QuoteSide::Ask => 1,
        },
        _ => 0,
    }
}

fn validate_decision(value: &DecisionEvent) -> Result<(), ReplayRejectionReason> {
    let invalid = |field: &str| ReplayRejectionReason::InvalidDecision {
        field: field.to_owned(),
    };
    if value.symbol.base.is_empty()
        || !value
            .symbol
            .base
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
        || value.symbol.quote != "USDT"
    {
        return Err(invalid("symbol"));
    }
    if value.long_venue == value.short_venue
        || !matches!(
            value.long_venue,
            AdapterId::BinanceUsdm | AdapterId::BybitLinear
        )
        || !matches!(
            value.short_venue,
            AdapterId::BinanceUsdm | AdapterId::BybitLinear
        )
    {
        return Err(invalid("venues"));
    }
    if value.requested_base.scaled() <= 0 {
        return Err(invalid("requested_base"));
    }
    if value.holding_end_ts_us <= value.local_recv_ts_us {
        return Err(invalid("holding_end_ts_us"));
    }
    if value.minimum_net_bps.scaled() < 0 {
        return Err(invalid("minimum_net_bps"));
    }
    for cap in &value.capacity_evidence {
        let identity_valid = match cap.leg {
            CapacityLeg::Long => cap.venue == Some(value.long_venue),
            CapacityLeg::Short => cap.venue == Some(value.short_venue),
            CapacityLeg::Pair => cap.venue.is_none(),
        };
        if !matches!(
            cap.source,
            CapacitySource::ConfiguredResearchLimit
                | CapacitySource::RiskLimit
                | CapacitySource::AuthenticatedMargin
        ) || !matches!(cap.validity, CapacityEvidenceValidity::Available)
            || !identity_valid
            || cap.symbol.as_ref() != Some(&value.symbol)
            || (cap.capacity_base.is_none() && cap.capacity_quote.is_none())
            || cap.capacity_base.is_some_and(|v| v.scaled() <= 0)
            || cap.capacity_quote.is_some_and(|v| v.scaled() <= 0)
            || !matches!(cap.source_ts_us, Some(ts) if ts > 0 && ts <= value.local_recv_ts_us)
        {
            return Err(invalid("capacity_evidence"));
        }
    }
    let costs = [&value.cost_model.binance, &value.cost_model.bybit];
    if costs.into_iter().any(|cost| {
        cost.entry_fee.rate.scaled() <= 0
            || cost.exit_fee.rate.scaled() <= 0
            || cost.entry_fee.rate.scaled() > ExactDecimal::SCALE
            || cost.exit_fee.rate.scaled() > ExactDecimal::SCALE
            || cost.entry_fee.liquidity != FeeLiquidity::Taker
            || cost.exit_fee.liquidity != FeeLiquidity::Taker
            || [
                cost.entry_slippage_bps,
                cost.exit_slippage_bps,
                cost.entry_book_impact_bps,
                cost.exit_book_impact_bps,
            ]
            .into_iter()
            .any(|bps| bps.scaled() < 0)
    }) || [
        value.cost_model.basis_risk_buffer_bps,
        value.cost_model.funding_error_buffer_bps,
        value.cost_model.leg_risk_buffer_bps,
    ]
    .into_iter()
    .any(|bps| bps.scaled() < 0)
    {
        return Err(invalid("cost_model"));
    }
    Ok(())
}

fn validate_causal_clock(event: &ReplayEvent) -> Result<(), ReplayRejectionReason> {
    let available = event.local_recv_ts_us();
    if available <= 0 {
        return Err(ReplayRejectionReason::InvalidAvailabilityTimestamp {
            timestamp_us: available,
        });
    }
    let timestamps: [Option<i64>; 2] = match event {
        ReplayEvent::Market(value) => [
            value.meta().exchange_event_ts_us,
            value.meta().exchange_trade_ts_us,
        ],
        ReplayEvent::Derivative(value) => [value.meta().source_ts_us, None],
        ReplayEvent::Decision(_) => [None, None],
    };
    for source in timestamps.into_iter().flatten() {
        if source <= 0 || source > available {
            return Err(ReplayRejectionReason::SourceAfterAvailability {
                source_ts_us: source,
                local_recv_ts_us: available,
            });
        }
    }
    Ok(())
}

fn replay_quote(
    book: &funding_core::feature::BookFeatures,
    side: NbboSide,
    decision_ts_us: i64,
) -> Result<NbboQuote, ReplayRejectionReason> {
    let quote = match side {
        NbboSide::Ask => &book.buy_from_asks,
        NbboSide::Bid => &book.sell_into_bids,
    };
    if !matches!(quote.validity, QuoteValidity::Valid) {
        return Err(ReplayRejectionReason::FeatureUnavailable {
            detail: format!("invalid executable quote: {:?}", quote.validity),
        });
    }
    let price = quote
        .average_price
        .ok_or_else(|| ReplayRejectionReason::FeatureUnavailable {
            detail: "missing executable average price".into(),
        })?;
    let age_us = decision_ts_us
        .checked_sub(book.source.local_recv_ts_us)
        .ok_or_else(|| ReplayRejectionReason::FeatureUnavailable {
            detail: "book age overflow".into(),
        })?;
    Ok(NbboQuote {
        venue: book.source.adapter,
        instrument_kind: InstrumentKind::Perpetual,
        symbol: book.source.symbol.clone(),
        side,
        price,
        requested_base: quote.requested_base,
        available_base: quote.available_base,
        quote_notional: quote.quote_notional,
        levels_consumed: quote.levels_consumed,
        age_us,
        source: book.source.clone(),
    })
}

fn named_from_quote(quote: &NbboQuote, kind: PriceKind) -> NamedPrice {
    NamedPrice {
        venue: quote.venue,
        instrument_kind: InstrumentKind::Perpetual,
        kind,
        value: quote.price,
        source: quote.source.clone(),
    }
}

fn evidence_price(
    mark: &MarkIndexSnapshot,
    kind: PriceKind,
    value: i128,
) -> Result<NamedPrice, ReplayRejectionReason> {
    let value =
        ExactDecimal::from_scaled(value).map_err(|error| ReplayRejectionReason::InvalidInput {
            detail: error.to_string(),
        })?;
    let effective_ts_us = mark.meta.source_ts_us.unwrap_or(mark.meta.local_recv_ts_us);
    Ok(NamedPrice {
        venue: mark.meta.venue,
        instrument_kind: InstrumentKind::Perpetual,
        kind,
        value,
        source: FeatureSource {
            event_id: mark.meta.event_id,
            adapter: mark.meta.venue,
            symbol: mark.meta.symbol.clone(),
            source_sequence: None,
            exchange_event_ts_us: mark.meta.source_ts_us,
            exchange_trade_ts_us: None,
            local_recv_ts_us: mark.meta.local_recv_ts_us,
            effective_ts_us,
            effective_ts_source: if mark.meta.source_ts_us.is_some() {
                EffectiveTimestampSource::ExchangeEvent
            } else {
                EffectiveTimestampSource::LocalReceive
            },
        },
    })
}

fn market_source(meta: &md_core::model::EventMeta, trade: bool) -> FeatureSource {
    let effective = if trade {
        meta.exchange_trade_ts_us.or(meta.exchange_event_ts_us)
    } else {
        meta.exchange_event_ts_us
    };
    FeatureSource {
        event_id: meta.event_id,
        adapter: meta.adapter,
        symbol: meta.symbol.clone(),
        source_sequence: meta.source_sequence,
        exchange_event_ts_us: meta.exchange_event_ts_us,
        exchange_trade_ts_us: meta.exchange_trade_ts_us,
        local_recv_ts_us: meta.local_recv_ts_us,
        effective_ts_us: effective.unwrap_or(meta.local_recv_ts_us),
        effective_ts_source: if trade && meta.exchange_trade_ts_us.is_some() {
            EffectiveTimestampSource::ExchangeTrade
        } else if effective.is_some() {
            EffectiveTimestampSource::ExchangeEvent
        } else {
            EffectiveTimestampSource::LocalReceive
        },
    }
}

fn metadata_error(reason: MetadataInvalidReason) -> ReplayRejectionReason {
    ReplayRejectionReason::MetadataUnavailable { reason }
}
fn feature_error(error: impl std::fmt::Debug) -> ReplayRejectionReason {
    ReplayRejectionReason::FeatureUnavailable {
        detail: format!("{error:?}"),
    }
}

// Canonical v1 uses fixed tags, big-endian fixed-width integers, one-byte
// optional presence tags, raw UUID bytes, and length-prefixed UTF-8/arrays.
fn canonical_event(out: &mut Vec<u8>, event: &ReplayEvent) {
    out.push(family_rank(event.family()));
    match event {
        ReplayEvent::Market(value) => match value {
            NormalizedEvent::Book(book) => {
                market_meta(out, &book.meta);
                array_len(out, book.bids.len());
                for level in &book.bids {
                    i128be(out, level.price);
                    i128be(out, level.quantity);
                }
                array_len(out, book.asks.len());
                for level in &book.asks {
                    i128be(out, level.price);
                    i128be(out, level.quantity);
                }
            }
            NormalizedEvent::Trade(trade) => {
                market_meta(out, &trade.meta);
                string(out, &trade.trade_id);
                i128be(out, trade.price);
                i128be(out, trade.quantity);
                out.push(match trade.taker_side {
                    md_core::model::TakerSide::Buy => 0,
                    md_core::model::TakerSide::Sell => 1,
                    md_core::model::TakerSide::Unknown => 2,
                });
            }
        },
        ReplayEvent::Derivative(value) => {
            derivative_meta(out, value.meta());
            match value {
                DerivativeEvent::Instrument(v) => {
                    out.push(match v.contract_kind {
                        funding_core::instrument::ContractKind::Perpetual => 0,
                    });
                    string(out, &v.settlement_asset);
                    for n in [
                        v.contract_multiplier,
                        v.tick_size,
                        v.quantity_step,
                        v.min_quantity,
                    ] {
                        i128be(out, n);
                    }
                    opt_i128(out, v.max_quantity);
                    i128be(out, v.min_notional);
                    u32be(out, v.funding_interval_secs);
                    out.push(interval_provenance(v.funding_interval_provenance));
                    opt_i128(out, v.funding_rate_floor);
                    opt_i128(out, v.funding_rate_cap);
                    out.push(match v.funding_rate_bounds_provenance {
                        funding_core::instrument::FundingRateBoundsProvenance::VenueFundingInfo => {
                            0
                        }
                        funding_core::instrument::FundingRateBoundsProvenance::Unknown => 1,
                    });
                    opt_i128(out, v.price_lower_bound);
                    opt_i128(out, v.price_upper_bound);
                    array_len(out, v.supported_position_modes.len());
                    for mode in &v.supported_position_modes {
                        out.push(match mode {
                            funding_core::instrument::PositionMode::OneWay => 0,
                            funding_core::instrument::PositionMode::Hedge => 1,
                        });
                    }
                    array_len(out, v.supported_account_modes.len());
                    for mode in &v.supported_account_modes {
                        out.push(match mode {
                            funding_core::instrument::AccountMode::Classic => 0,
                            funding_core::instrument::AccountMode::Unified => 1,
                            funding_core::instrument::AccountMode::Portfolio => 2,
                        });
                    }
                }
                DerivativeEvent::MarkIndex(v) => {
                    i128be(out, v.mark_price);
                    i128be(out, v.index_price);
                }
                DerivativeEvent::FundingEstimate(v) => {
                    funding_common(
                        out,
                        v.rate,
                        v.rate_kind,
                        v.basis,
                        v.interval_secs,
                        v.interval_provenance,
                    );
                    i64be(out, v.next_funding_ts_us);
                }
                DerivativeEvent::FundingSettlement(v) => {
                    funding_common(
                        out,
                        v.rate,
                        v.rate_kind,
                        v.basis,
                        v.interval_secs,
                        v.interval_provenance,
                    );
                    i64be(out, v.settlement_ts_us);
                }
                DerivativeEvent::OpenInterest(v) => {
                    i128be(out, v.open_interest);
                    out.push(match v.unit {
                        funding_core::public::OpenInterestUnit::Contracts => 0,
                        funding_core::public::OpenInterestUnit::BaseAsset => 1,
                    });
                    opt_i128(out, v.quote_notional);
                }
                DerivativeEvent::TraderRatio(v) => {
                    out.push(trader_kind(v.metric_kind));
                    i128be(out, v.long_ratio);
                    i128be(out, v.short_ratio);
                    i128be(out, v.long_short_ratio);
                }
                DerivativeEvent::QuoteConversion(v) => {
                    out.push(match v.side {
                        funding_core::public::QuoteSide::Bid => 0,
                        funding_core::public::QuoteSide::Ask => 1,
                    });
                    i128be(out, v.price);
                    i128be(out, v.executable_quantity);
                }
            }
        }
        ReplayEvent::Decision(v) => decision_payload(out, v),
    }
}
fn canonical_event_bytes(event: &ReplayEvent) -> Vec<u8> {
    let mut bytes = Vec::new();
    canonical_event(&mut bytes, event);
    bytes
}

fn opportunity_reason_code(reason: &OpportunityRejectionReason) -> &'static str {
    match reason {
        OpportunityRejectionReason::InvalidHoldingWindow => "OPPORTUNITY_INVALID_HOLDING_WINDOW",
        OpportunityRejectionReason::InvalidEntryBasis => "OPPORTUNITY_INVALID_ENTRY_BASIS",
        OpportunityRejectionReason::IdentityMismatch => "OPPORTUNITY_IDENTITY_MISMATCH",
        OpportunityRejectionReason::NonPositiveRequestedBase => {
            "OPPORTUNITY_NON_POSITIVE_REQUESTED_BASE"
        }
        OpportunityRejectionReason::RequestedQuantityMismatch => {
            "OPPORTUNITY_REQUESTED_QUANTITY_MISMATCH"
        }
        OpportunityRejectionReason::MissingNotionalEvidence => {
            "OPPORTUNITY_MISSING_NOTIONAL_EVIDENCE"
        }
        OpportunityRejectionReason::FundingEvidenceMismatch => {
            "OPPORTUNITY_FUNDING_EVIDENCE_MISMATCH"
        }
        OpportunityRejectionReason::InvalidCostModel => "OPPORTUNITY_INVALID_COST_MODEL",
        OpportunityRejectionReason::FutureEvidence { .. } => "OPPORTUNITY_FUTURE_EVIDENCE",
        OpportunityRejectionReason::StaleEvidence { .. } => "OPPORTUNITY_STALE_EVIDENCE",
        OpportunityRejectionReason::InsufficientCapacity { .. } => {
            "OPPORTUNITY_INSUFFICIENT_CAPACITY"
        }
        OpportunityRejectionReason::NoAnnouncedSettlementInWindow => {
            "OPPORTUNITY_NO_ANNOUNCED_SETTLEMENT_IN_WINDOW"
        }
        OpportunityRejectionReason::ArithmeticOverflow => "OPPORTUNITY_ARITHMETIC_OVERFLOW",
        OpportunityRejectionReason::NetEdgeNotPositive => "OPPORTUNITY_NET_EDGE_NOT_POSITIVE",
        OpportunityRejectionReason::NetEdgeBelowMinimum { .. } => {
            "OPPORTUNITY_NET_EDGE_BELOW_MINIMUM"
        }
    }
}
fn market_meta(out: &mut Vec<u8>, v: &md_core::model::EventMeta) {
    u16be(out, v.schema_version);
    out.extend_from_slice(v.event_id.as_bytes());
    out.push(venue_rank(v.adapter));
    symbol(out, &v.symbol);
    string(out, &v.source_symbol);
    string(out, &v.source_stream);
    opt_u64(out, v.source_sequence);
    opt_i64(out, v.exchange_event_ts_us);
    opt_i64(out, v.exchange_trade_ts_us);
    out.push(precision(v.event_ts_precision));
    out.push(precision(v.trade_ts_precision));
    i64be(out, v.local_recv_ts_us);
    u32be(out, v.raw_size_bytes);
}
fn derivative_meta(out: &mut Vec<u8>, v: &DerivativeMeta) {
    u16be(out, v.schema_version);
    out.extend_from_slice(v.event_id.as_bytes());
    out.push(venue_rank(v.venue));
    symbol(out, &v.symbol);
    string(out, &v.venue_symbol);
    opt_i64(out, v.source_ts_us);
    out.push(precision(v.source_ts_precision));
    i64be(out, v.local_recv_ts_us);
}
fn decision_payload(out: &mut Vec<u8>, v: &DecisionEvent) {
    out.extend_from_slice(v.event_id.as_bytes());
    i64be(out, v.local_recv_ts_us);
    symbol(out, &v.symbol);
    out.push(venue_rank(v.long_venue));
    out.push(venue_rank(v.short_venue));
    decimal(out, v.requested_base);
    i64be(out, v.holding_end_ts_us);
    cost_model(out, &v.cost_model);
    decimal(out, v.minimum_net_bps);
    array_len(out, v.capacity_evidence.len());
    for item in &v.capacity_evidence {
        capacity(out, item);
    }
}
fn cost_model(out: &mut Vec<u8>, v: &CostModel) {
    venue_cost(out, &v.binance);
    venue_cost(out, &v.bybit);
    decimal(out, v.basis_risk_buffer_bps);
    decimal(out, v.funding_error_buffer_bps);
    decimal(out, v.leg_risk_buffer_bps);
}
fn venue_cost(out: &mut Vec<u8>, v: &VenueCostModel) {
    fee(
        out,
        v.entry_fee.rate,
        v.entry_fee.source,
        v.entry_fee.liquidity,
    );
    fee(
        out,
        v.exit_fee.rate,
        v.exit_fee.source,
        v.exit_fee.liquidity,
    );
    decimal(out, v.entry_slippage_bps);
    decimal(out, v.exit_slippage_bps);
    decimal(out, v.entry_book_impact_bps);
    decimal(out, v.exit_book_impact_bps);
}
fn fee(out: &mut Vec<u8>, rate: ExactDecimal, source: FeeSource, liquidity: FeeLiquidity) {
    decimal(out, rate);
    out.push(match source {
        FeeSource::AuthenticatedCommission => 0,
        FeeSource::ExplicitConfig => 1,
    });
    out.push(match liquidity {
        FeeLiquidity::Maker => 0,
        FeeLiquidity::Taker => 1,
    });
}
fn capacity(out: &mut Vec<u8>, v: &CapacityEvidence) {
    out.push(match v.source {
        CapacitySource::ConfiguredResearchLimit => 0,
        CapacitySource::InstrumentRule => 1,
        CapacitySource::BookDepth => 2,
        CapacitySource::RiskLimit => 3,
        CapacitySource::AuthenticatedMargin => 4,
    });
    opt_adapter(out, v.venue);
    out.push(match v.leg {
        CapacityLeg::Long => 0,
        CapacityLeg::Short => 1,
        CapacityLeg::Pair => 2,
    });
    match &v.symbol {
        Some(s) => {
            out.push(1);
            symbol(out, s);
        }
        None => out.push(0),
    };
    opt_decimal(out, v.capacity_base);
    opt_decimal(out, v.capacity_quote);
    opt_uuid(out, v.source_event_id);
    opt_i64(out, v.source_ts_us);
    match &v.validity {
        CapacityEvidenceValidity::Available => out.push(0),
        CapacityEvidenceValidity::Unavailable { reason } => {
            out.push(1);
            string(out, reason);
        }
        CapacityEvidenceValidity::Stale { age_us, limit_us } => {
            out.push(2);
            i64be(out, *age_us);
            i64be(out, *limit_us);
        }
    }
}
fn funding_common(
    out: &mut Vec<u8>,
    rate: i128,
    kind: funding_core::public::FundingRateKind,
    basis: funding_core::public::FundingBasis,
    interval: u32,
    provenance: funding_core::public::FundingIntervalProvenance,
) {
    i128be(out, rate);
    out.push(match kind {
        funding_core::public::FundingRateKind::IndicativeNext => 0,
        funding_core::public::FundingRateKind::SettledActual => 1,
    });
    out.push(match basis {
        funding_core::public::FundingBasis::MarkNotional => 0,
    });
    u32be(out, interval);
    out.push(interval_provenance(provenance));
}
fn interval_provenance(v: funding_core::public::FundingIntervalProvenance) -> u8 {
    match v {
        funding_core::public::FundingIntervalProvenance::VenuePayload => 0,
        funding_core::public::FundingIntervalProvenance::InstrumentRule => 1,
        funding_core::public::FundingIntervalProvenance::AssumedVenueDefault => 2,
    }
}
fn trader_kind(v: TraderMetricKind) -> u8 {
    match v {
        TraderMetricKind::BinanceTopAccountRatio => 0,
        TraderMetricKind::BinanceTopPositionRatio => 1,
        TraderMetricKind::BybitLongShortRatio => 2,
    }
}
fn precision(v: md_core::model::TimestampPrecision) -> u8 {
    match v {
        md_core::model::TimestampPrecision::Microsecond => 0,
        md_core::model::TimestampPrecision::Millisecond => 1,
        md_core::model::TimestampPrecision::Unavailable => 2,
    }
}
fn symbol(out: &mut Vec<u8>, value: &CanonicalSymbol) {
    string(out, &value.base);
    string(out, &value.quote);
}
fn string(out: &mut Vec<u8>, value: &str) {
    put(out, value.as_bytes());
}
fn decimal(out: &mut Vec<u8>, value: ExactDecimal) {
    i128be(out, value.scaled());
}
fn opt_decimal(out: &mut Vec<u8>, value: Option<ExactDecimal>) {
    match value {
        Some(v) => {
            out.push(1);
            decimal(out, v);
        }
        None => out.push(0),
    }
}
fn opt_adapter(out: &mut Vec<u8>, value: Option<AdapterId>) {
    match value {
        Some(v) => {
            out.push(1);
            out.push(venue_rank(v));
        }
        None => out.push(0),
    }
}
fn opt_uuid(out: &mut Vec<u8>, value: Option<Uuid>) {
    match value {
        Some(v) => {
            out.push(1);
            out.extend_from_slice(v.as_bytes());
        }
        None => out.push(0),
    }
}
fn opt_i128(out: &mut Vec<u8>, value: Option<i128>) {
    match value {
        Some(v) => {
            out.push(1);
            i128be(out, v);
        }
        None => out.push(0),
    }
}
fn opt_i64(out: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(v) => {
            out.push(1);
            i64be(out, v);
        }
        None => out.push(0),
    }
}
fn opt_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(v) => {
            out.push(1);
            u64be(out, v);
        }
        None => out.push(0),
    }
}
fn array_len(out: &mut Vec<u8>, value: usize) {
    u64be(out, value as u64);
}
fn i128be(out: &mut Vec<u8>, value: i128) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn i64be(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn u64be(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn u32be(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn u16be(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}
fn put(out: &mut Vec<u8>, value: &[u8]) {
    u64be(out, value.len() as u64);
    out.extend_from_slice(value);
}
fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(HEX[(byte >> 4) as usize] as char);
        value.push(HEX[(byte & 0xf) as usize] as char);
    }
    value
}
