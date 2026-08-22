//! Deterministic, read-only funding-arbitrage feature calculators.
//!
//! Phase 2B adds calculators to this crate incrementally. Trading and
//! authenticated venue access deliberately remain outside this boundary.

pub use funding_core::{calendar, feature, opportunity};

pub mod book;
pub mod flow;
