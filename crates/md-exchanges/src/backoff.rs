use std::sync::Arc;

/// Capped exponential backoff whose non-negative jitter is injected by callers.
///
/// `healthy_ms` is the duration of the session which just ended. A session at
/// least as long as `healthy_reset_ms` starts a new retry sequence.
pub struct Backoff {
    initial_ms: u64,
    maximum_ms: u64,
    healthy_reset_ms: u64,
    attempt: u32,
    jitter: Arc<dyn Fn(u64) -> u64 + Send + Sync>,
}

impl Backoff {
    pub fn without_jitter(initial_ms: u64, maximum_ms: u64, healthy_reset_ms: u64) -> Self {
        Self::with_jitter(initial_ms, maximum_ms, healthy_reset_ms, |_| 0)
    }

    pub fn with_jitter(
        initial_ms: u64,
        maximum_ms: u64,
        healthy_reset_ms: u64,
        jitter: impl Fn(u64) -> u64 + Send + Sync + 'static,
    ) -> Self {
        assert!(initial_ms > 0, "initial backoff must be positive");
        assert!(
            maximum_ms >= initial_ms,
            "maximum must cover initial backoff"
        );
        Self {
            initial_ms,
            maximum_ms,
            healthy_reset_ms,
            attempt: 0,
            jitter: Arc::new(jitter),
        }
    }

    pub fn next_delay_ms(&mut self, healthy_ms: u64) -> u64 {
        if healthy_ms >= self.healthy_reset_ms {
            self.attempt = 0;
        }

        let multiplier = 1_u64.checked_shl(self.attempt.min(63)).unwrap_or(u64::MAX);
        let base = self
            .initial_ms
            .saturating_mul(multiplier)
            .min(self.maximum_ms);
        self.attempt = self.attempt.saturating_add(1);
        base.saturating_add((self.jitter)(base))
            .min(self.maximum_ms)
    }
}
