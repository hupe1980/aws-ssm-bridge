//! Token bucket rate limiting for DoS protection.
//!
//! Provides configurable rate limiting to protect against:
//! - Message flooding attacks
//! - Resource exhaustion
//! - Abusive client behavior
//!
//! # Example
//!
//! ```rust
//! use aws_ssm_bridge::rate_limit::{RateLimiter, RateLimitConfig};
//! use std::time::Duration;
//!
//! // Allow 100 messages per second with burst of 50
//! let limiter = RateLimiter::new(RateLimitConfig {
//!     tokens_per_second: 100.0,
//!     bucket_size: 50,
//!     ..Default::default()
//! });
//!
//! // Check if action is allowed
//! if limiter.try_acquire() {
//!     // Process message
//! } else {
//!     // Rate limited - reject or queue
//! }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Configuration for the rate limiter.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Tokens added per second (sustained rate).
    pub tokens_per_second: f64,

    /// Maximum tokens in the bucket (burst capacity).
    pub bucket_size: u32,

    /// Initial tokens (defaults to bucket_size).
    pub initial_tokens: Option<u32>,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            tokens_per_second: 1000.0, // 1000 msg/sec default
            bucket_size: 100,          // Allow bursts of 100
            initial_tokens: None,      // Start full
        }
    }
}

impl RateLimitConfig {
    /// Create a permissive rate limiter (high throughput).
    pub fn permissive() -> Self {
        Self {
            tokens_per_second: 10_000.0,
            bucket_size: 1000,
            initial_tokens: None,
        }
    }

    /// Create a strict rate limiter (low throughput).
    pub fn strict() -> Self {
        Self {
            tokens_per_second: 100.0,
            bucket_size: 10,
            initial_tokens: None,
        }
    }

    /// Create an unlimited rate limiter (no limiting).
    pub fn unlimited() -> Self {
        Self {
            tokens_per_second: 1_000_000_000.0, // 1 billion/sec (effectively unlimited)
            bucket_size: 1_000_000,             // 1M burst
            initial_tokens: None,
        }
    }
}

/// Token bucket rate limiter.
///
/// Thread-safe, lock-free implementation using atomic operations.
/// Suitable for high-concurrency scenarios.
pub struct RateLimiter {
    /// Current tokens (scaled by 1000 for precision).
    tokens: AtomicU64,

    /// Last refill time (nanoseconds since creation).
    last_refill: AtomicU64,

    /// Tokens per nanosecond (scaled).
    tokens_per_ns: f64,

    /// Maximum tokens (scaled).
    max_tokens: u64,

    /// Creation instant for time tracking.
    start: Instant,
}

impl RateLimiter {
    /// Scale factor for token precision.
    const SCALE: u64 = 1000;

    /// Create a new rate limiter with the given configuration.
    pub fn new(config: RateLimitConfig) -> Self {
        let max_tokens = (config.bucket_size as u64) * Self::SCALE;
        let initial = config
            .initial_tokens
            .map(|t| (t as u64) * Self::SCALE)
            .unwrap_or(max_tokens);

        Self {
            tokens: AtomicU64::new(initial),
            last_refill: AtomicU64::new(0),
            tokens_per_ns: config.tokens_per_second * (Self::SCALE as f64) / 1_000_000_000.0,
            max_tokens,
            start: Instant::now(),
        }
    }

    /// Try to acquire a token. Returns true if successful.
    ///
    /// This is a non-blocking operation.
    #[inline]
    pub fn try_acquire(&self) -> bool {
        self.try_acquire_n(1)
    }

    /// Try to acquire N tokens. Returns true if successful.
    #[inline]
    pub fn try_acquire_n(&self, n: u32) -> bool {
        let cost = (n as u64) * Self::SCALE;
        self.refill();

        // Try to subtract tokens atomically
        let mut current = self.tokens.load(Ordering::Relaxed);
        loop {
            if current < cost {
                return false;
            }

            match self.tokens.compare_exchange_weak(
                current,
                current - cost,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Get the current number of available tokens.
    pub fn available(&self) -> u32 {
        self.refill();
        (self.tokens.load(Ordering::Relaxed) / Self::SCALE) as u32
    }

    /// Check if the bucket is empty.
    pub fn is_empty(&self) -> bool {
        self.available() == 0
    }

    /// Refill tokens based on elapsed time.
    fn refill(&self) {
        let now_ns = self.start.elapsed().as_nanos() as u64;
        let last = self.last_refill.load(Ordering::Relaxed);

        if now_ns <= last {
            return;
        }

        // Calculate tokens to add
        let elapsed_ns = now_ns - last;
        let new_tokens = (elapsed_ns as f64 * self.tokens_per_ns) as u64;

        if new_tokens == 0 {
            return;
        }

        // Try to update last_refill
        if self
            .last_refill
            .compare_exchange(last, now_ns, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            // Add tokens up to max
            let mut current = self.tokens.load(Ordering::Relaxed);
            loop {
                let new_total = (current + new_tokens).min(self.max_tokens);
                if new_total == current {
                    break;
                }

                match self.tokens.compare_exchange_weak(
                    current,
                    new_total,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }
        }
    }

    /// Wait until a token is available, then acquire it.
    ///
    /// Returns the duration waited.
    pub async fn acquire(&self) -> Duration {
        self.acquire_n(1).await
    }

    /// Wait until N tokens are available, then acquire them.
    pub async fn acquire_n(&self, n: u32) -> Duration {
        let start = Instant::now();

        while !self.try_acquire_n(n) {
            // Calculate wait time based on token deficit
            let deficit = (n as u64) * Self::SCALE;
            let wait_ns = (deficit as f64 / self.tokens_per_ns) as u64;
            let wait = Duration::from_nanos(wait_ns.max(1_000_000)); // Min 1ms

            tokio::time::sleep(wait.min(Duration::from_millis(100))).await;
        }

        start.elapsed()
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(RateLimitConfig::default())
    }
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("available", &self.available())
            .field("max_tokens", &(self.max_tokens / Self::SCALE))
            .finish()
    }
}

/// Rate limit result for detailed feedback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitResult {
    /// Request allowed.
    Allowed,
    /// Request denied due to rate limit.
    Denied {
        /// Estimated wait time until tokens available.
        retry_after: Duration,
    },
}

impl RateLimiter {
    /// Check rate limit with detailed result.
    pub fn check(&self) -> RateLimitResult {
        if self.try_acquire() {
            RateLimitResult::Allowed
        } else {
            let deficit = Self::SCALE; // 1 token
            let wait_ns = (deficit as f64 / self.tokens_per_ns) as u64;
            RateLimitResult::Denied {
                retry_after: Duration::from_nanos(wait_ns),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_basic() {
        let limiter = RateLimiter::new(RateLimitConfig {
            tokens_per_second: 10.0,
            bucket_size: 5,
            initial_tokens: Some(5),
        });

        // Should allow 5 requests immediately (bucket is full)
        for _ in 0..5 {
            assert!(limiter.try_acquire());
        }

        // 6th request should be denied
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn test_rate_limiter_unlimited() {
        let limiter = RateLimiter::new(RateLimitConfig::unlimited());

        // Should allow many requests
        for _ in 0..10000 {
            assert!(limiter.try_acquire());
        }
    }

    #[test]
    fn test_rate_limiter_available() {
        let limiter = RateLimiter::new(RateLimitConfig {
            tokens_per_second: 100.0,
            bucket_size: 10,
            initial_tokens: Some(10),
        });

        assert_eq!(limiter.available(), 10);

        limiter.try_acquire_n(3);
        assert_eq!(limiter.available(), 7);
    }

    #[tokio::test]
    async fn test_rate_limiter_refill() {
        let limiter = RateLimiter::new(RateLimitConfig {
            tokens_per_second: 1000.0, // 1 token per ms
            bucket_size: 10,
            initial_tokens: Some(0),
        });

        assert!(!limiter.try_acquire());

        // Wait for refill
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Should have some tokens now
        assert!(limiter.try_acquire());
    }

    #[test]
    fn test_rate_limit_result() {
        let limiter = RateLimiter::new(RateLimitConfig {
            tokens_per_second: 10.0,
            bucket_size: 1,
            initial_tokens: Some(1),
        });

        assert_eq!(limiter.check(), RateLimitResult::Allowed);

        match limiter.check() {
            RateLimitResult::Denied { retry_after } => {
                assert!(retry_after > Duration::ZERO);
            }
            _ => panic!("Expected Denied"),
        }
    }
}

// =============================================================================
// Property-Based Tests
// =============================================================================

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Property: Bucket never exceeds maximum size
        #[test]
        fn bucket_never_exceeds_max(
            tokens_per_sec in 1.0f64..10000.0,
            bucket_size in 1u32..1000,
            initial in 0u32..1000,
        ) {
            let initial = initial.min(bucket_size);
            let limiter = RateLimiter::new(RateLimitConfig {
                tokens_per_second: tokens_per_sec,
                bucket_size,
                initial_tokens: Some(initial),
            });

            // Available should never exceed bucket size
            prop_assert!(limiter.available() <= bucket_size);
        }

        /// Property: Acquiring N tokens reduces available by N (when possible)
        #[test]
        fn acquire_reduces_available(
            bucket_size in 10u32..100,
            acquire_count in 1u32..10,
        ) {
            let limiter = RateLimiter::new(RateLimitConfig {
                tokens_per_second: 1000.0,
                bucket_size,
                initial_tokens: Some(bucket_size),
            });

            let before = limiter.available();
            if limiter.try_acquire_n(acquire_count) {
                let after = limiter.available();
                prop_assert_eq!(after, before - acquire_count);
            }
        }

        /// Property: Unlimited limiter always allows requests
        #[test]
        fn unlimited_always_allows(count in 1u32..10000) {
            let limiter = RateLimiter::new(RateLimitConfig::unlimited());

            for _ in 0..count {
                prop_assert!(limiter.try_acquire());
            }
        }

        /// Property: Strict limiter rate < permissive limiter rate
        #[test]
        fn strict_less_than_permissive(_dummy: bool) {
            let strict = RateLimitConfig::strict();
            let permissive = RateLimitConfig::permissive();

            prop_assert!(strict.tokens_per_second < permissive.tokens_per_second);
            prop_assert!(strict.bucket_size < permissive.bucket_size);
        }
    }
}
