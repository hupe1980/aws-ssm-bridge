//! Error recovery and retry logic with exponential backoff

use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::errors::{Error, Result};

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,

    /// Initial backoff duration
    pub initial_backoff: Duration,

    /// Maximum backoff duration
    pub max_backoff: Duration,

    /// Backoff multiplier (typically 2.0 for exponential)
    pub multiplier: f64,

    /// Add jitter to prevent thundering herd
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: true,
        }
    }
}

impl RetryConfig {
    /// Create config with no retries
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            ..Default::default()
        }
    }

    /// Create config for aggressive retries
    pub fn aggressive() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(10),
            multiplier: 1.5,
            jitter: true,
        }
    }
}

/// Retry strategy implementation
pub struct RetryStrategy {
    config: RetryConfig,
    attempt: u32,
}

impl RetryStrategy {
    /// Create a new retry strategy
    pub fn new(config: RetryConfig) -> Self {
        Self { config, attempt: 0 }
    }

    /// Check if we should retry
    pub fn should_retry(&self, error: &Error) -> bool {
        if self.attempt >= self.config.max_attempts {
            return false;
        }

        // Only retry retriable errors
        error.is_retriable()
    }

    /// Calculate backoff duration for current attempt
    pub fn backoff_duration(&self) -> Duration {
        if self.attempt == 0 {
            return Duration::from_secs(0);
        }

        let base = self.config.initial_backoff.as_millis() as f64;
        let multiplier = self.config.multiplier.powi((self.attempt - 1) as i32);
        let mut duration_ms = base * multiplier;

        // Apply max backoff limit
        let max_ms = self.config.max_backoff.as_millis() as f64;
        duration_ms = duration_ms.min(max_ms);

        // Add jitter if configured (±25%)
        if self.config.jitter {
            let jitter_factor = 1.0 + (rand::random::<f64>() - 0.5) * 0.5;
            duration_ms *= jitter_factor;
        }

        Duration::from_millis(duration_ms as u64)
    }

    /// Wait for backoff duration
    pub async fn wait(&mut self) {
        let duration = self.backoff_duration();
        self.attempt += 1;

        if duration > Duration::from_secs(0) {
            debug!(
                attempt = self.attempt,
                backoff_ms = duration.as_millis(),
                "Backing off before retry"
            );
            sleep(duration).await;
        }
    }

    /// Reset retry state
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Get current attempt number
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

/// Execute a function with retry logic
pub async fn retry_with_backoff<F, Fut, T>(
    config: RetryConfig,
    mut operation: F,
    operation_name: &str,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut strategy = RetryStrategy::new(config);

    loop {
        match operation().await {
            Ok(result) => {
                if strategy.attempt() > 0 {
                    debug!(
                        operation = operation_name,
                        attempt = strategy.attempt(),
                        "Operation succeeded after retry"
                    );
                }
                return Ok(result);
            }
            Err(error) => {
                if strategy.should_retry(&error) {
                    warn!(
                        operation = operation_name,
                        attempt = strategy.attempt(),
                        error = ?error,
                        "Operation failed, will retry"
                    );
                    strategy.wait().await;
                } else {
                    warn!(
                        operation = operation_name,
                        attempt = strategy.attempt(),
                        error = ?error,
                        "Operation failed, no more retries"
                    );
                    return Err(error);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_calculation() {
        let config = RetryConfig {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            multiplier: 2.0,
            jitter: false,
        };

        let mut strategy = RetryStrategy::new(config);

        assert_eq!(strategy.backoff_duration(), Duration::from_secs(0));
        strategy.attempt += 1;
        assert_eq!(strategy.backoff_duration(), Duration::from_millis(100));
        strategy.attempt += 1;
        assert_eq!(strategy.backoff_duration(), Duration::from_millis(200));
        strategy.attempt += 1;
        assert_eq!(strategy.backoff_duration(), Duration::from_millis(400));
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.initial_backoff, Duration::from_millis(100));
        assert_eq!(config.max_backoff, Duration::from_secs(30));
        assert_eq!(config.multiplier, 2.0);
        assert!(config.jitter);
    }

    #[test]
    fn test_retry_config_none() {
        let config = RetryConfig::none();
        assert_eq!(config.max_attempts, 1);
    }

    #[test]
    fn test_retry_config_aggressive() {
        let config = RetryConfig::aggressive();
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.initial_backoff, Duration::from_millis(50));
        assert_eq!(config.multiplier, 1.5);
    }

    #[test]
    fn test_retry_strategy_should_retry() {
        let config = RetryConfig {
            max_attempts: 3,
            ..Default::default()
        };
        let mut strategy = RetryStrategy::new(config);

        // Retriable error should be retried
        assert!(strategy.should_retry(&Error::Timeout));

        // Non-retriable error should not be retried
        assert!(!strategy.should_retry(&Error::Cancelled));

        // After max attempts, no retries
        strategy.attempt = 3;
        assert!(!strategy.should_retry(&Error::Timeout));
    }

    #[test]
    fn test_retry_strategy_reset() {
        let config = RetryConfig::default();
        let mut strategy = RetryStrategy::new(config);

        strategy.attempt = 5;
        assert_eq!(strategy.attempt(), 5);

        strategy.reset();
        assert_eq!(strategy.attempt(), 0);
    }

    #[test]
    fn test_backoff_respects_max() {
        let config = RetryConfig {
            max_attempts: 10,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(5),
            multiplier: 10.0, // Aggressive multiplier
            jitter: false,
        };

        let mut strategy = RetryStrategy::new(config);
        strategy.attempt = 5; // Would be 10000 seconds without cap

        let duration = strategy.backoff_duration();
        assert!(duration <= Duration::from_secs(5));
    }

}
