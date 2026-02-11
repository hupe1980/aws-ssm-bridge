//! AWS SSM client wrapper
//!
//! Note: Direct SSM API integration currently handled by `SessionManager`
//! in `session.rs`. This module is reserved for future use cases requiring
//! a standalone client (e.g., listing sessions, describing instances).

use aws_sdk_ssm::Client;
use std::sync::Arc;

/// Wrapper around AWS SSM client with additional utilities.
///
/// Currently unused — `SessionManager` handles all SSM API calls directly.
/// Retained for future expansion (instance listing, session enumeration, etc.).
#[allow(dead_code)]
pub(crate) struct SsmClient {
    client: Arc<Client>,
}

#[allow(dead_code)]
impl SsmClient {
    /// Create a new SSM client from AWS config.
    pub fn new(config: &aws_config::SdkConfig) -> Self {
        Self {
            client: Arc::new(Client::new(config)),
        }
    }

    /// Get reference to the underlying AWS SDK client.
    pub fn inner(&self) -> &Client {
        &self.client
    }
}

#[cfg(test)]
mod tests {
    // Note: These tests would require AWS credentials or mocking
}
