//! AWS SSM client wrapper

use aws_sdk_ssm::Client;
use std::sync::Arc;

/// Wrapper around AWS SSM client with additional utilities
#[allow(dead_code)] // Reserved for future direct SSM API integration
pub struct SsmClient {
    /// Internal AWS SDK client
    client: Arc<Client>,
}

#[allow(dead_code)] // Reserved for future direct SSM API integration
impl SsmClient {
    /// Create a new SSM client from AWS config
    pub fn new(config: &aws_config::SdkConfig) -> Self {
        Self {
            client: Arc::new(Client::new(config)),
        }
    }

    /// Create from default AWS config
    pub async fn from_env() -> Self {
        let config = aws_config::load_from_env().await;
        Self::new(&config)
    }

    /// Get reference to the underlying AWS SDK client
    pub fn inner(&self) -> &Client {
        &self.client
    }

    /// Get Arc clone of the client (for sharing across threads)
    pub fn clone_client(&self) -> Arc<Client> {
        Arc::clone(&self.client)
    }
}

impl From<Client> for SsmClient {
    fn from(client: Client) -> Self {
        Self {
            client: Arc::new(client),
        }
    }
}

impl From<Arc<Client>> for SsmClient {
    fn from(client: Arc<Client>) -> Self {
        Self { client }
    }
}

#[cfg(test)]
mod tests {
    // Note: These tests would require AWS credentials or mocking
}
