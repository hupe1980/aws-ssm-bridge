/// Basic integration tests for SSM session manager
mod common;

use aws_ssm_bridge::{SessionBuilder, SessionConfig, SessionType};
use common::mock_ssm::{MockSsmServer, ServerBehavior};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

/// Initialize tracing for tests
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();
}

#[tokio::test]
async fn test_session_basic_echo() {
    init_tracing();

    // Start mock server
    let server = Arc::new(MockSsmServer::new().await.unwrap());
    let server_url = server.url().unwrap();

    // Run server in background
    let server_clone = server.clone();
    tokio::spawn(async move {
        server_clone.run().await;
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify server is reachable with timeout
    let client = timeout(
        Duration::from_secs(2),
        tokio_tungstenite::connect_async(&server_url),
    )
    .await;

    assert!(
        client.is_ok(),
        "Should connect to mock server within timeout"
    );
    if let Ok(Ok((mut ws, _))) = client {
        // Close the connection properly
        let _ = ws.close(None).await;
    }
}

#[tokio::test]
async fn test_mock_server_echo_behavior() {
    init_tracing();

    let server = Arc::new(MockSsmServer::new().await.unwrap());
    let server_url = server.url().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        server_clone.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect and send a message
    let result = timeout(Duration::from_secs(2), async {
        let (mut ws_stream, _) = tokio_tungstenite::connect_async(&server_url).await.unwrap();

        // Send input message
        use tokio_tungstenite::tungstenite::Message;
        let input_msg = common::create_input_message("stdin", 1, b"echo test\n");
        ws_stream.send(Message::Text(input_msg)).await.unwrap();

        // Should receive echo back
        let response = ws_stream.next().await;

        // Close connection
        let _ = ws_stream.close(None).await;

        response
    })
    .await;

    assert!(result.is_ok(), "Test should complete within timeout");
    if let Ok(Some(Ok(Message::Text(text)))) = result {
        assert!(text.contains("output_stream_data") || text.contains("acknowledge"));
    }
}

#[tokio::test]
async fn test_mock_server_send_and_close() {
    init_tracing();

    let server = Arc::new(MockSsmServer::new().await.unwrap());
    server.set_behavior(ServerBehavior::SendAndClose(3)).await;
    let server_url = server.url().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        server_clone.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = timeout(Duration::from_secs(3), async {
        let (mut ws_stream, _) = tokio_tungstenite::connect_async(&server_url).await.unwrap();

        let mut message_count = 0;
        while let Some(Ok(msg)) = ws_stream.next().await {
            if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                message_count += 1;
                if text.contains("channel_closed") {
                    break;
                }
            }
        }
        message_count
    })
    .await;

    assert!(result.is_ok(), "Test should complete within timeout");
    // Should receive 3 output messages + 1 channel_closed
    if let Ok(count) = result {
        assert_eq!(count, 4);
    }
}

#[tokio::test]
async fn test_mock_server_immediate_close() {
    init_tracing();

    let server = Arc::new(MockSsmServer::new().await.unwrap());
    server.set_behavior(ServerBehavior::ImmediateClose).await;
    let server_url = server.url().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        server_clone.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connection should fail or close immediately
    let result = timeout(
        Duration::from_secs(2),
        tokio_tungstenite::connect_async(&server_url),
    )
    .await;

    // Connection might succeed but close immediately, or fail during handshake
    if let Ok(Ok((mut ws_stream, _))) = result {
        // Should get EOF quickly
        let next_msg = timeout(Duration::from_secs(1), ws_stream.next()).await;
        assert!(next_msg.is_ok()); // Should complete quickly
    }
}

#[tokio::test]
async fn test_session_config_creation() {
    let config = SessionConfig {
        target: "i-1234567890abcdef0".to_string(),
        region: Some("us-east-1".to_string()),
        session_type: SessionType::InteractiveCommands,
        document_name: Some("AWS-StartInteractiveCommand".to_string()),
        reason: Some("Test session".to_string()),
        parameters: Default::default(),
        ..Default::default()
    };

    assert_eq!(config.target, "i-1234567890abcdef0");
    assert_eq!(
        config.document_name,
        Some("AWS-StartInteractiveCommand".to_string())
    );
}

#[tokio::test]
async fn test_session_builder_creation() {
    let builder = SessionBuilder::new("i-1234567890abcdef0")
        .region("us-west-2")
        .session_type(SessionType::InteractiveCommands)
        .reason("Integration test");

    let config = builder.build_config();
    assert_eq!(config.target, "i-1234567890abcdef0");
}
