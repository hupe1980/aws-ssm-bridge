/// Basic integration tests for SSM session manager
mod common;

use aws_ssm_bridge::{SessionBuilder, SessionConfig, SessionType};
use common::mock_ssm::{MockSsmServer, ServerBehavior};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
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
        ws_stream
            .send(Message::Text(input_msg.into()))
            .await
            .unwrap();

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

/// Verify multiple concurrent producers can send through a single WebSocket
/// connection without deadlock or message loss.  This mirrors the library's
/// lock-free writer-channel architecture where heartbeat, ACK, retransmit,
/// and command tasks all funnel through one mpsc channel.
#[tokio::test]
async fn test_concurrent_writers_no_contention() {
    init_tracing();

    let server = Arc::new(MockSsmServer::new().await.unwrap());
    let server_url = server.url().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        server_clone.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = timeout(Duration::from_secs(5), async {
        let (ws_stream, _) = tokio_tungstenite::connect_async(&server_url).await.unwrap();
        let (write, read) = ws_stream.split();

        // Wrap the writer in Arc<Mutex> to share across tasks — this is the
        // pattern the library *replaced*. We use it here to prove the mock
        // server can handle the resulting burst of messages.
        let write = Arc::new(tokio::sync::Mutex::new(write));

        const NUM_PRODUCERS: usize = 4;
        const MSGS_PER_PRODUCER: usize = 25;
        let total = NUM_PRODUCERS * MSGS_PER_PRODUCER;

        // Barrier ensures all producers start sending simultaneously
        let barrier = Arc::new(Barrier::new(NUM_PRODUCERS));

        let mut handles = Vec::new();
        for producer_id in 0..NUM_PRODUCERS {
            let w = Arc::clone(&write);
            let b = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                b.wait().await; // synchronise start
                for i in 0..MSGS_PER_PRODUCER {
                    let seq = (producer_id * MSGS_PER_PRODUCER + i) as i64;
                    let payload = format!("p{}m{}", producer_id, i);
                    let msg = common::create_input_message("stdin", seq, payload.as_bytes());
                    w.lock()
                        .await
                        .send(Message::Text(msg.into()))
                        .await
                        .unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // Collect echo responses (each input yields an output + an ack = 2 msgs)
        let expected_responses = total * 2;
        let mut received = 0usize;
        let mut read = read;
        while received < expected_responses {
            match timeout(Duration::from_secs(3), read.next()).await {
                Ok(Some(Ok(Message::Text(_)))) => received += 1,
                Ok(Some(Ok(_))) => {} // pong, binary, etc.
                _ => break,
            }
        }

        // Close
        {
            let mut w = write.lock().await;
            let _ = w.close().await;
        }

        received
    })
    .await;

    assert!(result.is_ok(), "Test should complete within timeout");
    let received = result.unwrap();
    let expected = 4 * 25 * 2; // output + ack per input
    assert_eq!(
        received, expected,
        "Expected {} responses, got {} — messages lost under contention",
        expected, received
    );
}

/// Verify delayed echo does not block or drop concurrent messages.
#[tokio::test]
async fn test_concurrent_writers_with_delay() {
    init_tracing();

    let server = Arc::new(MockSsmServer::new().await.unwrap());
    server.set_behavior(ServerBehavior::Delayed(10)).await;
    let server_url = server.url().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        server_clone.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = timeout(Duration::from_secs(10), async {
        let (ws_stream, _) = tokio_tungstenite::connect_async(&server_url).await.unwrap();
        let (write, read) = ws_stream.split();
        let write = Arc::new(tokio::sync::Mutex::new(write));

        const PRODUCERS: usize = 3;
        const MSGS: usize = 10;
        let barrier = Arc::new(Barrier::new(PRODUCERS));

        let mut handles = Vec::new();
        for pid in 0..PRODUCERS {
            let w = Arc::clone(&write);
            let b = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                b.wait().await;
                for i in 0..MSGS {
                    let seq = (pid * MSGS + i) as i64;
                    let msg = common::create_input_message(
                        "stdin",
                        seq,
                        format!("d{pid}m{i}").as_bytes(),
                    );
                    w.lock()
                        .await
                        .send(Message::Text(msg.into()))
                        .await
                        .unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let expected = PRODUCERS * MSGS * 2;
        let mut received = 0usize;
        let mut read = read;
        while received < expected {
            match timeout(Duration::from_secs(5), read.next()).await {
                Ok(Some(Ok(Message::Text(_)))) => received += 1,
                Ok(Some(Ok(_))) => {}
                _ => break,
            }
        }

        {
            let mut w = write.lock().await;
            let _ = w.close().await;
        }

        received
    })
    .await;

    assert!(result.is_ok(), "Test should complete within timeout");
    let received = result.unwrap();
    assert_eq!(received, 3 * 10 * 2, "All delayed responses should arrive");
}
