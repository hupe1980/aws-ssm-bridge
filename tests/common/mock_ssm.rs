//! Mock SSM Session Manager WebSocket server for testing

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};
use tracing::{debug, error, info};

/// Mock SSM server behavior modes
#[derive(Debug, Clone, Copy)]
pub enum ServerBehavior {
    /// Normal operation - echo input to output
    Normal,
    /// Send a specific number of messages then close
    SendAndClose(usize),
    /// Immediately close connection after handshake
    ImmediateClose,
    /// Drop connection without proper close
    AbruptDisconnect,
    /// Delay responses by specified milliseconds
    Delayed(u64),
}

/// Mock SSM Session Manager server
pub struct MockSsmServer {
    listener: TcpListener,
    behavior: Arc<Mutex<ServerBehavior>>,
    received_messages: Arc<Mutex<Vec<String>>>,
}

impl MockSsmServer {
    /// Create a new mock server on a random port
    pub async fn new() -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        Ok(Self {
            listener,
            behavior: Arc::new(Mutex::new(ServerBehavior::Normal)),
            received_messages: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Get the server's listening address
    pub fn addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Get the WebSocket URL
    pub fn url(&self) -> std::io::Result<String> {
        let addr = self.addr()?;
        Ok(format!("ws://{}", addr))
    }

    /// Set server behavior mode
    pub async fn set_behavior(&self, behavior: ServerBehavior) {
        *self.behavior.lock().await = behavior;
    }

    /// Get all messages received by the server
    pub async fn received_messages(&self) -> Vec<String> {
        self.received_messages.lock().await.clone()
    }

    /// Run the mock server
    pub async fn run(self: Arc<Self>) {
        info!("Mock SSM server listening on {}", self.addr().unwrap());

        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    debug!("New connection from {}", addr);
                    let server = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = server.handle_connection(stream).await {
                            error!("Connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                    break;
                }
            }
        }
    }

    /// Handle a single WebSocket connection
    async fn handle_connection(&self, stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
        let ws_stream = accept_async(stream).await?;
        debug!("WebSocket handshake completed");

        let behavior = *self.behavior.lock().await;

        match behavior {
            ServerBehavior::ImmediateClose => {
                debug!("Immediate close mode - closing connection");
                return Ok(());
            }
            ServerBehavior::AbruptDisconnect => {
                debug!("Abrupt disconnect mode - dropping connection");
                return Ok(());
            }
            _ => {}
        }

        self.handle_messages(ws_stream, behavior).await
    }

    /// Handle WebSocket messages based on behavior mode
    async fn handle_messages(
        &self,
        mut ws_stream: WebSocketStream<TcpStream>,
        behavior: ServerBehavior,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut message_count = 0;

        if let ServerBehavior::SendAndClose(count) = behavior {
            // Send specified number of output messages
            for i in 0..count {
                let output = super::create_output_message(
                    "stdout",
                    i as i64,
                    format!("Message {}\n", i).as_bytes(),
                );
                ws_stream.send(Message::Text(output.into())).await?;
            }

            // Send channel closed
            let close_msg = super::create_channel_closed_message("stdout", "");
            ws_stream.send(Message::Text(close_msg.into())).await?;
            debug!("Connection closed after {} messages", count);
            ws_stream.close(None).await?;
            return Ok(());
        }

        // Normal message loop
        while let Some(msg) = ws_stream.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    debug!("Received text message: {}", text);
                    self.received_messages.lock().await.push(text.to_string());

                    // Parse message type
                    if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&text) {
                        let msg_type = json_val["messageType"].as_str().unwrap_or("");
                        let seq = json_val["sequenceNumber"].as_i64().unwrap_or(0);
                        let msg_id = json_val["messageId"].as_str().unwrap_or("");

                        match msg_type {
                            "input_stream_data" => {
                                // Echo input to output (simulate command execution)
                                if let Some(payload) = json_val["payload"].as_str() {
                                    if let Ok(decoded) =
                                        base64::engine::general_purpose::STANDARD.decode(payload)
                                    {
                                        // Apply delay if configured
                                        if let ServerBehavior::Delayed(ms) = behavior {
                                            tokio::time::sleep(tokio::time::Duration::from_millis(
                                                ms,
                                            ))
                                            .await;
                                        }

                                        // Send output
                                        let output =
                                            super::create_output_message("stdout", seq, &decoded);
                                        ws_stream.send(Message::Text(output.into())).await?;

                                        // Send acknowledge
                                        let ack = super::create_ack_message(msg_id, seq);
                                        ws_stream.send(Message::Text(ack.into())).await?;
                                    }
                                }
                            }
                            "pause_publication" => {
                                debug!("Received pause_publication");
                                // Send acknowledge
                                let ack = super::create_ack_message(msg_id, seq);
                                ws_stream.send(Message::Text(ack.into())).await?;
                            }
                            "start_publication" => {
                                debug!("Received start_publication");
                                // Send acknowledge
                                let ack = super::create_ack_message(msg_id, seq);
                                ws_stream.send(Message::Text(ack.into())).await?;
                            }
                            _ => {
                                debug!("Unknown message type: {}", msg_type);
                            }
                        }
                    }
                }
                Ok(Message::Binary(data)) => {
                    debug!("Received binary message: {} bytes", data.len());
                }
                Ok(Message::Ping(data)) => {
                    debug!("Received ping");
                    ws_stream.send(Message::Pong(data)).await?;
                }
                Ok(Message::Pong(_)) => {
                    debug!("Received pong");
                }
                Ok(Message::Close(_)) => {
                    debug!("Received close frame");
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }

            message_count += 1;
        }

        debug!("Connection closed after {} messages", message_count);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_server_creation() {
        let server = MockSsmServer::new().await.unwrap();
        let addr = server.addr().unwrap();
        assert!(addr.port() > 0);
    }

    #[tokio::test]
    async fn test_mock_server_url() {
        let server = MockSsmServer::new().await.unwrap();
        let url = server.url().unwrap();
        assert!(url.starts_with("ws://127.0.0.1:"));
    }
}
