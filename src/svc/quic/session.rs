use claviron::runtime::AppState;
use claviron::runtime::gate::Permit;
use quinn::{ReadExactError, RecvStream, SendStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

use crate::svc::WELCOME_MESSAGE;
#[cfg(feature = "metrics")]
use crate::svc::metrics::CHAT_METRICS;
use crate::types::messages::{ClientMessage, ServerMessage, UnreadSummary};
use crate::{ChatClient, ChatServer, Error, Result, handle_client_message};

async fn read_frame(recv: &mut RecvStream) -> Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match recv.read_exact(&mut len_buf).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(e) => {
            return Err(Error::msg(format!(
                "read_frame failed with error {}",
                e
            )));
        }
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];

    recv.read_exact(&mut payload).await.map_err(|e| {
        Error::msg(format!("read_frame failed with error {}", e))
    })?;

    Ok(Some(payload))
}

async fn write_frame(
    send: &mut SendStream,
    data: &[u8],
) -> Result<()> {
    send.write_all(&(data.len() as u32).to_le_bytes()).await.map_err(|e| {
        Error::msg(format!("WriteFrame failed with error {}", e))
    })?;
    if !data.is_empty() {
        send.write_all(data)
            .await
            .map_err(|e| Error::msg(format!("write_frame err: {}", e)))?;
    }
    Ok(())
}

/// QUIC svc session: We handle Hello/LoginPass here, and forward everything else to handle_binary_message.
pub async fn handle_quic_chat_stream(
    mut send: SendStream,
    mut recv: RecvStream,
    state: AppState,
    cn: String,
    watcher: Permit,
) -> Result<()> {
    let chat_server = state
        .registry_ref()
        .resolve::<ChatServer>()
        .ok_or_else(|| Error::msg("ChatServer not resolved"))?;

    // 1) The first frame is mandatory: Hello {token} or LoginPass {..}
    // 1) First frame required: Hello {token} or LoginPass {..}
    let Some(first) = read_frame(&mut recv).await? else {
        return Ok(());
    };
    let hello = rmp_serde::from_slice::<WireMessage>(&first).map_err(|_| {
        Error::msg("invalid first frame (expect Hello/LoginPass)")
    })?;

    // 2) Authentication
    // 2) Auth
    let user = if let WireMessage::Hello = hello {
        match chat_server.user_by_identifier(&cn).await {
            Ok(user) => user,
            Err(e) => {
                warn!(cn = %cn, error = %e, "❌ Auth DB lookup failed; closing connection");
                return Err(Error::msg(e));
            }
        }
    } else {
        return Err(Error::msg("first frame must be Hello/LoginPass"));
    };

    let max_message_size = chat_server.config_snapshot().max_message_size;

    info!(
        username = %user.username(),
        "🔐 QUIC connection established"
    );

    // 3) Client registration and Welcome
    let (tx, mut srv_rx) = mpsc::channel::<ServerMessage>(max_message_size);

    let client = ChatClient::new(user.clone(), tx);
    let connection_id = client.connection_id();

    chat_server.connect_client(client.clone()).await;

    // Increment connection metrics
    #[cfg(feature = "metrics")]
    CHAT_METRICS.connect();

    warn!(
        "🔌 NEW QUIC connection - username: {}, connection_id: {}",
        user.username(),
        connection_id
    );

    // Send welcome message DIRECTLY to stream (before event loop starts)
    {
        let unread_summary = chat_server.get_unread_summary(user.id()).await.unwrap_or_else(|e| {
            warn!(username =  user.username(), error = %e, "❌ Failed to fetch unread counts");
            UnreadSummary { private_messages_count: 0, room_messages: vec![] }
        });

        let welcome_msg = ServerMessage::welcome(
            connection_id,
            user.uuid(),
            user.username_arc().clone(),
            user.name_arc().clone(),
            WELCOME_MESSAGE,
            unread_summary.clone(),
            chat_server.config_snapshot().history.clone(),
        );

        // Write Welcome directly to stream BEFORE event loop starts
        // IMPORTANT: Wrap in WireEnvelope {t: "App", d: ServerMessage}
        // Client's first read doesn't have auto-unwrap logic (only reader goroutine does)
        use serde::Serialize;
        #[derive(Serialize)]
        struct WireEnvelope {
            t: &'static str,
            d: ServerMessage,
        }
        let envelope = WireEnvelope { t: "App", d: welcome_msg };

        match rmp_serde::to_vec_named(&envelope) {
            Ok(bytes) => {
                if let Err(e) = write_frame(&mut send, &bytes).await {
                    warn!(username = user.username(), error = %e, "❌ Failed to send welcome frame");
                    return Err(e);
                }
                info!(
                    username = user.username(),
                    "✅ Welcome sent to stream (WireEnvelope)"
                );
            }
            Err(e) => {
                warn!(username = user.username(), error = %e, "❌ Failed to serialize welcome");
                return Err(Error::MsgEncode(e));
            }
        }
    }

    info!(
        username = %user.username(),
        connection_id = %connection_id,
        "🚀 Starting QUIC event loop"
    );

    // Main event loop - coordinated with tokio::select! to prevent race conditions
    // No application-level ping or timeout needed - QUIC transport handles keepalive
    let mut send_stream = send;
    let mut recv_stream = recv;

    loop {
        tokio::select! {
            // 1) Shutdown signal
            _ = watcher.wait_graceful_shutdown() => {
                warn!(
                    username = %user.username(),
                    "🔻 Shutdown signal received"
                );
                tokio::select! {
                    _ = watcher.wait_forced_shutdown() => {
                        warn!(username = %user.username(), "forced shutdown interrupted Close frame");
                    }
                    result = write_frame(&mut send_stream, CLOSE_WIRE) => {
                        if let Err(e) = result {
                            warn!(error = %e, "❌ Failed to send Close frame on shutdown");
                        }
                    }
                }
                // allow frame flush before closing
                tokio::task::yield_now().await;
                // or a short sleep if you want extra safety:
                // tokio::time::sleep(Duration::from_millis(50)).await;
                break;
            }

            // 2) Outbound message from channel
            Some(msg) = srv_rx.recv() => {
                match rmp_serde::to_vec_named(&msg) {
                    Ok(bytes) => {
                        debug!(message = ?msg, size = bytes.len(), "📤 Sending message to client");
                        if let Err(e) = write_frame(&mut send_stream, &bytes).await {
                            error!(error = %e, "❌ Failed to send message frame");
                            break;
                        }
                        // Increment sent message metrics
                        #[cfg(feature = "metrics")]
                        CHAT_METRICS.message_sent();
                    }
                    Err(e) => {
                        warn!(error = %e, "❌ Failed to serialize message");
                    }
                }
            }

            // 3) Inbound frame from QUIC stream
            result = read_frame(&mut recv_stream) => {
                match result {
                    Ok(Some(buf)) => {
                        let (should_continue, response) = handle_quic_frame(&chat_server, &client, buf).await;

                        // Send immediate response if needed (e.g., Pong)
                        if let Some(response_bytes) = response
                            && let Err(e) = write_frame(&mut send_stream, &response_bytes).await {
                                error!(error = %e, "❌ Failed to send response frame");
                                break;
                            }

                        if !should_continue {
                            break;
                        }
                    }
                    Ok(None) => {
                        info!(username = user.username(), "Client closed stream");
                        break;
                    }
                    Err(e) => {
                        error!(username = user.username(), error = %e, "❌ QUIC read error");
                        break;
                    }
                }
            }
        }
    }

    // Cleanup
    let _ = send_stream.finish();

    // Decrement connection metrics
    #[cfg(feature = "metrics")]
    CHAT_METRICS.disconnect();

    chat_server.disconnect_client(connection_id).await;

    info!(
        username = %user.username(),
        connection_id = %connection_id,
        "🏁 QUIC connection closed gracefully"
    );

    Ok(())
}

/// Handle individual QUIC frames (WireMessage protocol)
/// Returns (should_continue, optional_response_bytes)
async fn handle_quic_frame(
    server: &ChatServer,
    client: &ChatClient,
    buf: Vec<u8>,
) -> (bool, Option<Vec<u8>>) {
    let wire: WireMessage = match rmp_serde::from_slice(&buf) {
        Ok(w) => w,
        Err(e) => {
            warn!(error = %e, "❌ Invalid binary payload (msgpack decode failed)");
            return (true, None); // Continue connection on decode errors
        }
    };

    match wire {
        WireMessage::App(app_msg) => {
            // Increment received message metrics
            #[cfg(feature = "metrics")]
            CHAT_METRICS.message_received();

            handle_client_message(server, client, app_msg).await;
            (true, None)
        }
        WireMessage::Hello => {
            warn!(
                username = client.username(),
                "⚠️  Ignoring duplicate Hello message after authentication"
            );
            (false, None) // Close connection on duplicate auth attempt
        }
        WireMessage::Close => {
            debug!(
                username = client.username(),
                "🔸 Client sent Close message"
            );
            (false, None) // Close connection
        }
        WireMessage::Ping => {
            trace!(
                "⬅️  PING received from user: {} (sending Pong)",
                client.username(),
            );
            // Send Pong response (QUIC handles transport keepalive, but respond for client compatibility)
            (true, Some(PONG_WIRE.to_vec()))
        }
        WireMessage::Pong => {
            trace!(
                "⬅️  PONG received - user: {}, connection_id: {}",
                client.username(),
                client.connection_id()
            );
            (true, None) // Continue connection
        }
    }
}

/// Pre-encoded MessagePack for {"t":"Pong"} matching the #[serde(tag = "t")] layout.
/// Bytes: 0x81 A1 't' A4 'P' 'o' 'n' 'g'
const PONG_WIRE: &[u8] = b"\x81\xa1t\xa4Pong";

/// Pre-encoded MessagePack for {"t":"Close"} matching the #[serde(tag = "t")] layout.
/// Bytes: 0x81 A1 't' A5 'C' 'l' 'o' 's' 'e'
const CLOSE_WIRE: &[u8] = b"\x81\xa1t\xa5Close";

#[derive(serde::Deserialize)]
#[serde(tag = "t", content = "d")]
enum WireMessage {
    Ping,
    Pong,
    Close,
    Hello,
    App(ClientMessage),
}
