use claviron::runtime::AppState;
use fastwebsockets::{FragmentCollector, Frame, OpCode, WebSocket};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use rmp_serde as rmps;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use crate::models::users::ChatUser;
use crate::svc::WELCOME_MESSAGE;
#[cfg(feature = "metrics")]
use crate::svc::metrics::CHAT_METRICS;
use crate::types::messages::{ClientMessage, ServerMessage, UnreadSummary};
use crate::{ChatClient, ChatServer, handle_client_message};

/// Handle WebSocket connection with fastwebsockets
pub(crate) async fn handle_websocket(
    upgraded: Upgraded,
    state: AppState,
    authenticated: crate::types::client::ChatAuthentication,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let crate::types::client::ChatAuthentication {
        user,
        session: auth_session,
    } = authenticated;
    let server = state
        .registry_ref()
        .resolve::<ChatServer>()
        .ok_or("ChatServer not registered")?;
    let chat_config = server.config_snapshot();

    debug!(
        username = %user.username(),
        "🔐 WebSocket connection established"
    );

    // Create fastwebsockets instance (no Arc<Mutex> needed!)
    let mut raw_ws = WebSocket::after_handshake(
        TokioIo::new(upgraded),
        fastwebsockets::Role::Server,
    );
    raw_ws.set_auto_close(true);
    raw_ws.set_auto_pong(true);
    let mut ws = FragmentCollector::new(raw_ws);

    // Create outbound channel for server messages
    let (tx, mut rx) =
        mpsc::channel::<ServerMessage>(chat_config.max_message_size);

    // Create svc client and register with server
    let client =
        ChatClient::new(user.clone(), tx.clone()).with_session(auth_session);
    if !client.auth_valid() {
        return Err("chat session expired or revoked".into());
    }
    let connection_id = client.connection_id();
    server.connect_client(client.clone()).await;

    // Increment connection metrics
    #[cfg(feature = "metrics")]
    CHAT_METRICS.connect();

    debug!(
        "🔌 WebSocket connection - username: {}, connection_id: {}",
        user.username(),
        connection_id
    );

    // Send welcome message
    {
        let unread_summary = server.get_unread_summary(user.id()).await.unwrap_or_else(|e| {
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
        );

        if let Err(e) = tx.send(welcome_msg).await {
            warn!(username = user.username(), error = %e, "❌ Failed to send welcome message");
        }
    }

    // Setup ping timer
    let ping_interval_duration = chat_config.ping_interval;
    let idle_timeout = chat_config.idle_timeout;

    let mut ping_timer = interval(ping_interval_duration);
    ping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ping_timer.tick().await; // Skip the first immediate tick

    debug!(
        username = %user.username(),
        connection_id = %connection_id,
        ping_interval = ?ping_interval_duration,
        idle_timeout = ?idle_timeout,
        "💬 Starting WebSocket event loop"
    );

    // Main event loop - no deadlocks, clean and simple!
    loop {
        tokio::select! {
            // 1) Shutdown signal
            _ = state.on_shutdown() => {
                warn!(
                    username = %user.username(),
                    "🔻 Shutdown signal received"
                );
                break;
            }

            // 2) Ping timer tick
            _ = ping_timer.tick() => {
                if !client.auth_valid() { break; }
                if let Err(e) = send_ping(&mut ws, &user, connection_id).await {
                    error!(
                        username = %user.username(),
                        connection_id = %connection_id,
                        error = %e,
                        "🏓 ❌ Failed to send PING"
                    );
                    break;
                }
            }

            // 3) Outbound message from channels
            Some(msg) = rx.recv() => {
                if !client.auth_valid() { break; }
                match rmps::to_vec_named(&msg) {
                    Ok(bytes) => {
                        trace!(message = ?msg, size = bytes.len(), "📤 Sending message to client");
                        let frame = Frame::binary(
                            fastwebsockets::Payload::Borrowed(&bytes)
                        );
                        if let Err(e) = ws.write_frame(frame).await {
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

            // 4) Inbound frame from WebSocket (with idle timeout)
            result = tokio::time::timeout(idle_timeout, ws.read_frame()) => {
                match result {
                    Ok(Ok(frame)) => {
                        if !handle_ws_frame(server.as_ref(), &client, frame).await {
                            break;
                        }
                    }
                    Ok(Err(e)) => {
                        let es = e.to_string();
                        if es.contains("unexpected eof") || es.contains("close_notify") || es.contains("endpoint closed the connection") {
                            info!(username = user.username(), error = %es, "Peer closed connection");
                        } else {
                            error!(username = user.username(), error = %es, "❌ WebSocket read error");
                        }
                        break;
                    }
                    Err(_) => {
                        warn!(
                            "💀 WebSocket IDLE TIMEOUT - username: {}, no client activity",
                            user.username()
                        );
                        break;
                    }
                }
            }
        }
    }

    // Cleanup: send close frame
    debug!(username = user.username(), "📤 Sending close frame");
    let close_frame = Frame::close(1000, b"Normal closure");
    let _ = ws.write_frame(close_frame).await;

    // Decrement connection metrics
    #[cfg(feature = "metrics")]
    CHAT_METRICS.disconnect();

    server.disconnect_client(connection_id).await;

    info!(
        username = %user.username(),
        connection_id = %connection_id,
        "🏁 WebSocket connection closed gracefully"
    );

    Ok(())
}

/// Send PING frame to client
async fn send_ping<S>(
    ws: &mut FragmentCollector<S>,
    user: &ChatUser,
    connection_id: Uuid,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = Frame::new(
        true,
        OpCode::Ping,
        None,
        fastwebsockets::Payload::Borrowed(&[]),
    );
    ws.write_frame(frame).await?;
    trace!(
        "➡️  PING sent - username: {}, connection_id: {}",
        user.username(),
        connection_id
    );
    Ok(())
}

/// Handle individual WebSocket frames
pub(crate) async fn handle_ws_frame(
    server: &ChatServer,
    client: &ChatClient,
    frame: Frame<'_>,
) -> bool {
    if !client.auth_valid() {
        return false;
    }
    match frame.opcode {
        OpCode::Binary => {
            match rmps::from_slice::<ClientMessage>(&frame.payload) {
                Ok(msg) => {
                    // Increment received message metrics
                    #[cfg(feature = "metrics")]
                    CHAT_METRICS.message_received();

                    handle_client_message(server, client, msg).await;
                    true
                }
                Err(e) => {
                    warn!(error = %e, "❌ Invalid binary payload (msgpack decode failed)");
                    true
                }
            }
        }
        OpCode::Text => {
            warn!(
                user_name = client.username(),
                "⚠️ Ignoring unexpected text frame"
            );
            true
        }
        OpCode::Close => {
            debug!(user_id = client.username(), "🔸 Client sent close frame");
            false
        }
        OpCode::Ping => {
            trace!(
                "⬅️  PING received from user: {} (auto PONG sent by fastwebsockets)",
                client.username(),
            );
            true
        }
        OpCode::Pong => {
            trace!(
                "⬅️  PONG received - user: {}, connection_id: {}",
                client.username(),
                client.connection_id()
            );
            true
        }
        _ => {
            warn!(user_id = client.user_id(), "❗ Received unknown frame type");
            true
        }
    }
}

#[cfg(test)]
#[path = "../../../tests/svc/web/session.rs"]
mod tests;
