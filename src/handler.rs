use tracing::debug;

use crate::ChatServer;
use crate::types::client::ChatClient;
use crate::types::messages::{ClientMessage, MessageStatusType, ServerMessage};

/// Handle text message from client (same logic as Axum version)
pub(crate) async fn handle_client_message(
    server: &ChatServer,
    client: &ChatClient,
    msg: ClientMessage,
) {
    debug!(
        username = client.username(),
        // message = ?msg,
        "📥 Received  message from client"
    );

    // Handle message (reuse existing logic from app/svc/handler.rs)
    if !client.auth_valid() {
        return;
    }

    let result = match msg {
        ClientMessage::JoinRoom { room } => {
            match server.join_room(client.connection_id(), &room).await {
                Ok(_) => {
                    // Send updated user list to room
                    send_user_list_to_room(server, &room).await;
                    Ok(format!("you joined room: {}", room))
                }
                Err(e) => Err(e),
            }
        }

        ClientMessage::LeaveRoom { room } => server
            .leave_room(client.connection_id(), &room)
            .await
            .map(|_| format!("you left room: {}", room)),

        ClientMessage::SendToRoom { uuid, room, content } => {
            // Check rate limit before sending
            if let Err(retry_after) =
                server.check_rate_limit(client.connection_id())
            {
                let _ = client
                    .send(ServerMessage::message_status(
                        uuid,
                        MessageStatusType::Failed,
                    ))
                    .await;
                Err(format!(
                    "Rate limit exceeded. Try again in {} seconds",
                    retry_after
                ))
            } else {
                match server
                    .send_to_room(client.user_id(), &room, uuid, content)
                    .await
                {
                    Ok(_) => {
                        // Client already shows optimistic UI, no need to send "Sent" status
                        Ok(String::new())
                    }
                    Err(e) => {
                        // Only notify on failure
                        let _ = client
                            .send(ServerMessage::message_status(
                                uuid,
                                MessageStatusType::Failed,
                            ))
                            .await;
                        Err(e)
                    }
                }
            }
        }

        ClientMessage::SendDirect { uuid, to_user_uuid, content } => {
            // Check rate limit before sending
            if let Err(retry_after) =
                server.check_rate_limit(client.connection_id())
            {
                let _ = client
                    .send(ServerMessage::message_status(
                        uuid,
                        MessageStatusType::Failed,
                    ))
                    .await;
                Err(format!(
                    "Rate limit exceeded. Try again in {} seconds",
                    retry_after
                ))
            } else {
                match server.user_id_by_uuid(&to_user_uuid).await {
                    Ok(to_user_id) => {
                        match server
                            .send_direct(
                                client.user_id(),
                                to_user_id,
                                uuid,
                                content,
                            )
                            .await
                        {
                            Ok(_) => {
                                // Client already shows optimistic UI, no need to send "Sent" status
                                Ok(String::new())
                            }
                            Err(e) => {
                                // Only notify on failure
                                let _ = client
                                    .send(ServerMessage::message_status(
                                        uuid,
                                        MessageStatusType::Failed,
                                    ))
                                    .await;
                                Err(e)
                            }
                        }
                    }
                    Err(e) => {
                        // Only notify on failure (user lookup failed)
                        let _ = client
                            .send(ServerMessage::message_status(
                                uuid,
                                MessageStatusType::Failed,
                            ))
                            .await;
                        Err(e)
                    }
                }
            }
        }

        ClientMessage::Typing { room } => server
            .send_typing(client.user_id(), &room)
            .await
            .map(|_| String::new()),

        ClientMessage::TypingDirect { to_user_uuid } => {
            match server.user_id_by_uuid(&to_user_uuid).await {
                Ok(to_user_id) => server
                    .send_typing_direct(client.user_id(), to_user_id)
                    .await
                    .map(|_| String::new()),
                Err(e) => Err(e),
            }
        }

        ClientMessage::GetHistory { room, limit, since, before } => {
            match server.get_room_history(&room, limit, since, before).await {
                Ok(messages) => {
                    let _ = client
                        .send(ServerMessage::MessageHistory {
                            room: room.clone(),
                            messages,
                        })
                        .await;
                    Ok(String::new())
                }
                Err(e) => Err(e),
            }
        }

        ClientMessage::GetDirectHistory {
            with_user_uuid,
            limit,
            since,
            before,
        } => match server.user_id_by_uuid(&with_user_uuid).await {
            Ok(with_user_id) => {
                match server
                    .get_direct_history(
                        client.user_id(),
                        with_user_id,
                        limit,
                        since,
                        before,
                    )
                    .await
                {
                    Ok(messages) => {
                        let _ = client
                            .send(ServerMessage::DirectMessageHistory {
                                with_user_uuid,
                                messages,
                            })
                            .await;
                        Ok(String::new())
                    }
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        },

        ClientMessage::ListRooms => {
            let rooms = server.list_rooms().await;
            debug!(
                username = client.username(),
                "📋 ListRooms request received"
            );
            let _ = client.send(ServerMessage::RoomList { rooms }).await;
            Ok(String::new())
        }

        ClientMessage::ListUsers { room } => match server
            .list_users(&room)
            .await
        {
            Ok(users) => {
                let _ = client
                    .send(ServerMessage::UserList { room: room.clone(), users })
                    .await;
                Ok(String::new())
            }
            Err(e) => Err(e),
        },

        ClientMessage::MarkDelivered { uuid, room } => server
            .mark_delivered(&uuid, client.user_id(), room.as_deref())
            .await
            .map(|_| String::new()),

        ClientMessage::MarkRead { uuid, room } => server
            .mark_read(&uuid, client.user_id(), room.as_deref())
            .await
            .map(|_| String::new()),

        ClientMessage::GetUnreadPrivate => {
            match server.get_unread_private_details(client.user_id()).await {
                Ok(messages) => {
                    let _ = client
                        .send(ServerMessage::UnreadPrivateMessages { messages })
                        .await;
                    Ok(String::new())
                }
                Err(e) => Err(e),
            }
        }

        ClientMessage::GetUnreadRoom { room } => {
            match server.get_unread_room_count(client.user_id(), &room).await {
                Ok(count) => {
                    let _ = client
                        .send(ServerMessage::UnreadRoomMessages {
                            room: room.clone(),
                            count,
                        })
                        .await;
                    Ok(String::new())
                }
                Err(e) => Err(e),
            }
        }

        ClientMessage::GetOnlineUsers => {
            match server.get_online_users().await {
                Ok(users) => {
                    let _ =
                        client.send(ServerMessage::OnlineUsers { users }).await;
                    Ok(String::new())
                }
                Err(e) => Err(e),
            }
        }
    };

    // Send response if needed
    match result {
        Ok(msg) if !msg.is_empty() => {
            let _ = client.send(ServerMessage::success(msg)).await;
        }
        Err(error) => {
            let _ = client.send(ServerMessage::error(error)).await;
        }
        _ => {}
    }
}

/// Publish presence as protocol data, never as a synthetic chat message.
async fn send_user_list_to_room(
    server: &ChatServer,
    room_name: &str,
) {
    if let Ok(users) = server.list_users(room_name).await {
        debug!(
            room = %room_name,
            user_count = users.len(),
            "📤 Broadcasting user list to room"
        );
        let _ = server
            .broadcast_to_room(
                room_name,
                ServerMessage::UserList { room: room_name.to_string(), users },
            )
            .await;
    }
}
