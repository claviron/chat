//! Chat records use the host's Claviron pipeline; chat never opens a log file.

use std::sync::Arc;

use claviron::logging::pipeline::{
    ChannelId, Level, LogChannels, Record, Value,
};

use crate::event::ChatDeliveryEvent;
use crate::types::messages::MessageStatusType;

pub(crate) struct ChatLog {
    channels: Arc<LogChannels>,
    channel: ChannelId,
}

impl ChatLog {
    pub(crate) fn resolve(channels: Arc<LogChannels>) -> Option<Self> {
        // The producer identity is fixed; its destination is operator-owned.
        let channel = channels.channel("chat")?;
        Some(Self { channels, channel })
    }

    pub(crate) fn record(
        &self,
        level: Level,
        message: &str,
        fields: &[(&str, Value<'_>)],
    ) {
        let record = Record { level, target: "claviron_chat", message, fields };
        if let Err(error) = self.channels.emit(self.channel, &record) {
            // Pipeline failures must not be reported through the failed channel.
            tracing::error!(%error, "Chat log record could not be published");
        }
    }

    /// Log only compact delivery metadata, once at the originating worker.
    pub(crate) fn delivery(
        &self,
        event: &ChatDeliveryEvent,
    ) {
        use ChatDeliveryEvent::*;
        use Value::{I64, Str};

        match event {
            RoomMessage { uuid } | DirectMessage { uuid, .. } => {
                let uuid = uuid.to_string();
                let kind = if matches!(event, RoomMessage { .. }) {
                    "room"
                } else {
                    "private"
                };
                self.record(
                    Level::Info,
                    "Chat message accepted",
                    &[("message_uuid", Str(&uuid)), ("kind", Str(kind))],
                );
            }
            UserJoined { room, user_id } | UserLeft { room, user_id } => {
                let action = if matches!(event, UserJoined { .. }) {
                    "joined"
                } else {
                    "left"
                };
                self.record(
                    Level::Info,
                    "Chat room membership changed",
                    &[
                        ("user_id", I64(i64::from(*user_id))),
                        ("room", Str(room)),
                        ("action", Str(action)),
                    ],
                );
            }
            UserOnline { user_id } | UserOffline { user_id } => {
                self.record(
                    Level::Info,
                    "Chat presence changed",
                    &[
                        ("user_id", I64(i64::from(*user_id))),
                        (
                            "online",
                            Value::Bool(matches!(event, UserOnline { .. })),
                        ),
                    ],
                );
            }
            MessageStatus { uuid, status, room_message, .. } => {
                let uuid = uuid.to_string();
                let status = match status {
                    MessageStatusType::Sent => "sent",
                    MessageStatusType::Delivered => "delivered",
                    MessageStatusType::Read => "read",
                    MessageStatusType::Failed => "failed",
                };
                self.record(
                    Level::Debug,
                    "Chat message status changed",
                    &[
                        ("message_uuid", Str(&uuid)),
                        ("status", Str(status)),
                        ("room_message", Value::Bool(*room_message)),
                    ],
                );
            }
            // Typing and periodic presence refresh are deliberately not log records.
            UserTyping { .. } | UserTypingDirect { .. } => {}
        }
    }
}
