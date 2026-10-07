//! Chat-domain events carried over Claviron's Orbit event bridge.
//!
//! Orbit event frames are intentionally small. Chat therefore publishes compact
//! delivery signals here and lets each worker hydrate larger persisted messages
//! from the chat stores before fanning out to its local clients.

use claviron::orbit::FleetEventCodec;
use claviron::runtime::events::Event;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::types::messages::MessageStatusType;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ChatDeliveryEvent {
    RoomMessage {
        uuid: Uuid,
    },
    DirectMessage {
        uuid: Uuid,
        to_user_id: i32,
    },
    UserJoined {
        room: String,
        user_id: i32,
    },
    UserLeft {
        room: String,
        user_id: i32,
    },
    UserOnline {
        user_id: i32,
    },
    UserOffline {
        user_id: i32,
    },
    UserTyping {
        room: String,
        user_id: i32,
    },
    UserTypingDirect {
        from_user_id: i32,
        to_user_id: i32,
    },
    MessageStatus {
        uuid: Uuid,
        sender_user_id: i32,
        status: MessageStatusType,
        room_message: bool,
    },
}

/// Ephemeral worker-presence records replicated through Orbit.
///
/// A zero connection count removes the worker's record immediately. Active
/// records are periodically refreshed so a new worker can reconstruct the
/// current fleet view and records left by a crashed worker can expire locally.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ChatPresenceEvent {
    User {
        instance_id: Uuid,
        revision: u64,
        user_id: i32,
        connections: u32,
    },
    Room {
        instance_id: Uuid,
        revision: u64,
        user_id: i32,
        room: String,
        connections: u32,
        created_at_unix: i64,
    },
}

impl Event for ChatDeliveryEvent {}

impl FleetEventCodec for ChatDeliveryEvent {
    const TOPIC: &'static str = "claviron.chat.delivery.v1";

    fn encode_orbit(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|err| err.to_string())
    }
}

impl Event for ChatPresenceEvent {}

impl FleetEventCodec for ChatPresenceEvent {
    const TOPIC: &'static str = "claviron.chat.presence.v1";

    fn encode_orbit(&self) -> Vec<u8> {
        rmp_serde::to_vec(self).unwrap_or_default()
    }

    fn decode_orbit(bytes: &[u8]) -> Result<Self, String> {
        rmp_serde::from_slice(bytes).map_err(|err| err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use claviron::orbit::{EVENT_PAYLOAD_MAX, FleetEventCodec};
    use uuid::Uuid;

    use super::ChatPresenceEvent;

    #[test]
    fn presence_room_event_fits_orbit_frame() {
        let event = ChatPresenceEvent::Room {
            instance_id: Uuid::nil(),
            revision: u64::MAX,
            user_id: i32::MAX,
            room: "r".repeat(128),
            connections: u32::MAX,
            created_at_unix: i64::MAX,
        };
        let encoded = event.encode_orbit();
        let frame_len = 12 + ChatPresenceEvent::TOPIC.len() + encoded.len();

        assert!(
            frame_len <= EVENT_PAYLOAD_MAX,
            "presence frame is {frame_len} bytes"
        );
        assert!(matches!(
            ChatPresenceEvent::decode_orbit(&encoded),
            Ok(ChatPresenceEvent::Room { room, .. }) if room.len() == 128
        ));
    }
}
