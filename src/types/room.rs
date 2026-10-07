use std::collections::HashMap;

use time::OffsetDateTime;
use tracing::{debug, warn};
use uuid::Uuid;

use super::client::ChatClient;
use super::messages::ServerMessage;

/// Represents a svc room/channel
pub struct ChatRoom {
    /// Room identifier
    pub name: String,

    /// Connected clients in this room (connection_id -> client) - supports multi-device
    members: HashMap<Uuid, ChatClient>,

    /// Room creation time
    pub created_at: OffsetDateTime,
}

impl ChatRoom {
    /// Create a new svc room
    pub fn new(name: String) -> Self {
        Self {
            name,
            members: HashMap::new(),
            created_at: OffsetDateTime::now_utc(),
        }
    }

    /// Add a client to this room (multi-device: same user can join multiple times with different connections)
    pub fn add_member(
        &mut self,
        client: ChatClient,
    ) -> bool {
        let connection_id = client.connection_id();
        let user_id = client.user_id();

        if self.members.contains_key(&connection_id) {
            warn!(
                room = %self.name,
                connection_id = %connection_id,
                user_id = user_id,
                "Connection already in room"
            );
            return false;
        }

        debug!(
            room = %self.name,
            connection_id = %connection_id,
            user_id = user_id,
            username = client.username(),
            "Connection joined room"
        );

        self.members.insert(connection_id, client);
        true
    }

    /// Remove a client from this room by connection_id
    pub fn remove_member_by_connection(
        &mut self,
        connection_id: Uuid,
    ) -> Option<ChatClient> {
        if let Some(client) = self.members.remove(&connection_id) {
            debug!(
                room = %self.name,
                connection_id = %connection_id,
                user_id = client.user_id(),
                username = client.username(),
                "Connection left room"
            );
            Some(client)
        } else {
            warn!(
                room = %self.name,
                connection_id = %connection_id,
                "Attempted to remove connection not in room"
            );
            None
        }
    }

    /// Get number of members in this room
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Check if room is empty
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Get a client by connection ID
    #[allow(unused)]
    pub fn get_connection(
        &self,
        connection_id: Uuid,
    ) -> Option<&ChatClient> {
        self.members.get(&connection_id)
    }

    /// Get all members
    pub fn get_members(&self) -> Vec<&ChatClient> {
        self.members.values().collect()
    }

    /// Broadcast a message to all members in this room
    pub async fn broadcast(
        &self,
        msg: ServerMessage,
    ) {
        let mut failed_clients = Vec::new();

        for (connection_id, client) in &self.members {
            if !client.send(msg.clone()).await {
                failed_clients.push(*connection_id);
            }
        }

        if !failed_clients.is_empty() {
            warn!(
                room = %self.name,
                failed_count = failed_clients.len(),
                "Some broadcasts failed"
            );
        }
    }

    /// Broadcast a message to all connections except one (by connection_id)
    #[allow(unused)]
    pub async fn broadcast_except_connection(
        &self,
        msg: ServerMessage,
        exclude_connection_id: Uuid,
    ) {
        let mut failed_clients = Vec::new();
        let mut sent_count = 0;

        debug!(
            room = %self.name,
            total_members = self.members.len(),
            exclude_connection_id = %exclude_connection_id,
            "Starting broadcast_except_connection"
        );

        for (connection_id, client) in &self.members {
            if *connection_id == exclude_connection_id {
                debug!(
                    room = %self.name,
                    connection_id = %connection_id,
                    "Skipping excluded connection"
                );
                continue;
            }

            debug!(
                room = %self.name,
                connection_id = %connection_id,
                user_id = client.user_id(),
                username = client.username(),
                "Sending to member"
            );

            if !client.send(msg.clone()).await {
                failed_clients.push(*connection_id);
            } else {
                sent_count += 1;
            }
        }

        debug!(
            room = %self.name,
            sent_count = sent_count,
            failed_count = failed_clients.len(),
            "Broadcast complete"
        );

        if !failed_clients.is_empty() {
            warn!(
                room = %self.name,
                failed_count = failed_clients.len(),
                "Some broadcasts failed"
            );
        }
    }

    /// Try to broadcast (non-blocking, drops message if channel is full)
    #[allow(unused)]
    pub fn try_broadcast(
        &self,
        msg: ServerMessage,
    ) {
        for client in self.members.values() {
            client.try_send(msg.clone());
        }
    }

    /// Try to broadcast except one connection (non-blocking)
    #[allow(unused)]
    pub fn try_broadcast_except_connection(
        &self,
        msg: ServerMessage,
        exclude_connection_id: Uuid,
    ) {
        for (connection_id, client) in &self.members {
            if *connection_id != exclude_connection_id {
                client.try_send(msg.clone());
            }
        }
    }
}
