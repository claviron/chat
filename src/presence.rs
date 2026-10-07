//! Fleet-replicated chat presence.
//!
//! Transport handles stay worker-local. This registry stores only compact
//! remote user/room counters learned through Orbit; callers merge them with
//! their authoritative local connection maps when answering presence queries.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use uuid::Uuid;

use crate::event::ChatPresenceEvent;

#[derive(Clone)]
pub(crate) struct PresenceRegistry {
    instance_id: Uuid,
    revision: Arc<AtomicU64>,
    remote: Arc<RwLock<RemotePresence>>,
}

#[derive(Default)]
struct RemotePresence {
    users: HashMap<(Uuid, i32), RemoteUser>,
    rooms: HashMap<(Uuid, i32, String), RemoteRoom>,
}

struct RemoteUser {
    connections: usize,
    revision: u64,
    last_seen: Instant,
}

struct RemoteRoom {
    connections: usize,
    created_at_unix: i64,
    revision: u64,
    last_seen: Instant,
}

#[derive(Debug)]
pub(crate) struct RemoteRoomSummary {
    pub(crate) member_count: usize,
    pub(crate) created_at_unix: i64,
}

impl PresenceRegistry {
    pub(crate) fn new() -> Self {
        Self {
            instance_id: Uuid::new_v4(),
            revision: Arc::new(AtomicU64::new(0)),
            remote: Arc::default(),
        }
    }

    pub(crate) fn instance_id(&self) -> Uuid {
        self.instance_id
    }

    pub(crate) fn next_revision(&self) -> u64 {
        self.revision.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
    }

    pub(crate) async fn apply(
        &self,
        event: ChatPresenceEvent,
    ) {
        let now = Instant::now();
        let mut remote = self.remote.write().await;
        match event {
            ChatPresenceEvent::User {
                instance_id,
                revision,
                user_id,
                connections,
            } => {
                if instance_id == self.instance_id {
                    return;
                }
                let key = (instance_id, user_id);
                if remote
                    .users
                    .get(&key)
                    .is_some_and(|record| record.revision >= revision)
                {
                    return;
                }
                remote.users.insert(
                    key,
                    RemoteUser {
                        connections: connections as usize,
                        revision,
                        last_seen: now,
                    },
                );
                if connections == 0 {
                    for ((worker, user, _), record) in &mut remote.rooms {
                        if *worker == instance_id
                            && *user == user_id
                            && record.revision < revision
                        {
                            record.connections = 0;
                            record.revision = revision;
                            record.last_seen = now;
                        }
                    }
                }
            }
            ChatPresenceEvent::Room {
                instance_id,
                revision,
                user_id,
                room,
                connections,
                created_at_unix,
            } => {
                if instance_id == self.instance_id {
                    return;
                }
                if remote.users.get(&(instance_id, user_id)).is_some_and(
                    |record| {
                        record.revision >= revision && record.connections == 0
                    },
                ) {
                    return;
                }
                let key = (instance_id, user_id, room);
                if remote
                    .rooms
                    .get(&key)
                    .is_some_and(|record| record.revision >= revision)
                {
                    return;
                }
                remote.rooms.insert(
                    key,
                    RemoteRoom {
                        connections: connections as usize,
                        created_at_unix,
                        revision,
                        last_seen: now,
                    },
                );
            }
        }
    }

    pub(crate) async fn has_remote_user(
        &self,
        user_id: i32,
        max_age: Duration,
    ) -> bool {
        let now = Instant::now();
        let mut remote = self.remote.write().await;
        remote.prune(now, max_age);
        remote.users.iter().any(|((_, candidate), record)| {
            *candidate == user_id && record.connections > 0
        })
    }

    pub(crate) async fn has_remote_room_user(
        &self,
        room: &str,
        user_id: i32,
        max_age: Duration,
    ) -> bool {
        let now = Instant::now();
        let mut remote = self.remote.write().await;
        remote.prune(now, max_age);
        remote.rooms.iter().any(|((_, candidate, candidate_room), record)| {
            *candidate == user_id
                && candidate_room == room
                && record.connections > 0
        })
    }

    pub(crate) async fn remote_user_ids(
        &self,
        max_age: Duration,
    ) -> HashSet<i32> {
        let now = Instant::now();
        let mut remote = self.remote.write().await;
        remote.prune(now, max_age);
        remote
            .users
            .iter()
            .filter(|(_, record)| record.connections > 0)
            .map(|((_, user_id), _)| *user_id)
            .collect()
    }

    pub(crate) async fn remote_room_user_ids(
        &self,
        room: &str,
        max_age: Duration,
    ) -> HashSet<i32> {
        let now = Instant::now();
        let mut remote = self.remote.write().await;
        remote.prune(now, max_age);
        remote
            .rooms
            .iter()
            .filter(|((_, _, candidate_room), record)| {
                candidate_room == room && record.connections > 0
            })
            .map(|((_, user_id, _), _)| *user_id)
            .collect()
    }

    pub(crate) async fn remote_rooms(
        &self,
        max_age: Duration,
    ) -> HashMap<String, RemoteRoomSummary> {
        let now = Instant::now();
        let mut remote = self.remote.write().await;
        remote.prune(now, max_age);

        let mut rooms = HashMap::<String, RemoteRoomSummary>::new();
        for ((_, _, room), record) in &remote.rooms {
            if record.connections == 0 {
                continue;
            }
            let summary =
                rooms.entry(room.clone()).or_insert(RemoteRoomSummary {
                    member_count: 0,
                    created_at_unix: record.created_at_unix,
                });
            summary.member_count += record.connections;
            summary.created_at_unix =
                summary.created_at_unix.min(record.created_at_unix);
        }
        rooms
    }
}

impl RemotePresence {
    fn prune(
        &mut self,
        now: Instant,
        max_age: Duration,
    ) {
        self.users.retain(|_, record| {
            now.duration_since(record.last_seen) <= max_age
        });
        self.rooms.retain(|_, record| {
            now.duration_since(record.last_seen) <= max_age
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::PresenceRegistry;
    use crate::event::ChatPresenceEvent;

    #[tokio::test]
    async fn remote_presence_is_aggregated_and_removed() {
        let source = PresenceRegistry::new();
        let target = PresenceRegistry::new();
        let instance_id = source.instance_id();

        target
            .apply(ChatPresenceEvent::User {
                instance_id,
                revision: source.next_revision(),
                user_id: 42,
                connections: 2,
            })
            .await;
        target
            .apply(ChatPresenceEvent::Room {
                instance_id,
                revision: source.next_revision(),
                user_id: 42,
                room: "general".to_string(),
                connections: 1,
                created_at_unix: 100,
            })
            .await;

        let max_age = Duration::from_secs(30);
        assert!(target.has_remote_user(42, max_age).await);
        assert!(target.has_remote_room_user("general", 42, max_age).await);
        assert_eq!(
            target.remote_rooms(max_age).await["general"].member_count,
            1
        );

        target
            .apply(ChatPresenceEvent::User {
                instance_id,
                revision: source.next_revision(),
                user_id: 42,
                connections: 0,
            })
            .await;

        assert!(!target.has_remote_user(42, max_age).await);
        assert!(!target.has_remote_room_user("general", 42, max_age).await);
    }

    #[tokio::test]
    async fn local_echo_is_ignored() {
        let presence = PresenceRegistry::new();
        presence
            .apply(ChatPresenceEvent::User {
                instance_id: presence.instance_id(),
                revision: presence.next_revision(),
                user_id: 7,
                connections: 1,
            })
            .await;

        assert!(
            presence.remote_user_ids(Duration::from_secs(30)).await.is_empty()
        );
    }

    #[tokio::test]
    async fn user_stays_online_while_another_worker_has_connections() {
        let first_worker = PresenceRegistry::new();
        let second_worker = PresenceRegistry::new();
        let observer = PresenceRegistry::new();
        let max_age = Duration::from_secs(30);

        observer
            .apply(ChatPresenceEvent::User {
                instance_id: first_worker.instance_id(),
                revision: first_worker.next_revision(),
                user_id: 9,
                connections: 2,
            })
            .await;
        observer
            .apply(ChatPresenceEvent::User {
                instance_id: second_worker.instance_id(),
                revision: second_worker.next_revision(),
                user_id: 9,
                connections: 1,
            })
            .await;
        observer
            .apply(ChatPresenceEvent::Room {
                instance_id: first_worker.instance_id(),
                revision: first_worker.next_revision(),
                user_id: 9,
                room: "support".to_string(),
                connections: 2,
                created_at_unix: 100,
            })
            .await;
        observer
            .apply(ChatPresenceEvent::Room {
                instance_id: second_worker.instance_id(),
                revision: second_worker.next_revision(),
                user_id: 9,
                room: "support".to_string(),
                connections: 1,
                created_at_unix: 110,
            })
            .await;
        observer
            .apply(ChatPresenceEvent::User {
                instance_id: first_worker.instance_id(),
                revision: first_worker.next_revision(),
                user_id: 9,
                connections: 0,
            })
            .await;

        assert!(observer.has_remote_user(9, max_age).await);
        assert_eq!(observer.remote_user_ids(max_age).await.len(), 1);
        assert_eq!(
            observer.remote_rooms(max_age).await["support"].member_count,
            1
        );

        observer
            .apply(ChatPresenceEvent::User {
                instance_id: second_worker.instance_id(),
                revision: second_worker.next_revision(),
                user_id: 9,
                connections: 0,
            })
            .await;

        assert!(!observer.has_remote_user(9, max_age).await);
    }

    #[tokio::test]
    async fn stale_worker_records_expire() {
        let source = PresenceRegistry::new();
        let observer = PresenceRegistry::new();
        observer
            .apply(ChatPresenceEvent::User {
                instance_id: source.instance_id(),
                revision: source.next_revision(),
                user_id: 12,
                connections: 1,
            })
            .await;

        {
            let mut remote = observer.remote.write().await;
            let record = remote
                .users
                .get_mut(&(source.instance_id(), 12))
                .expect("remote user should exist");
            record.last_seen -= Duration::from_secs(2);
        }

        assert!(
            observer.remote_user_ids(Duration::from_secs(1)).await.is_empty()
        );
    }

    #[tokio::test]
    async fn stale_snapshot_cannot_resurrect_removed_presence() {
        let source = PresenceRegistry::new();
        let observer = PresenceRegistry::new();
        let stale_revision = source.next_revision();
        let stale = ChatPresenceEvent::User {
            instance_id: source.instance_id(),
            revision: stale_revision,
            user_id: 21,
            connections: 2,
        };

        observer.apply(stale.clone()).await;
        observer
            .apply(ChatPresenceEvent::User {
                instance_id: source.instance_id(),
                revision: source.next_revision(),
                user_id: 21,
                connections: 0,
            })
            .await;
        observer.apply(stale).await;

        assert!(
            observer.remote_user_ids(Duration::from_secs(30)).await.is_empty()
        );
    }
}
