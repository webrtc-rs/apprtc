//! P2P-only V2 room state.
//!
//! This module deliberately does not reuse the V1 room types: V1 identifiers are
//! opaque strings and retain the legacy lazy-registration behavior, whereas V2
//! identifiers are `u64` values and every browser socket must prove a prior
//! admission with its token.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

const MAX_P2P_MEMBERS: usize = 2;
const MAX_QUEUED_MESSAGES: usize = 1024;

pub type RoomId = u64;
pub type ClientId = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    pub signal_epoch: u64,
    pub admission_token: String,
    pub is_initiator: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    pub signal_epoch: u64,
    pub is_initiator: bool,
    pub queued_messages: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub room_id: RoomId,
    pub client_id: ClientId,
    pub signal_epoch: u64,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Promotion {
    pub room_id: RoomId,
    pub client_id: ClientId,
    pub signal_epoch: u64,
}

struct Member {
    token: String,
    is_initiator: bool,
    registered: bool,
    reconnect_deadline: Option<Instant>,
    queued_messages: VecDeque<String>,
}

struct Room {
    members: HashMap<ClientId, Member>,
    signal_epoch: u64,
}

pub struct RoomTable {
    rooms: HashMap<RoomId, Room>,
    reconnect_grace: Duration,
}

impl RoomTable {
    pub fn new(reconnect_grace: Duration) -> Self {
        Self {
            rooms: HashMap::new(),
            reconnect_grace,
        }
    }

    pub fn admit(
        &mut self,
        now: Instant,
        room_id: RoomId,
        client_id: ClientId,
        admission_token: String,
    ) -> Result<Admission, String> {
        let room = self.rooms.entry(room_id).or_insert_with(|| Room {
            members: HashMap::new(),
            signal_epoch: 0,
        });
        if room.members.contains_key(&client_id) {
            return Err("DUPLICATE_CLIENT".into());
        }
        if room.members.len() >= MAX_P2P_MEMBERS {
            return Err("NO_SFU_AVAILABLE".into());
        }

        let is_initiator = room.members.is_empty();
        room.members.insert(
            client_id,
            Member {
                token: admission_token.clone(),
                is_initiator,
                registered: false,
                reconnect_deadline: Some(now + self.reconnect_grace),
                queued_messages: VecDeque::new(),
            },
        );
        Ok(Admission {
            signal_epoch: room.signal_epoch,
            admission_token,
            is_initiator,
        })
    }

    pub fn register(
        &mut self,
        room_id: RoomId,
        client_id: ClientId,
        token: &str,
    ) -> Result<Registration, String> {
        let room = self.rooms.get_mut(&room_id).ok_or("UNAUTHORIZED")?;
        let is_initiator = {
            let member = room.members.get_mut(&client_id).ok_or("UNAUTHORIZED")?;
            if !constant_time_eq(member.token.as_bytes(), token.as_bytes()) {
                return Err("UNAUTHORIZED".into());
            }
            if member.registered {
                return Err("Duplicated registration".into());
            }
            member.registered = true;
            member.reconnect_deadline = None;
            member.is_initiator
        };

        let mut queued_messages = Vec::new();
        for (&other_id, other) in &mut room.members {
            if other_id != client_id {
                queued_messages.extend(other.queued_messages.drain(..));
            }
        }
        Ok(Registration {
            signal_epoch: room.signal_epoch,
            is_initiator,
            queued_messages,
        })
    }

    pub fn deregister(&mut self, now: Instant, room_id: RoomId, client_id: ClientId) {
        if let Some(member) = self
            .rooms
            .get_mut(&room_id)
            .and_then(|room| room.members.get_mut(&client_id))
            && member.registered
        {
            member.registered = false;
            member.reconnect_deadline = Some(now + self.reconnect_grace);
        }
    }

    pub fn send(
        &mut self,
        room_id: RoomId,
        client_id: ClientId,
        signal_epoch: u64,
        message: String,
    ) -> Result<Option<Delivery>, String> {
        let room = self.rooms.get_mut(&room_id).ok_or("CLIENT_NOT_FOUND")?;
        if room.signal_epoch != signal_epoch {
            return Ok(None);
        }
        if !room.members.contains_key(&client_id) {
            return Err("CLIENT_NOT_FOUND".into());
        }
        let peer_id = room.members.keys().find(|&&id| id != client_id).copied();
        let Some(peer_id) = peer_id else {
            let source = room.members.get_mut(&client_id).expect("member checked");
            if source.queued_messages.len() >= MAX_QUEUED_MESSAGES {
                return Err("RESOURCE_EXHAUSTED".into());
            }
            source.queued_messages.push_back(message);
            return Ok(None);
        };
        if room
            .members
            .get(&peer_id)
            .is_some_and(|peer| peer.registered)
        {
            return Ok(Some(Delivery {
                room_id,
                client_id: peer_id,
                signal_epoch,
                message,
            }));
        }
        let source = room.members.get_mut(&client_id).expect("member checked");
        if source.queued_messages.len() >= MAX_QUEUED_MESSAGES {
            return Err("RESOURCE_EXHAUSTED".into());
        }
        source.queued_messages.push_back(message);
        Ok(None)
    }

    pub fn remove(
        &mut self,
        room_id: RoomId,
        client_id: ClientId,
        token: &str,
    ) -> Result<Option<Promotion>, String> {
        let room = self.rooms.get_mut(&room_id).ok_or("ROOM_NOT_FOUND")?;
        let member = room.members.get(&client_id).ok_or("CLIENT_NOT_FOUND")?;
        if !constant_time_eq(member.token.as_bytes(), token.as_bytes()) {
            return Err("UNAUTHORIZED".into());
        }
        room.members.remove(&client_id);
        let promotion = promote_survivor(room_id, room);
        if room.members.is_empty() {
            self.rooms.remove(&room_id);
        }
        Ok(promotion)
    }

    pub fn occupancy(&self, room_id: RoomId) -> usize {
        self.rooms
            .get(&room_id)
            .map_or(0, |room| room.members.len())
    }

    pub fn room_count(&self) -> usize {
        self.rooms.len()
    }

    pub fn member_count(&self) -> usize {
        self.rooms.values().map(|room| room.members.len()).sum()
    }

    pub fn registered_count(&self) -> usize {
        self.rooms
            .values()
            .flat_map(|room| room.members.values())
            .filter(|member| member.registered)
            .count()
    }

    pub fn handle_timeout(&mut self, now: Instant) -> Vec<Promotion> {
        let mut promotions = Vec::new();
        let room_ids = self.rooms.keys().copied().collect::<Vec<_>>();
        for room_id in room_ids {
            let Some(room) = self.rooms.get_mut(&room_id) else {
                continue;
            };
            let expired = room
                .members
                .iter()
                .filter_map(|(&client_id, member)| {
                    member
                        .reconnect_deadline
                        .filter(|&deadline| deadline <= now)
                        .map(|_| client_id)
                })
                .collect::<Vec<_>>();
            if expired.is_empty() {
                continue;
            }
            for client_id in expired {
                room.members.remove(&client_id);
            }
            if let Some(promotion) = promote_survivor(room_id, room) {
                promotions.push(promotion);
            }
            if room.members.is_empty() {
                self.rooms.remove(&room_id);
            }
        }
        promotions
    }

    pub fn poll_timeout(&self) -> Option<Instant> {
        self.rooms
            .values()
            .flat_map(|room| room.members.values())
            .filter_map(|member| member.reconnect_deadline)
            .min()
    }

    pub fn clear(&mut self) {
        self.rooms.clear();
    }
}

fn promote_survivor(room_id: RoomId, room: &mut Room) -> Option<Promotion> {
    if room.members.len() != 1 {
        return None;
    }
    let (&client_id, member) = room.members.iter_mut().next()?;
    member.is_initiator = true;
    member.queued_messages.clear();
    member.registered.then_some(Promotion {
        room_id,
        client_id,
        signal_epoch: room.signal_epoch,
    })
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (&left, &right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_registration_relay_and_promotion() {
        let now = Instant::now();
        let mut rooms = RoomTable::new(Duration::from_secs(10));
        let first = rooms.admit(now, 42, 101, "token-1".into()).unwrap();
        let second = rooms.admit(now, 42, 102, "token-2".into()).unwrap();
        assert!(first.is_initiator);
        assert!(!second.is_initiator);
        assert_ne!(first.admission_token, second.admission_token);
        assert_eq!(
            rooms.admit(now, 42, 103, "token-3".into()),
            Err("NO_SFU_AVAILABLE".into())
        );

        rooms.register(42, 101, &first.admission_token).unwrap();
        rooms.register(42, 102, &second.admission_token).unwrap();
        let delivery = rooms.send(42, 101, 0, "candidate".into()).unwrap();
        assert_eq!(delivery.unwrap().client_id, 102);
        assert!(rooms.send(42, 101, 1, "stale".into()).unwrap().is_none());

        let promotion = rooms
            .remove(42, 102, &second.admission_token)
            .unwrap()
            .unwrap();
        assert_eq!(promotion.client_id, 101);
        assert_eq!(rooms.occupancy(42), 1);
    }

    #[test]
    fn token_binding_and_reconnect_expiry_are_enforced() {
        let now = Instant::now();
        let mut rooms = RoomTable::new(Duration::from_secs(10));
        let admission = rooms.admit(now, 7, 8, "token".into()).unwrap();
        assert_eq!(rooms.register(7, 8, "wrong"), Err("UNAUTHORIZED".into()));
        rooms.register(7, 8, &admission.admission_token).unwrap();
        rooms.deregister(now, 7, 8);
        rooms
            .register(7, 8, &admission.admission_token)
            .expect("member may reconnect within grace");
        rooms.deregister(now, 7, 8);
        rooms.handle_timeout(now + Duration::from_secs(10));
        assert_eq!(rooms.occupancy(7), 0);
        assert_eq!(
            rooms.register(7, 8, &admission.admission_token),
            Err("UNAUTHORIZED".into())
        );
    }
}
