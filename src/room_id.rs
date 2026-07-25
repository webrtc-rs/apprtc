//! V2 room identifiers on the wire.
//!
//! One value has three renderings and each boundary uses exactly one of them:
//!
//! | Boundary                        | Rendering                                  |
//! |---------------------------------|--------------------------------------------|
//! | Room links, browser JSON        | base64url token, 22 characters (`signaling::v2`) |
//! | gRPC to signaling and workers   | the raw 16 bytes                            |
//! | ICE ufrag inside the SFU        | 32 hex characters (`Uuid::simple`)          |
//!
//! Keeping them straight matters: a room is keyed by the value received, so a second
//! spelling reaching the authority would become a second room.

use signaling::v2::{RoomId, is_room_uuid};

/// The 16 bytes carried by a V2 `bytes room_id` field.
pub fn room_id_to_bytes(room_id: &RoomId) -> Vec<u8> {
    room_id.as_bytes().to_vec()
}

/// Read a V2 `bytes room_id` field back, rejecting anything that is not exactly the 16
/// bytes of a UUIDv8. Both processes validate: the edge has already checked the token it
/// decoded, and signaling checks again rather than trusting its caller.
pub fn room_id_from_bytes(bytes: &[u8]) -> Option<RoomId> {
    let bytes: [u8; 16] = bytes.try_into().ok()?;
    let room_id = RoomId::from_bytes(bytes);
    is_room_uuid(&room_id).then_some(room_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use signaling::v2::new_room_id;

    #[test]
    fn room_ids_round_trip_through_the_wire_form() {
        let room_id = new_room_id();
        assert_eq!(
            room_id_from_bytes(&room_id_to_bytes(&room_id)),
            Some(room_id)
        );
    }

    #[test]
    fn wire_form_rejects_wrong_length_and_non_v8_uuids() {
        let room_id = new_room_id();
        let bytes = room_id_to_bytes(&room_id);
        assert_eq!(room_id_from_bytes(&[]), None);
        assert_eq!(room_id_from_bytes(&bytes[..15]), None);
        assert_eq!(room_id_from_bytes(&[bytes.clone(), vec![0]].concat()), None);
        // Well-formed UUID, wrong version: not something this service minted.
        assert_eq!(room_id_from_bytes(uuid::Uuid::new_v4().as_bytes()), None);
    }
}
