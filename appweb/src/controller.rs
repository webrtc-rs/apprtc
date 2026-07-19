//! Pure decision logic for the signaling control client.
//!
//! Everything here is deterministic over wire values — no sockets, no clock, no
//! randomness — so the control protocol's decisions are unit-testable in
//! isolation. Two objects carry the API:
//!
//! - [`Controller`] owns the client-side protocol state (heartbeat sequence,
//!   reconnect backoff bookkeeping) and namespaces the stateless frame decisions
//!   (classification, response correlation, registration acknowledgement).
//! - [`ControlResponseExt`] puts the response→domain conversions on
//!   [`AppControlResponse`] itself, so the request side reads
//!   `request(…).await?.admission()`.
//!
//! The I/O driver that moves the frames lives in [`crate::ws_client`].

use signaling::collider::StatusSnapshot;
use signaling::messages::AppControlResponse;
use std::time::Duration;
use tokio_tungstenite::tungstenite::{Bytes, Message};

const RECONNECT_MIN_DELAY: Duration = Duration::from_secs(1);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);

/// A successful room admission, as the HTTP layer consumes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    pub is_initiator: bool,
    pub messages: Vec<String>,
}

/// What one control-channel frame means to whoever is waiting on the socket.
#[derive(Debug, PartialEq)]
pub(crate) enum FrameAction {
    /// A JSON text payload — a control response, for [`Controller::correlate_response`].
    Text(String),
    /// The peer acknowledged our ping (any pong counts as the heartbeat ack).
    Pong,
    /// The peer pinged us; answer with this payload.
    Ping(Bytes),
    /// The peer closed the socket.
    Closed,
    /// Frame types with no meaning on the control channel.
    Ignore,
}

/// Client-side state of one control connection: which heartbeat we are on and how
/// far the reconnect backoff has escalated. The driver owns exactly one per
/// connection task and calls the stateless frame decisions through the same type.
#[derive(Debug, Default)]
pub(crate) struct Controller {
    heartbeat_sequence: u64,
    reconnect_attempt: u32,
}

impl Controller {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    // ── stateless frame decisions ──

    pub(crate) fn classify_frame(frame: Message) -> FrameAction {
        match frame {
            Message::Text(text) => FrameAction::Text(text.to_string()),
            Message::Pong(_) => FrameAction::Pong,
            Message::Ping(payload) => FrameAction::Ping(payload),
            Message::Close(_) => FrameAction::Closed,
            Message::Binary(_) | Message::Frame(_) => FrameAction::Ignore,
        }
    }

    /// Decode a control response and match it against the outstanding request.
    /// `Ok(Some)` is our response, `Ok(None)` is a stale response for an earlier request
    /// (keep waiting), `Err` is an undecodable frame, which fails the request.
    pub(crate) fn correlate_response(
        text: &str,
        request_id: u64,
    ) -> Result<Option<AppControlResponse>, String> {
        let response = AppControlResponse::from_wire(text)?;
        Ok((response.requestid == request_id).then_some(response))
    }

    /// Registration uses the same response envelope as other control operations.
    pub(crate) fn registration_ack(frame: &str, expected_requestid: u64) -> Result<(), String> {
        let response = AppControlResponse::from_wire(frame)?;
        let registered =
            response.result.as_deref() == Some("OK") && response.requestid == expected_requestid;
        if registered {
            Ok(())
        } else {
            Err(format!("signaling control registration failed: {frame}"))
        }
    }

    // ── stateful sequencing ──

    /// Advance to the next heartbeat and return its sequence number.
    pub(crate) fn next_heartbeat(&mut self) -> u64 {
        self.heartbeat_sequence = self.heartbeat_sequence.wrapping_add(1);
        self.heartbeat_sequence
    }

    /// Record one more reconnect attempt and return `(attempt, delay)`: an
    /// exponential backoff of 1 s doubling per attempt to a 30 s cap (the exponent
    /// saturates at attempt 6), plus caller-supplied jitter, with the jittered
    /// total never exceeding the cap.
    pub(crate) fn schedule_reconnect(&mut self, jitter: Duration) -> (u32, Duration) {
        self.reconnect_attempt = self.reconnect_attempt.saturating_add(1);
        let exponent = self.reconnect_attempt.saturating_sub(1).min(5);
        let base_delay = (RECONNECT_MIN_DELAY * 2_u32.pow(exponent)).min(RECONNECT_MAX_DELAY);
        let delay = (base_delay + jitter).min(RECONNECT_MAX_DELAY);
        (self.reconnect_attempt, delay)
    }

    /// A successful reconnect resets the backoff escalation.
    pub(crate) fn reconnected(&mut self) {
        self.reconnect_attempt = 0;
    }
}

/// Response→domain conversions, as methods on the response itself.
pub(crate) trait ControlResponseExt: Sized {
    /// Every successful response carries `result: "OK"`; any other result
    /// is the operation's error, and `fallback` covers a malformed omission.
    fn ack(self, fallback: &str) -> Result<Self, String>;
    fn admission(self) -> Result<Admission, String>;
    fn occupancy_count(self) -> Result<usize, String>;
    fn status_snapshot(self) -> Result<StatusSnapshot, String>;
}

impl ControlResponseExt for AppControlResponse {
    fn ack(self, fallback: &str) -> Result<Self, String> {
        match self.result.as_deref() {
            Some("OK") => Ok(self),
            Some("ERR") => Err(self.reason.unwrap_or_else(|| fallback.to_string())),
            Some(result) => Err(format!("invalid control result: {result}")),
            None => Err(fallback.to_string()),
        }
    }

    fn admission(self) -> Result<Admission, String> {
        let response = self.ack("admission failed")?;
        Ok(Admission {
            is_initiator: response.is_initiator.unwrap_or(false),
            messages: response.messages.unwrap_or_default(),
        })
    }

    fn occupancy_count(self) -> Result<usize, String> {
        let response = self.ack("occupancy failed")?;
        match response.count {
            Some(count) => Ok(count),
            None => Err("occupancy failed".into()),
        }
    }

    fn status_snapshot(self) -> Result<StatusSnapshot, String> {
        let response = self.ack("status failed")?;
        Ok(StatusSnapshot {
            rooms: response.rooms.unwrap_or(0),
            clients: response.clients.unwrap_or(0),
            websocket_connections: response.websocket_connections.unwrap_or(0),
            total_websocket_connections: response.total_websocket_connections.unwrap_or(0),
            websocket_errors: response.websocket_errors.unwrap_or(0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_from_one_second_and_caps_at_thirty() {
        let mut controller = Controller::new();
        assert_eq!(
            controller.schedule_reconnect(Duration::ZERO),
            (1, Duration::from_secs(1))
        );
        assert_eq!(
            controller.schedule_reconnect(Duration::ZERO),
            (2, Duration::from_secs(2))
        );
        for _ in 3..=5 {
            controller.schedule_reconnect(Duration::ZERO);
        }
        // Attempt 6 would be 32 s; the cap holds it at 30 s, and stays there.
        assert_eq!(
            controller.schedule_reconnect(Duration::ZERO),
            (6, Duration::from_secs(30))
        );
        assert_eq!(
            controller.schedule_reconnect(Duration::from_millis(249)),
            (7, Duration::from_secs(30))
        );
    }

    #[test]
    fn backoff_resets_after_a_successful_reconnect_and_jitters_below_the_cap() {
        let mut controller = Controller::new();
        controller.schedule_reconnect(Duration::ZERO);
        controller.schedule_reconnect(Duration::ZERO);
        controller.reconnected();
        // The escalation starts over, and jitter is added below the cap.
        assert_eq!(
            controller.schedule_reconnect(Duration::from_millis(249)),
            (1, Duration::from_millis(1249))
        );
    }

    #[test]
    fn heartbeat_sequence_increments_and_wraps() {
        let mut controller = Controller::new();
        assert_eq!(controller.next_heartbeat(), 1);
        assert_eq!(controller.next_heartbeat(), 2);
        controller.heartbeat_sequence = u64::MAX;
        assert_eq!(controller.next_heartbeat(), 0);
    }

    #[test]
    fn replies_are_correlated_by_request_id() {
        let matched =
            Controller::correlate_response(r#"{"requestid":"5","result":"OK"}"#, 5).unwrap();
        assert_eq!(matched.unwrap().requestid, 5);
        // A stale response for an earlier request is skipped, not an error.
        assert!(
            Controller::correlate_response(r#"{"requestid":"4","result":"OK"}"#, 5)
                .unwrap()
                .is_none()
        );
        // An undecodable frame fails the request.
        assert!(Controller::correlate_response("not json", 5).is_err());
    }

    #[test]
    fn registration_requires_the_registered_control_frame() {
        assert!(Controller::registration_ack(r#"{"requestid":"1","result":"OK"}"#, 1).is_ok());
        assert!(Controller::registration_ack(r#"{"requestid":"2","result":"OK"}"#, 1).is_err());
        assert!(Controller::registration_ack("garbage", 1).is_err());
    }

    #[test]
    fn control_frames_classify_by_what_the_waiter_should_do() {
        assert_eq!(
            Controller::classify_frame(Message::text(r#"{"requestid":"1","result":"OK"}"#)),
            FrameAction::Text(r#"{"requestid":"1","result":"OK"}"#.into())
        );
        assert_eq!(
            Controller::classify_frame(Message::Pong(Bytes::from_static(b"1"))),
            FrameAction::Pong
        );
        assert_eq!(
            Controller::classify_frame(Message::Ping(Bytes::from_static(b"2"))),
            FrameAction::Ping(Bytes::from_static(b"2"))
        );
        assert_eq!(
            Controller::classify_frame(Message::Close(None)),
            FrameAction::Closed
        );
        assert_eq!(
            Controller::classify_frame(Message::Binary(Bytes::from_static(b"x"))),
            FrameAction::Ignore
        );
    }

    #[test]
    fn error_replies_map_to_their_result_or_the_fallback() {
        let error = AppControlResponse {
            requestid: 1,
            result: Some("ERR".into()),
            reason: Some("FULL".into()),
            ..Default::default()
        };
        assert_eq!(error.admission().unwrap_err(), "FULL");

        let bare_error = AppControlResponse {
            requestid: 1,
            result: Some("ERR".into()),
            ..Default::default()
        };
        assert_eq!(bare_error.admission().unwrap_err(), "admission failed");

        let admitted = AppControlResponse {
            requestid: 1,
            result: Some("OK".into()),
            is_initiator: Some(true),
            messages: Some(vec!["offer".into()]),
            ..Default::default()
        };
        assert_eq!(
            admitted.admission().unwrap(),
            Admission {
                is_initiator: true,
                messages: vec!["offer".into()],
            }
        );
    }

    #[test]
    fn occupancy_and_status_replies_convert_with_defaults() {
        let counted = AppControlResponse {
            requestid: 1,
            result: Some("OK".into()),
            count: Some(2),
            ..Default::default()
        };
        assert_eq!(counted.occupancy_count().unwrap(), 2);
        let missing = AppControlResponse {
            requestid: 1,
            result: Some("OK".into()),
            ..Default::default()
        };
        assert_eq!(missing.occupancy_count().unwrap_err(), "occupancy failed");

        let snapshot = AppControlResponse {
            requestid: 1,
            result: Some("OK".into()),
            rooms: Some(1),
            clients: Some(2),
            ..Default::default()
        }
        .status_snapshot()
        .unwrap();
        assert_eq!(snapshot.rooms, 1);
        assert_eq!(snapshot.clients, 2);
        assert_eq!(snapshot.websocket_connections, 0);
        assert_eq!(snapshot.total_websocket_connections, 0);
    }
}
