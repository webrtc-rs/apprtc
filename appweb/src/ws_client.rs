//! AppWeb's client for the signaling room authority.
//!
//! The initial supported deployment is all-in-one, so this adapter calls the
//! Collider owner task directly. Its operation/result boundary is transport
//! independent and can later be carried by the control WebSocket unchanged.

use signaling::collider::{AuthorityOperation, AuthorityResult, StatusSnapshot};
use signaling::ws_server::ColliderHandle;
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    pub is_initiator: bool,
    pub messages: Vec<String>,
}

#[derive(Clone)]
pub struct WsClient {
    collider: ColliderHandle,
}

impl WsClient {
    pub fn new(collider: ColliderHandle) -> Self {
        Self { collider }
    }

    pub async fn admit(
        &self,
        roomid: String,
        clientid: String,
        is_loopback: bool,
    ) -> Result<Admission, String> {
        match self
            .collider
            .request(AuthorityOperation::Admit {
                roomid,
                clientid,
                is_loopback,
                now: Instant::now(),
            })
            .await?
        {
            AuthorityResult::Admitted {
                is_initiator,
                messages,
            } => Ok(Admission {
                is_initiator,
                messages,
            }),
            AuthorityResult::Error { result } => Err(result),
            _ => Err("unexpected signaling authority response to admit".to_string()),
        }
    }

    pub async fn remove(&self, roomid: String, clientid: String) -> Result<(), String> {
        match self
            .collider
            .request(AuthorityOperation::Remove { roomid, clientid })
            .await?
        {
            AuthorityResult::Removed => Ok(()),
            AuthorityResult::Error { result } => Err(result),
            _ => Err("unexpected signaling authority response to remove".to_string()),
        }
    }

    pub async fn occupancy(&self, roomid: String) -> Result<usize, String> {
        match self
            .collider
            .request(AuthorityOperation::Occupancy { roomid })
            .await?
        {
            AuthorityResult::Occupancy { count } => Ok(count),
            AuthorityResult::Error { result } => Err(result),
            _ => Err("unexpected signaling authority response to occupancy".to_string()),
        }
    }

    pub async fn inject(
        &self,
        roomid: String,
        clientid: String,
        msg: String,
    ) -> Result<(), String> {
        match self
            .collider
            .request(AuthorityOperation::Inject {
                roomid,
                clientid,
                msg,
                now: Instant::now(),
            })
            .await?
        {
            AuthorityResult::Injected => Ok(()),
            AuthorityResult::Error { result } => Err(result),
            _ => Err("unexpected signaling authority response to inject".to_string()),
        }
    }

    pub async fn status(&self) -> Result<StatusSnapshot, String> {
        match self.collider.request(AuthorityOperation::Status).await? {
            AuthorityResult::Status(status) => Ok(status),
            AuthorityResult::Error { result } => Err(result),
            _ => Err("unexpected signaling authority response to status".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn client() -> WsClient {
        WsClient::new(ColliderHandle::spawn(Duration::from_secs(10)))
    }

    #[tokio::test]
    async fn admit_inject_and_occupancy_round_trip() {
        let authority = client();
        let first = authority
            .admit("room".into(), "client-a".into(), false)
            .await
            .unwrap();
        assert!(first.is_initiator);
        assert!(first.messages.is_empty());
        authority
            .inject("room".into(), "client-a".into(), "offer".into())
            .await
            .unwrap();
        assert_eq!(authority.occupancy("room".into()).await.unwrap(), 1);
        let second = authority
            .admit("room".into(), "client-b".into(), false)
            .await
            .unwrap();
        assert!(!second.is_initiator);
        assert_eq!(second.messages, vec!["offer"]);
    }

    #[tokio::test]
    async fn remove_releases_capacity_and_status_is_available() {
        let authority = client();
        authority
            .admit("room".into(), "client-a".into(), false)
            .await
            .unwrap();
        authority
            .remove("room".into(), "client-a".into())
            .await
            .unwrap();
        assert_eq!(authority.occupancy("room".into()).await.unwrap(), 0);
        let status = authority.status().await.unwrap();
        assert!(status.rooms <= 1);
        assert!(status.total_websocket_connections <= 1);
    }

    #[tokio::test]
    async fn duplicate_admission_returns_authority_error() {
        let authority = client();
        authority
            .admit("room".into(), "client-a".into(), false)
            .await
            .unwrap();
        let error = authority
            .admit("room".into(), "client-a".into(), false)
            .await
            .unwrap_err();
        assert_eq!(error, "DUPLICATE_CLIENT");
    }

    #[tokio::test]
    async fn all_operations_report_transport_error_after_collider_shutdown() {
        let authority = client();
        authority.collider.shutdown().await.unwrap();
        assert!(
            authority
                .admit("room".into(), "client".into(), false)
                .await
                .is_err()
        );
        assert!(
            authority
                .remove("room".into(), "client".into())
                .await
                .is_err()
        );
        assert!(authority.occupancy("room".into()).await.is_err());
        assert!(
            authority
                .inject("room".into(), "client".into(), "msg".into())
                .await
                .is_err()
        );
        assert!(authority.status().await.is_err());
    }
}
