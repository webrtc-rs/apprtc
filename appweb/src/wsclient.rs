// Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

//! AppWeb's client for the signaling room authority.
//!
//! The initial supported deployment is all-in-one, so this adapter calls the
//! Collider owner task directly. Its operation/result boundary is transport
//! independent and can later be carried by the control WebSocket unchanged.

use signaling::collider::{AuthorityOperation, AuthorityResult, StatusSnapshot};
use signaling::wsserver::ColliderHandle;
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
