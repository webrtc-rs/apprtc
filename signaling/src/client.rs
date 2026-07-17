// Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

use std::collections::VecDeque;
use std::time::Instant;

const MAX_QUEUED_MSG_COUNT: usize = 1024;

/// One participant of a room.
///
/// Sans-IO: the Go `io.ReadWriteCloser` (`rwc`) is gone — the hub owns the real
/// socket and drains [`Client::poll_outbound`], so "registered" is just a flag
/// and outgoing messages accumulate in `outbound` instead of being written to a
/// socket. The `time.Timer` becomes a caller-supplied deadline the hub polls.
pub struct Client {
    id: String,
    /// Whether a connection is currently registered (was `rwc != nil`).
    registered: bool,
    /// Messages this client sent that are queued until the peer registers
    /// (`c.msgs` in Go).
    msgs: VecDeque<String>,
    /// Server->client messages ready for the driver to write to this client's
    /// socket (replaces the direct `sendServerMsg(rwc, …)` writes). Each entry is
    /// a raw relay payload, framed as a `{ "msg": … }` server frame on the wire.
    outbound: VecDeque<String>,
    /// The register-timeout deadline: the hub reaps the client if it is still
    /// unregistered at this instant (was `timer *time.Timer`). `None` once
    /// registered or when no timeout is pending.
    timeout: Option<Instant>,
    /// True for the first client to join a room (the WebRTC caller). Folded in
    /// from `apprtc.py` `Client.is_initiator`.
    is_initiator: bool,
}

impl Client {
    /// `timeout` is the register-timeout deadline (was the `*time.Timer` argument).
    pub fn new(id: String, timeout: Option<Instant>) -> Self {
        Self {
            id,
            registered: false,
            msgs: VecDeque::new(),
            outbound: VecDeque::new(),
            timeout,
            is_initiator: false,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn is_initiator(&self) -> bool {
        self.is_initiator
    }

    pub fn set_initiator(&mut self, is_initiator: bool) {
        self.is_initiator = is_initiator;
    }

    /// The pending register-timeout deadline, if any (for the hub's `poll_timeout`).
    pub fn timeout(&self) -> Option<Instant> {
        self.timeout
    }

    /// Replace the register-timeout deadline. The previous one is simply dropped
    /// (there is no live `time.Timer` to `Stop` in sans-IO).
    pub fn set_timer(&mut self, timeout: Option<Instant>) {
        self.timeout = timeout;
    }

    /// Bind a connection to the client if one is not already registered.
    pub fn register(&mut self) -> Result<(), String> {
        if self.registered {
            return Err("Duplicated registration".to_string());
        }
        self.set_timer(None);
        self.registered = true;
        Ok(())
    }

    /// Drop the client's connection registration. The hub closes the underlying
    /// socket (the Go `rwc.Close()`); the queued `msgs` are kept so a client
    /// roaming between networks can reconnect.
    pub fn deregister(&mut self) {
        self.registered = false;
    }

    pub fn registered(&self) -> bool {
        self.registered
    }

    /// Add a message to this client's pending-flush queue.
    pub fn enqueue(&mut self, msg: String) -> Result<(), String> {
        if self.msgs.len() >= MAX_QUEUED_MSG_COUNT {
            return Err("Too many messages queued for the client".to_string());
        }
        self.msgs.push_back(msg);
        Ok(())
    }

    /// Queue a raw payload for delivery to this client (was `sendServerMsg(rwc, …)`).
    fn deliver(&mut self, msg: String) {
        self.outbound.push_back(msg);
    }

    /// Drain the next server->client message, or `None`. The hub frames it as a
    /// `{ "msg": … }` server frame and writes it to this client's socket.
    pub fn poll_outbound(&mut self) -> Option<String> {
        self.outbound.pop_front()
    }

    /// Flush this client's queued messages to `other` (which must be registered).
    pub fn send_queued(&mut self, other: &mut Client) -> Result<(), String> {
        if self.id == other.id || !other.registered {
            return Err("Invalid client".to_string());
        }
        for msg in self.msgs.drain(..) {
            other.deliver(msg);
        }
        Ok(())
    }

    /// Send `msg` to `other` if it has registered, otherwise queue it on this
    /// client until `other` registers.
    pub fn send(&mut self, other: &mut Client, msg: String) -> Result<(), String> {
        if self.id == other.id {
            return Err("Invalid client".to_string());
        }
        if other.registered {
            other.deliver(msg);
            Ok(())
        } else {
            self.enqueue(msg)
        }
    }
}
