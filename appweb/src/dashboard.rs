// Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

use serde::Serialize;
use std::time::Instant;

/// Server counters and uptime for the `/status` endpoint.
///
/// Sans-IO: the mutex that guarded the Go `dashboard` is gone (the owning run
/// loop serializes access), and the clock is caller-supplied — `new` records a
/// start `Instant` and `get_report` computes uptime from the `now` it is given,
/// rather than reading the wall clock itself.
#[derive(Debug, Clone)]
pub struct Dashboard {
    start_time: Instant,

    total_ws: u64,
    total_recv_msgs: u64,
    total_send_msgs: u64,
    ws_errs: u64,
    http_errs: u64,
}

/// The JSON body served by `/status`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct StatusReport {
    #[serde(rename = "upsec")]
    pub up_time_sec: f64,
    #[serde(rename = "openws")]
    pub open_ws: u64,
    #[serde(rename = "totalws")]
    pub total_ws: u64,
    #[serde(rename = "wserrors")]
    pub ws_errs: u64,
    #[serde(rename = "httperrors")]
    pub http_errs: u64,
}

impl Dashboard {
    /// `now` is the process start instant, supplied by the caller.
    pub fn new(now: Instant) -> Self {
        Self {
            start_time: now,
            total_ws: 0,
            total_recv_msgs: 0,
            total_send_msgs: 0,
            ws_errs: 0,
            http_errs: 0,
        }
    }

    /// Build a status report. `open_ws` is the current live WebSocket count,
    /// supplied by the caller (in Go this came from `roomTable.wsCount()`, which
    /// now lives in the `signaling` crate).
    pub fn get_report(&self, now: Instant, open_ws: u64) -> StatusReport {
        StatusReport {
            up_time_sec: now.duration_since(self.start_time).as_secs_f64(),
            open_ws,
            total_ws: self.total_ws,
            ws_errs: self.ws_errs,
            http_errs: self.http_errs,
        }
    }

    pub fn incr_ws(&mut self) {
        self.total_ws += 1;
    }

    pub fn on_ws_err(&mut self) {
        self.ws_errs += 1;
    }

    pub fn on_http_err(&mut self) {
        self.http_errs += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dashboard_ws_count() {
        let now = Instant::now();
        let mut db = Dashboard::new(now);

        // The live WebSocket count is supplied by the caller (was
        // `roomTable.wsCount()`); the dashboard only tracks the running total.
        let r = db.get_report(now, 0);
        assert_eq!(
            r.open_ws, 0,
            "get_report().open_ws is {}, want 0",
            r.open_ws
        );
        assert_eq!(
            r.total_ws, 0,
            "get_report().total_ws is {}, want 0",
            r.total_ws
        );

        db.incr_ws();
        let r = db.get_report(now, 0);
        assert_eq!(
            r.open_ws, 0,
            "get_report().open_ws is {}, want 0",
            r.open_ws
        );
        assert_eq!(
            r.total_ws, 1,
            "get_report().total_ws is {}, want 1",
            r.total_ws
        );

        // Registering a WebSocket bumps the caller-supplied live count (mirrors
        // the Go test registering a mock connection on the room table).
        let r = db.get_report(now, 1);
        assert_eq!(
            r.open_ws, 1,
            "get_report().open_ws is {}, want 1",
            r.open_ws
        );
    }

    #[test]
    fn dashboard_ws_err() {
        let now = Instant::now();
        let mut db = Dashboard::new(now);

        let r = db.get_report(now, 0);
        assert_eq!(
            r.ws_errs, 0,
            "get_report().ws_errs is {}, want 0",
            r.ws_errs
        );

        db.on_ws_err();
        let r = db.get_report(now, 0);
        assert_eq!(
            r.ws_errs, 1,
            "get_report().ws_errs is {}, want 1",
            r.ws_errs
        );
    }

    #[test]
    fn dashboard_http_err() {
        let now = Instant::now();
        let mut db = Dashboard::new(now);

        let r = db.get_report(now, 0);
        assert_eq!(
            r.http_errs, 0,
            "get_report().http_errs is {}, want 0",
            r.http_errs
        );

        db.on_http_err();
        let r = db.get_report(now, 0);
        assert_eq!(
            r.http_errs, 1,
            "get_report().http_errs is {}, want 1",
            r.http_errs
        );
    }
}
