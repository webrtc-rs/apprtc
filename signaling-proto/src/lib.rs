#![warn(rust_2018_idioms)]

use prost::Message;

pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/signaling.v1.rs"));
}

pub use v1::{Request, Response};

impl Request {
    pub fn decode_wire(bytes: &[u8]) -> Result<Self, String> {
        Self::decode(bytes).map_err(|error| error.to_string())
    }

    pub fn encode_wire(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    pub fn operation_name(&self) -> &'static str {
        use v1::request::Command;
        match self.command {
            Some(Command::App(_)) => "app",
            Some(Command::Admit(_)) => "admit",
            Some(Command::Remove(_)) => "remove",
            Some(Command::Occupancy(_)) => "occupancy",
            Some(Command::Inject(_)) => "inject",
            Some(Command::Status(_)) => "status",
            None => "missing",
        }
    }

    pub fn register(request_id: u64, app_id: String, token: String) -> Self {
        Self {
            request_id,
            command: Some(v1::request::Command::App(v1::request::Register {
                app_id,
                token,
            })),
        }
    }

    pub fn admit(request_id: u64, room_id: String, client_id: String, is_loopback: bool) -> Self {
        Self {
            request_id,
            command: Some(v1::request::Command::Admit(v1::request::Admit {
                room_id,
                client_id,
                is_loopback,
            })),
        }
    }

    pub fn remove(request_id: u64, room_id: String, client_id: String) -> Self {
        Self {
            request_id,
            command: Some(v1::request::Command::Remove(v1::request::Remove {
                room_id,
                client_id,
            })),
        }
    }

    pub fn occupancy(request_id: u64, room_id: String) -> Self {
        Self {
            request_id,
            command: Some(v1::request::Command::Occupancy(v1::request::Occupancy {
                room_id,
            })),
        }
    }

    pub fn inject(request_id: u64, room_id: String, client_id: String, msg: String) -> Self {
        Self {
            request_id,
            command: Some(v1::request::Command::Inject(v1::request::Inject {
                room_id,
                client_id,
                msg,
            })),
        }
    }

    pub fn status(request_id: u64) -> Self {
        Self {
            request_id,
            command: Some(v1::request::Command::Status(v1::request::StatusRequest {})),
        }
    }
}

impl Response {
    pub fn decode_wire(bytes: &[u8]) -> Result<Self, String> {
        Self::decode(bytes).map_err(|error| error.to_string())
    }

    pub fn encode_wire(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    pub fn ok(request_id: u64) -> Self {
        Self {
            request_id,
            result: Some(v1::response::Result::Ok(v1::response::Ok { payload: None })),
        }
    }

    pub fn ok_with_payload(request_id: u64, payload: v1::response::ok::Payload) -> Self {
        Self {
            request_id,
            result: Some(v1::response::Result::Ok(v1::response::Ok {
                payload: Some(payload),
            })),
        }
    }

    pub fn err(request_id: u64, reason: impl Into<String>) -> Self {
        Self {
            request_id,
            result: Some(v1::response::Result::Err(v1::response::Err {
                reason: reason.into(),
            })),
        }
    }

    pub fn is_ok(&self) -> bool {
        matches!(self.result, Some(v1::response::Result::Ok(_)))
    }

    pub fn reason(&self) -> &str {
        match &self.result {
            Some(v1::response::Result::Err(err)) => &err.reason,
            Some(v1::response::Result::Ok(_)) | None => "",
        }
    }

    pub fn result_name(&self) -> &'static str {
        match self.result {
            Some(v1::response::Result::Ok(_)) => "OK",
            Some(v1::response::Result::Err(_)) => "ERR",
            None => "MISSING",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_and_responses_round_trip_as_protobuf() {
        let request = Request::admit(7, "room".into(), "client".into(), true);
        let decoded = Request::decode_wire(&request.encode_wire()).unwrap();
        assert_eq!(decoded, request);
        assert_eq!(decoded.request_id, 7);
        assert_eq!(decoded.operation_name(), "admit");
        assert!(matches!(
            decoded.command,
            Some(v1::request::Command::Admit(v1::request::Admit {
                room_id,
                client_id,
                is_loopback: true,
            })) if room_id == "room" && client_id == "client"
        ));

        let response = Response::err(7, "FULL");
        let decoded = Response::decode_wire(&response.encode_wire()).unwrap();
        assert_eq!(decoded, response);
        assert_eq!(decoded.result_name(), "ERR");
        assert_eq!(decoded.reason(), "FULL");

        let response = Response::ok_with_payload(
            8,
            v1::response::ok::Payload::Occupancy(v1::response::OccupancyResult { count: 2 }),
        );
        let decoded = Response::decode_wire(&response.encode_wire()).unwrap();
        assert!(decoded.is_ok());
        assert!(matches!(
            decoded.result,
            Some(v1::response::Result::Ok(v1::response::Ok {
                payload: Some(v1::response::ok::Payload::Occupancy(
                    v1::response::OccupancyResult { count: 2 }
                )),
            }))
        ));
    }
}
