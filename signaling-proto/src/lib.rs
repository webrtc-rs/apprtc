#![warn(rust_2018_idioms)]

use prost::Message;

pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/signaling.v1.rs"));
}

pub use v1::{Request, Response, Result as ResultCode};

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

    pub fn register(requestid: u64, appid: String, token: String) -> Self {
        Self {
            requestid,
            command: Some(v1::request::Command::App(v1::Register { appid, token })),
        }
    }

    pub fn admit(requestid: u64, roomid: String, clientid: String, is_loopback: bool) -> Self {
        Self {
            requestid,
            command: Some(v1::request::Command::Admit(v1::Admit {
                roomid,
                clientid,
                is_loopback,
            })),
        }
    }

    pub fn remove(requestid: u64, roomid: String, clientid: String) -> Self {
        Self {
            requestid,
            command: Some(v1::request::Command::Remove(v1::Remove {
                roomid,
                clientid,
            })),
        }
    }

    pub fn occupancy(requestid: u64, roomid: String) -> Self {
        Self {
            requestid,
            command: Some(v1::request::Command::Occupancy(v1::Occupancy { roomid })),
        }
    }

    pub fn inject(requestid: u64, roomid: String, clientid: String, msg: String) -> Self {
        Self {
            requestid,
            command: Some(v1::request::Command::Inject(v1::Inject {
                roomid,
                clientid,
                msg,
            })),
        }
    }

    pub fn status(requestid: u64) -> Self {
        Self {
            requestid,
            command: Some(v1::request::Command::Status(v1::StatusRequest {})),
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

    pub fn ok(requestid: u64) -> Self {
        Self {
            requestid,
            result: ResultCode::Ok.into(),
            reason: String::new(),
            payload: None,
        }
    }

    pub fn err(requestid: u64, reason: impl Into<String>) -> Self {
        Self {
            requestid,
            result: ResultCode::Err.into(),
            reason: reason.into(),
            payload: None,
        }
    }

    pub fn result_name(&self) -> &'static str {
        match ResultCode::try_from(self.result) {
            Ok(ResultCode::Ok) => "OK",
            Ok(ResultCode::Err) => "ERR",
            Ok(ResultCode::Unspecified) | Err(_) => "UNSPECIFIED",
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
        assert_eq!(decoded.operation_name(), "admit");

        let response = Response::err(7, "FULL");
        let decoded = Response::decode_wire(&response.encode_wire()).unwrap();
        assert_eq!(decoded, response);
        assert_eq!(decoded.result_name(), "ERR");
    }
}
