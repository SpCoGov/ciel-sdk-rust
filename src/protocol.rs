use crate::{Error, ErrorKind, Result};
use serde::{
    Deserialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Value};
use std::fmt;

pub(crate) fn input(code: &str) -> Error {
    Error::new(ErrorKind::Input, code)
}
pub(crate) fn bad() -> Error {
    Error::new(ErrorKind::Protocol, "INVALID_RESPONSE")
}
pub(crate) fn id(value: &str) -> Result<()> {
    if (3..=64).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        Ok(())
    } else {
        Err(input("INVALID_ID"))
    }
}
pub(crate) fn hex(value: &str, len: usize) -> Result<()> {
    if value.len() == len
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(())
    } else {
        Err(input("INVALID_IDENTITY"))
    }
}
pub(crate) fn address(server: &str) -> Result<()> {
    let uri: tokio_tungstenite::tungstenite::http::Uri = server
        .parse()
        .map_err(|_| input("INVALID_SERVER_ADDRESS"))?;
    if uri.scheme_str() != Some("wss")
        || uri.path() != "/ws/service"
        || uri.query().is_some()
        || uri.host().is_none()
        || uri.authority().is_none_or(|a| a.as_str().contains('@'))
        || uri.port_u16() == Some(0)
        || (uri.port().is_some() && uri.port_u16().is_none())
        || server.contains('#')
        || server.len() > 2048
    {
        return Err(input("INVALID_SERVER_ADDRESS"));
    }
    Ok(())
}
pub(crate) fn text(value: &str, max: usize, nonblank: bool) -> Result<()> {
    if value.len() > max || (nonblank && value.trim().is_empty()) {
        Err(input("INVALID_TEXT"))
    } else {
        Ok(())
    }
}
pub(crate) fn payload(value: &Value) -> Result<()> {
    if serde_json::to_vec(value)
        .map_err(|_| input("INVALID_PAYLOAD"))?
        .len()
        > 2048
    {
        return Err(input("PAYLOAD_TOO_LARGE"));
    }
    Ok(())
}
pub(crate) fn encode(value: &Value) -> Result<String> {
    let text = serde_json::to_string(value).map_err(|_| input("INVALID_PAYLOAD"))?;
    if text.len() > 4096 {
        return Err(input("MESSAGE_TOO_LARGE"));
    }
    Ok(text)
}
pub(crate) fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key].as_str().ok_or_else(bad)
}
pub(crate) fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(|_| bad())
}
pub(crate) fn server_error(value: &Value) -> Result<Error> {
    let code = string(value, "code")?;
    if code.is_empty()
        || code.len() > 64
        || !code.as_bytes()[0].is_ascii_uppercase()
        || !code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(bad());
    }
    let mut error = Error::new(ErrorKind::Server, code);
    error.retryable = matches!(
        code,
        "INSTANCE_ALREADY_CONNECTED"
            | "RATE_LIMITED"
            | "CONNECTION_TIMEOUT"
            | "WRITE_TIMEOUT"
            | "CONNECTION_CLOSED"
    );
    Ok(error)
}
pub(crate) fn expected(value: &Value, kind: &str) -> Result<()> {
    if value["type"] == "error" {
        return Err(server_error(value)?);
    }
    if value["type"] == kind {
        Ok(())
    } else {
        Err(bad())
    }
}
/// 生成 32 位小写十六进制请求 ID。命令和通知重试必须复用原 ID 和原参数。
pub fn new_request_id() -> Result<String> {
    use ring::rand::{SecureRandom, SystemRandom};
    let mut bytes = [0u8; 16];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| Error::new(ErrorKind::Storage, "RANDOM_UNAVAILABLE"))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

// serde_json::Value normally accepts duplicate keys. Reject them at every depth.
struct Strict(Value);
impl<'de> Deserialize<'de> for Strict {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Strict;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("JSON value")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> std::result::Result<Strict, E> {
                serde_json::Number::from_f64(v)
                    .map(|v| Strict(v.into()))
                    .ok_or_else(|| E::custom("invalid number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> std::result::Result<Strict, E> {
                Ok(Strict(v.into()))
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Strict, E> {
                Ok(Strict(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Strict, A::Error> {
                let mut values = Vec::new();
                while let Some(Strict(v)) = a.next_element()? {
                    values.push(v);
                }
                Ok(Strict(values.into()))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Strict, A::Error> {
                let mut values = Map::new();
                while let Some(key) = a.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate key"));
                    }
                    values.insert(key, a.next_value::<Strict>()?.0);
                }
                Ok(Strict(values.into()))
            }
        }
        d.deserialize_any(V)
    }
}
pub(crate) fn parse(text: &[u8]) -> Result<Value> {
    if text.len() > 32768 {
        return Err(bad());
    }
    serde_json::from_slice::<Strict>(text)
        .map(|v| v.0)
        .map_err(|_| bad())
}
pub(crate) fn response(text: &[u8]) -> Result<Value> {
    let value = parse(text)?;
    if value["v"].as_u64() != Some(1) {
        return Err(Error::new(ErrorKind::Protocol, "UNSUPPORTED_VERSION"));
    }
    string(&value, "type")?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn boundaries_and_redaction() {
        assert!(parse(br#"{"a":{"secret":"x","secret":"y"}}"#).is_err());
        assert_eq!(
            parse(b"password:secret").unwrap_err().to_string(),
            "INVALID_RESPONSE"
        );
        assert!(response(br#"{"v":1.0,"type":"event"}"#).is_err());
        assert!(address("wss://user@localhost/ws/service").is_err());
        assert!(address("wss://localhost:0/ws/service").is_err());
        assert!(hex(&"A".repeat(64), 64).is_err());
        assert!(text(&"好".repeat(44), 128, true).is_err());
        assert!(payload(&Value::String("好".repeat(683))).is_err());
        assert!(server_error(&serde_json::json!({"code":"secret password"})).is_err());
        hex(&new_request_id().unwrap(), 32).unwrap();
    }
}
