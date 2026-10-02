//! CIEL 协议 v1 的异步 SDK。需要 Tokio runtime、Rust 1.89 或以上。
//!
//! 首次接入使用 [`enroll`]，随后用 [`Client::connect`] 连接。
//! [`Client::messages`] 提供有界推送流，心跳和重连独立运行。
//! 身份目录与 Java SDK / CIEL 参考客户端兼容，同一目录只能由一个客户端使用。
//! 不自动重放业务请求；超时或断线后的副作用可能已发生。
//!
//! ```no_run
//! use ciel_sdk::{Client, Message, Result};
//! use serde_json::json;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let client = Client::connect("identity").await?;
//!     let mut messages = client.messages();
//!     client.register_event("demo.changed").await?;
//!     client.subscribe_event("demo.changed").await?;
//!     client.publish_event("demo.changed", json!({"value": 42}), "publish-001").await?;
//!     if let Ok(Message::Event(event)) = messages.recv().await {
//!         println!("{}: {}", event.event_id, event.payload);
//!     }
//!     client.close().await;
//!     Ok(())
//! }
//! ```
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod client;
mod error;
mod identity;
mod protocol;
mod tls;
mod types;

pub use client::{Client, Options, State};
pub use error::{Error, ErrorKind, Result, SendStage};
pub use identity::{Identity, enroll};
pub use protocol::new_request_id;

// Keep both README examples executable through the native Rustdoc checks.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
mod readme_en {}
#[cfg(doctest)]
#[doc = include_str!("../README.zh-CN.md")]
mod readme_zh {}
pub use types::{
    CommandCall, CommandExecution, CommandStatus, Delivery, DeliveryStatus, Event, Message,
    Notification, Publication,
};
