# CIEL Rust SDK Guide

[简体中文](usage.zh-CN.md) · [Back to README](../README.md)

## Local dependency

This crate has not been published to crates.io:

```toml
[dependencies]
ciel-sdk = { path = "D:/ciel-sdk-rust" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde_json = "1"
```

## Enrollment

In CIEL WebUI, open **Services → Authorize enrollment**, authorize the exact service ID, and save the returned JSON as `worker-grant.json`. Required fields: `server`, `server_spki_sha256`, `service_id`, `enrollment_token`. Optional API metadata `id` and `expires_at` is accepted. Service IDs are case sensitive, 3–64 ASCII letters, digits, `.`, `_`, or `-`.

```rust,no_run
use ciel_sdk::{enroll, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let identity = enroll("worker-grant.json", "worker-identity").await?;
    println!("{} {}", identity.service_id, identity.instance_id);
    Ok(())
}
```

TLS verification finishes before any token or credential is sent. The provisioned SPKI SHA-256 is the trust anchor; hostname/SAN and public CA trust do not replace it. Certificate validity and handshake signatures are checked. TLS 1.2, early data, and session resumption are disabled.

The SDK privately saves and syncs `candidate-<instance_id>.json` **before** committing the one-time token, then atomically activates `identity.json`. If confirmation is lost, call `enroll` again with the original grant and directory. Recovery authenticates all candidates first; only explicit `AUTHENTICATION_FAILED` permits trying another candidate or enrolling again. Network, TLS, rate-limit and duplicate-connection errors preserve candidates and stop recovery. Existing identities are never overwritten; revoked/deleted instances need a fresh grant and directory.

Identity JSON, candidate names and `agent.lock` are compatible with the Java SDK and CIEL reference clients. Only one process may use a directory. Unix uses directory mode 0700 and file mode 0600; Windows uses an owner-only ACL through a hidden Windows PowerShell process. Symlinks/reparse points are rejected. `Identity` Debug omits credentials; explicit serialization contains them. Keep grants and identity files private.

## Events

```rust,no_run
use ciel_sdk::{Client, Message, Result, new_request_id};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::connect("worker-identity").await?;
    let mut messages = client.messages();
    client.register_event("demo.changed").await?;
    client.subscribe_event("demo.changed").await?;
    let publication = client.publish_event(
        "demo.changed", json!({"value": 42}), &new_request_id()?
    ).await?;
    println!("Queued to {} connections", publication.queued_count);
    if let Ok(Message::Event(event)) = messages.recv().await {
        println!("{}: {}", event.event_id, event.payload);
    }
    client.close().await;
    Ok(())
}
```

Event request IDs only correlate replies: repeating a publication creates another record and broadcast attempt. Events are best effort while online, without acknowledgement, offline replay or automatic retry. `queued_count` is not proof of delivery or processing.

## Commands and notifications

Command providers register commands and consume `Message::CommandExecute`. Pass the received `CommandExecution` to `respond_command`: it retains the original session internally, and a delayed result cannot be sent through a later connection. Reconnection does not cancel your business task. Run slow async work with `tokio::spawn`, blocking work with `spawn_blocking`, and limit concurrency in your application.

Callers need administrator permission for each target instance/command; registration grants no calling permission. Use `invoke_command(target_instance_id, command_id, input, timeout_seconds, request_id)`, then `get_command` or `await_command`. An accepted record may already be terminal. `Pending`/`Dispatched` are unfinished; `Succeeded`/`Failed`/`Unknown` are terminal. **Unknown means side effects are uncertain.** Completion pushes are best effort on the original caller connection; queries work after reconnection. Client wait timeouts do not cancel server execution. Persist the request ID and exact target/command/input/timeout before a real call; an intentional retry must preserve all of them. Conflict and retention/expiry errors are returned to the caller.

Notifications use `register_notification`, `send_notification`, and `get_notification`. Users choose receiving channels in CIEL; registration alone does not subscribe them. Identical request ID and type/title/body deduplicate sends. Changed parameters return `REQUEST_ID_CONFLICT`. `Notification.deliveries` groups channel statuses. `Pending`/`Sending` need further queries: there is no notification-completed push. Email `Delivered` means SMTP accepted it, not that the user received or read it.

## Public API and limits

All public APIs have Rustdoc: `cargo doc --no-deps --open`.

| Area | API |
|---|---|
| Identity | `enroll`, `Client::connect`, `connect_with_options`, `identity`, `close` |
| Observation | `state`, `states`, `messages` |
| Events | `register_event`, `unregister_event`, `subscribe_event`, `unsubscribe_event`, `publish_event` |
| Commands | `register_command`, `unregister_command`, `invoke_command`, `get_command`, `await_command`, `respond_command` |
| Notifications | `register_notification`, `unregister_notification`, `send_notification`, `get_notification` |
| IDs | `new_request_id` |

- Outgoing JSON: 4096 UTF-8 bytes. Serialized business JSON: 2048 bytes. Receive limit: 32768 bytes. Invalid JSON, duplicate keys, invalid versions and response correlations are rejected; parser diagnostics are redacted.
- At most 64 outstanding requests with a bounded send queue. Overload returns `QUEUE_FULL`. Business writes are spaced 200ms apart; heartbeats have priority. Request timeout includes queue time. Cancellation cannot undo a write that already started.
- Each push subscriber retains 64 messages. Handle Tokio `RecvError::Lagged`; events and command execution pushes can be lost. Use one command executor to avoid executing the same broadcast from multiple local subscribers. Slow consumers never block heartbeat.
- Default timeouts: connect/authenticate 10s, request 15s, heartbeat acknowledgement 10s. Heartbeat interval comes from `authenticated`; Ping/Pong cannot replace it. `Options` controls network timeouts.
- Transient failures reconnect after 1, 2, 4, 8, 16, 30s plus up to 250ms jitter, respecting numeric HTTP `Retry-After`. Authentication, certificate, pin and protocol failures are terminal. Server-side registrations/subscriptions persist; SDK operations are never automatically replayed.
- `Error` exposes `kind`, safe machine `code`, optional `request_id`/`record_id`, `send_stage`, `retryable`, and `retry_after_seconds`. `Attempted` means side effects may have happened; `retryable` permits connection recovery only. Reusing an ID whose old reply remains outstanding returns `REQUEST_ALREADY_PENDING`; query a known record or wait for reconnection.
- `close().await` closes all clones and waits for the identity lock to be released. Dropping the last handle requests asynchronous cleanup; explicitly close before another client immediately reuses the directory.

Runtime dependencies: Tokio, tokio-tungstenite, futures-util, Serde/serde_json, rustls, ring, sha2, x509-parser. HTTP, certificate generation and temporary-directory libraries are test-only.

## Examples and validation

```powershell
cd D:\ciel-sdk-rust
cargo run --example client -- enroll worker-grant.json worker-identity
cargo run --example client -- publish worker-identity
cargo run --example client -- serve worker-identity
# Authorize the caller instance in CIEL first:
cargo run --example client -- call caller-identity <target-instance-id>

cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo doc --no-deps
cargo package --offline --allow-dirty
```

Default checks cover parsing/UTF-8 limits, identity durability and locks, negative TLS cases, out-of-order replies, bounded queues, timeouts and heartbeat with a slow push consumer. Enable real-server acceptance with:

```powershell
$env:CIEL_SERVER_BINARY = 'D:\ciel\target\debug\ciel.exe'
$env:CIEL_REFERENCE_CLIENT_BINARY = 'D:\ciel\target\debug\examples\test_client.exe' # optional
cargo test --test acceptance -- --nocapture
```

This starts a separate CIEL process with a temporary database, TLS identity, users and grants; it never touches live CIEL data. It checks recovery, events, command/notification deduplication, a real heartbeat cycle, server restart/reconnection, stale result rejection and reference-client interoperability. Without `CIEL_SERVER_BINARY`, the acceptance test reports a skip and returns.

## Publishing

Repository: [SpCoGov/ciel-sdk-rust](https://github.com/SpCoGov/ciel-sdk-rust). Publish using your own crates.io account:

```sh
cargo package --allow-dirty
cargo publish
```

Before publishing, verify that the crate name is available, check the version and inspect the package contents. Cargo does not require GPG. This project has not been published to crates.io.
