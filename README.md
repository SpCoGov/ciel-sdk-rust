[简体中文](https://github.com/SpCoGov/ciel-sdk-rust/blob/main/README.zh-CN.md)

<h1 align="center">
  <br>
  <a href="https://github.com/SpCoGov/ciel-sdk-rust"><img src="https://raw.githubusercontent.com/SpCoGov/ciel-sdk-rust/main/assets/logo.svg" alt="CIEL logo" width="150"></a>
  <br>
  CIEL Rust SDK
  <br>
</h1>

<h4 align="center">Connect Rust services to CIEL with Tokio.</h4>

<p align="center">
  <a href="https://github.com/SpCoGov/ciel-sdk-rust">Repository</a> •
  <a href="https://github.com/SpCoGov/ciel-sdk-rust/blob/main/docs/usage.md">Documentation</a> •
  <a href="https://github.com/SpCoGov/ciel-sdk-rust/blob/main/examples/client.rs">Example</a> •
  <a href="https://github.com/SpCoGov/ciel-sdk-rust/issues">Issues</a> •
  <a href="https://github.com/SpCoGov/ciel-sdk-java">Java SDK</a>
</p>

## 🛠️ Getting started

Requires **Rust 1.89+** and a Tokio runtime. The crate is named `ciel-sdk` and imported as `ciel_sdk`.

Add the SDK and Tokio to your application's `Cargo.toml`:

```toml
[dependencies]
ciel-sdk = "0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

In CIEL WebUI, have an administrator issue an enrollment grant for your service ID and save it as `grant.json`. Enroll once to create the instance identity:

```rust,no_run
use ciel_sdk::{Client, Result, enroll};

#[tokio::main]
async fn main() -> Result<()> {
    let directory = "worker-identity";
    enroll("grant.json", directory).await?;
    let client = Client::connect(directory).await?;
    println!("{}", client.identity().instance_id);
    client.close().await;
    Ok(())
}
```

On subsequent starts, connect using the existing directory. Keep grants and identity files private. For events, commands, notifications, and recovery rules, see the [usage guide](https://github.com/SpCoGov/ciel-sdk-rust/blob/main/docs/usage.md).

## ⚙️ Build

```sh
git clone https://github.com/SpCoGov/ciel-sdk-rust
cd ciel-sdk-rust
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo doc --no-deps --open
```

API documentation is generated in `target/doc/ciel_sdk/index.html`. The [build and test guide](https://github.com/SpCoGov/ciel-sdk-rust/blob/main/docs/usage.md#examples-and-validation) covers runnable examples and isolated CIEL acceptance tests. Before a release, validate the package with `cargo publish --locked --dry-run`; see the [publishing instructions](https://github.com/SpCoGov/ciel-sdk-rust/blob/main/docs/usage.md#publishing).

## 🚀 Contributing

Report problems through [Issues](https://github.com/SpCoGov/ciel-sdk-rust/issues) or submit a [pull request](https://github.com/SpCoGov/ciel-sdk-rust/pulls). Include a minimal reproduction for bugs and run the relevant checks before submitting changes. Keep the English and Chinese documentation in sync.

## ⚗️ Stack

Tokio + tokio-tungstenite + Serde + rustls. The SDK uses ring, sha2 and x509-parser for random IDs and certificate checks. It has no dependency on the CIEL server crate and does not install a logging backend.

## 📜 License

[Apache License 2.0](https://github.com/SpCoGov/ciel-sdk-rust/blob/main/LICENSE).
