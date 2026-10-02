[简体中文](README.zh-CN.md)

<h1 align="center">
  <br>
  <a href="https://github.com/SpCoGov/ciel-sdk-rust"><img src="assets/logo.svg" alt="CIEL logo" width="150"></a>
  <br>
  CIEL Rust SDK
  <br>
</h1>

<h4 align="center">Connect Rust services to CIEL with Tokio.</h4>

<p align="center">
  <a href="https://github.com/SpCoGov/ciel-sdk-rust">Repository</a> •
  <a href="docs/usage.md">Documentation</a> •
  <a href="examples/client.rs">Example</a> •
  <a href="https://github.com/SpCoGov/ciel-sdk-rust/issues">Issues</a> •
  <a href="https://github.com/SpCoGov/ciel-sdk-java">Java SDK</a>
</p>

## 🛠️ Getting started

Requires **Rust 1.89+** and a Tokio runtime. The crate is named `ciel-sdk` and imported as `ciel_sdk`; it has not yet been published to crates.io.

```sh
git clone https://github.com/SpCoGov/ciel-sdk-rust
```

Add a local dependency to your application's `Cargo.toml`, adjusting the path to your checkout:

```toml
[dependencies]
ciel-sdk = { path = "../ciel-sdk-rust" }
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

On subsequent starts, connect using the existing directory. Keep grants and identity files private. For events, commands, notifications, and recovery rules, see the [usage guide](docs/usage.md).

## ⚙️ Build

```sh
cd ciel-sdk-rust
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo doc --no-deps --open
```

API documentation is generated in `target/doc/ciel_sdk/index.html`. The [build and test guide](docs/usage.md#examples-and-validation) covers runnable examples and isolated CIEL acceptance tests. Before a release, validate the package with `cargo package --allow-dirty`; see the [publishing instructions](docs/usage.md#publishing).

## 🚀 Contributing

Report problems through [Issues](https://github.com/SpCoGov/ciel-sdk-rust/issues) or submit a [pull request](https://github.com/SpCoGov/ciel-sdk-rust/pulls). Include a minimal reproduction for bugs and run the relevant checks before submitting changes. Keep the English and Chinese documentation in sync.

## ⚗️ Stack

Tokio + tokio-tungstenite + Serde + rustls. The SDK uses ring, sha2 and x509-parser for random IDs and certificate checks. It has no dependency on the CIEL server crate and does not install a logging backend.

## 📜 License

[Apache License 2.0](LICENSE).
