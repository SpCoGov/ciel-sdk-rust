[English](https://github.com/SpCoGov/ciel-sdk-rust/blob/main/README.md)

<h1 align="center">
  <br>
  <a href="https://github.com/SpCoGov/ciel-sdk-rust"><img src="https://raw.githubusercontent.com/SpCoGov/ciel-sdk-rust/main/assets/logo.svg" alt="CIEL logo" width="150"></a>
  <br>
  CIEL Rust SDK
  <br>
</h1>

<h4 align="center">基于 Tokio，让 Rust 服务接入 CIEL。</h4>

<p align="center">
  <a href="https://github.com/SpCoGov/ciel-sdk-rust">仓库</a> •
  <a href="https://github.com/SpCoGov/ciel-sdk-rust/blob/main/docs/usage.zh-CN.md">文档</a> •
  <a href="https://github.com/SpCoGov/ciel-sdk-rust/blob/main/examples/client.rs">示例</a> •
  <a href="https://github.com/SpCoGov/ciel-sdk-rust/issues">问题反馈</a> •
  <a href="https://github.com/SpCoGov/ciel-sdk-java">Java SDK</a>
</p>

## 🛠️ 快速开始

需要 **Rust 1.89+** 和 Tokio runtime。crate 名称为 `ciel-sdk`，导入名为 `ciel_sdk`。

在应用的 `Cargo.toml` 中添加 SDK 和 Tokio：

```toml
[dependencies]
ciel-sdk = "0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

在 CIEL WebUI 中由管理员为服务 ID 签发注册授权，将 JSON 保存为 `grant.json`。首次运行注册并保存实例身份：

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

以后启动时直接使用已有身份目录连接，不再重复注册。请私密保存授权和身份文件。事件、命令、通知及恢复规则见[使用指南](https://github.com/SpCoGov/ciel-sdk-rust/blob/main/docs/usage.zh-CN.md)。

## ⚙️ 构建

```sh
git clone https://github.com/SpCoGov/ciel-sdk-rust
cd ciel-sdk-rust
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo doc --no-deps --open
```

API 文档输出到 `target/doc/ciel_sdk/index.html`。可运行示例和隔离 CIEL 验收测试见[构建和测试说明](https://github.com/SpCoGov/ciel-sdk-rust/blob/main/docs/usage.zh-CN.md#示例与验证)。发布前使用 `cargo publish --locked --dry-run` 检查包，步骤见[发布说明](https://github.com/SpCoGov/ciel-sdk-rust/blob/main/docs/usage.zh-CN.md#发布)。

## 🚀 贡献

通过 [Issues](https://github.com/SpCoGov/ciel-sdk-rust/issues) 反馈问题，或提交 [Pull Request](https://github.com/SpCoGov/ciel-sdk-rust/pulls)。报告缺陷时请附最小复现，提交修改前运行相关检查，并同步更新中英文文档。

## ⚗️ 技术栈

Tokio + tokio-tungstenite + Serde + rustls。使用 ring、sha2、x509-parser 生成随机 ID 和校验证书；独立于 CIEL 服务端，不安装日志后端。

## 📜 许可证

[Apache License 2.0](https://github.com/SpCoGov/ciel-sdk-rust/blob/main/LICENSE)。
