# CIEL Rust SDK 使用指南

[English](usage.md) · [返回 README](../README.zh-CN.md)

## 依赖

在应用的 Cargo.toml 中添加 SDK 和应用依赖：

```toml
[dependencies]
ciel-sdk = "0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde_json = "1"
```

本地开发时可以改用 `ciel-sdk = { path = "D:/ciel-sdk-rust" }`。

## 首次注册

在 WebUI 打开 **服务 → 授权注册**，授权准确的服务 ID，将返回 JSON 保存为 `worker-grant.json`。必需字段是 `server`、`server_spki_sha256`、`service_id`、`enrollment_token`；也接受 API 返回的 `id`、`expires_at` 元数据。服务 ID 区分大小写，由 3～64 个 ASCII 字母、数字、`.`、`_`、`-` 组成。

```rust,no_run
use ciel_sdk::{enroll, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let identity = enroll("worker-grant.json", "worker-identity").await?;
    println!("{} {}", identity.service_id, identity.instance_id);
    Ok(())
}
```

Token 和 Credential 只在 TLS 校验完成后发送。授权中的 SPKI SHA-256 是信任锚，不用主机名/SAN 或公共 CA 替代。SDK 检查证书有效期和握手签名，仅允许 TLS 1.3，并禁用 early data 与会话恢复。

先私有、持久化保存 `candidate-<instance_id>.json`，再发送 commit 消耗一次性 Token；收到匹配确认后原子激活 `identity.json`。确认丢失时，用原授权和目录再次调用 `enroll`：先逐个认证所有候选，只有明确 `AUTHENTICATION_FAILED` 才能继续尝试或重新注册。网络、TLS、限流、实例已有连接等错误保留候选并停止恢复。不会覆盖活动身份；凭据撤销或实例删除后需要新授权和新目录。

身份 JSON、候选文件名和 `agent.lock` 与 Java SDK、CIEL 参考客户端兼容，同一目录只能由一个进程使用。Unix 目录为 0700、文件为 0600；Windows 通过隐藏的 Windows PowerShell 进程设置仅当前用户可访问的 ACL。拒绝符号链接和重解析点。`Identity` Debug 隐藏凭据，显式序列化仍包含凭据，请私密保存授权和身份。

## 事件

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
    println!("已进入 {} 个连接的队列", publication.queued_count);
    if let Ok(Message::Event(event)) = messages.recv().await {
        println!("{}: {}", event.event_id, event.payload);
    }
    client.close().await;
    Ok(())
}
```

事件请求 ID 只关联响应，不提供去重：重发会产生新的发布记录和广播。事件仅在线尽力投递，没有接收确认、离线重放或自动重试；`queued_count` 不代表已接收或处理。

## 命令与通知

命令提供方用 `register_command` 注册，通过 `messages()` 消费 `Message::CommandExecute`，将收到的 `CommandExecution` 交给 `respond_command` 回传结果。对象内部保留原连接身份，重连后提交旧结果返回 `OLD_CONNECTION`。重连不会取消你的业务任务。耗时异步工作使用 `tokio::spawn`，阻塞工作使用 `spawn_blocking`，并由应用限制并发，避免堵塞消费循环。

调用方需要管理员对目标实例对应命令的授权，注册不会授予调用权限。使用 `invoke_command(target_instance_id, command_id, input, timeout_seconds, request_id)`，再用 `get_command` 或 `await_command` 获取终态。接受响应可能已经结束；`Pending`/`Dispatched` 表示未结束，`Succeeded`/`Failed`/`Unknown` 为终态。**Unknown 表示副作用不确定，不能盲目重新执行。** 完成推送只向原调用连接尽力投递，丢失后可查询恢复；客户端等待超时不会取消服务端执行。提交真实业务前持久保存请求 ID、目标、命令、输入和超时，人工决定重试时必须全部保持相同。冲突和记录保留/过期等错误直接返回。

通知用 `register_notification` 注册、`send_notification` 发送纯文本、`get_notification` 查询。用户在 CIEL 设置接收渠道，注册类型不会自动订阅用户。相同请求 ID 与类型/标题/正文去重，改变参数返回 `REQUEST_ID_CONFLICT`。`Notification.deliveries` 按渠道分组，`Pending`/`Sending` 需要继续查询，没有通知完成推送。邮件 `Delivered` 只表示 SMTP 接受，不保证收到或已读。

## API 与运行边界

所有公开 API 都有 Rustdoc：`cargo doc --no-deps --open`。

| 能力 | API |
|---|---|
| 身份与连接 | `enroll`、`Client::connect`、`connect_with_options`、`identity`、`close` |
| 状态与推送 | `state`、`states`、`messages` |
| 事件 | `register_event`、`unregister_event`、`subscribe_event`、`unsubscribe_event`、`publish_event` |
| 命令 | `register_command`、`unregister_command`、`invoke_command`、`get_command`、`await_command`、`respond_command` |
| 通知 | `register_notification`、`unregister_notification`、`send_notification`、`get_notification` |
| 请求 ID | `new_request_id` |

- 完整出站 JSON 最多 4096 字节 UTF-8，序列化业务 JSON 最多 2048 字节，入站最多 32768 字节。拒绝非法 JSON、重复字段、版本和响应关联错误，解析诊断不泄露原始消息。
- 最多 64 个未完成请求，发送队列有界，超限返回 `QUEUE_FULL`。普通业务写入间隔至少 200ms，心跳优先。超时包含排队时间，取消 future 不能撤回已开始的写入。
- 每个推送订阅者保留 64 条消息，必须处理 Tokio `RecvError::Lagged`；事件和命令派发都可能丢失。命令应由一个执行者消费，避免多个本地订阅者重复执行同一广播。慢消费者不会阻塞心跳。
- 默认连接/鉴权超时 10 秒、请求 15 秒、心跳确认 10 秒，心跳周期来自 `authenticated`，Ping/Pong 不能替代。通过 `Options` 调整网络时限。
- 临时连接故障按 1、2、4、8、16、30 秒退避，加不超过 250ms 抖动，遵守 HTTP 数字形式的 `Retry-After`。鉴权、证书、Pin、协议错误不会自动重试。注册和订阅由服务端持久保存，SDK 不重放业务或注册操作。
- `Error` 包含类别、安全机器码、请求/记录 ID、发送阶段、连接可重试标志和限流等待秒数。`Attempted` 表示副作用可能发生，`retryable` 仅允许恢复连接。旧响应未确认时复用 ID 返回 `REQUEST_ALREADY_PENDING`，应查询已知记录或等待重连。
- `close().await` 关闭全部 clone 并等待释放目录锁；丢弃最后一个 handle 会请求异步清理。需要立即接管目录时应显式关闭。

运行依赖为 Tokio、tokio-tungstenite、futures-util、Serde/serde_json、rustls、ring、sha2、x509-parser；HTTP、证书生成和临时目录库只用于测试。

## 示例与验证

```powershell
cd D:\ciel-sdk-rust
cargo run --example client -- enroll worker-grant.json worker-identity
cargo run --example client -- publish worker-identity
cargo run --example client -- serve worker-identity
# 先在 CIEL 中授权调用方实例：
cargo run --example client -- call caller-identity <target-instance-id>

cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo doc --no-deps
cargo package --offline --allow-dirty
```

默认测试覆盖 JSON/UTF-8、身份持久化和锁、TLS 拒绝场景、乱序响应、队列容量、超时和消费滞后时的独立心跳。启用真实服务验收：

```powershell
$env:CIEL_SERVER_BINARY = 'D:\ciel\target\debug\ciel.exe'
$env:CIEL_REFERENCE_CLIENT_BINARY = 'D:\ciel\target\debug\examples\test_client.exe' # 可选
cargo test --test acceptance -- --nocapture
```

验收启动独立的临时 CIEL 进程、数据库、TLS 身份、用户和授权，不访问正式数据。覆盖恢复、事件、命令/通知去重、真实心跳周期、重启后重连、旧结果拒绝及参考客户端互通。未设置 `CIEL_SERVER_BINARY` 时该用例提示跳过并返回。

## 发布

仓库：[SpCoGov/ciel-sdk-rust](https://github.com/SpCoGov/ciel-sdk-rust)。[Publish to crates.io 工作流](https://github.com/SpCoGov/ciel-sdk-rust/actions/workflows/publish.yml) 会先运行测试、构建 API 文档并验证发布包，再上传到 crates.io。

将 crates.io API Token 保存为仓库级 Secret `CARGO_REGISTRY_TOKEN`；首次发布需要允许发布新 crate。打开 **Actions → Publish to crates.io → Run workflow**：保留 `dry_run` 勾选只做检查，取消勾选则发布 `Cargo.toml` 中的版本。本机无需配置 Token 或 GPG。

在本机发布时，先运行 `cargo login`，然后执行：

```sh
cargo publish --locked --dry-run
cargo publish --locked
```

发布前检查包名、版本和打包内容。已发布版本不能覆盖，后续发布需要增加 `version`。Cargo 不需要 GPG。
