use serde::Deserialize;
use serde_json::Value;

/// 接受事件发布的确认；入队不代表订阅者已处理。
#[derive(Debug, Clone, Deserialize)]
pub struct Publication {
    /// 事件 ID。
    pub event_id: String,
    /// 服务端历史记录 ID。
    pub publication_id: u64,
    /// 全部订阅实例数，包含离线实例。
    pub subscriber_count: u64,
    /// 成功进入在线连接队列的数量。
    pub queued_count: u64,
    /// 本次请求关联 ID，不提供发布去重。
    pub request_id: String,
}
/// 实时事件推送，没有离线重放或接收确认。
#[derive(Debug, Clone, Deserialize)]
pub struct Event {
    /// 事件 ID。
    pub event_id: String,
    /// 发布记录 ID。
    pub publication_id: u64,
    /// 发布者服务 ID。
    pub service_id: String,
    /// 发布者实例 ID。
    pub instance_id: String,
    /// 服务端接受时间，Unix 秒。
    pub created_at: i64,
    /// 业务 JSON。
    pub payload: Value,
}
/// 命令状态；Unknown 表示副作用不确定，不能自动重新执行。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CommandStatus {
    /// 已接受，尚未派发。
    Pending,
    /// 已派发，尚无终态。
    Dispatched,
    /// 提供方报告成功。
    Succeeded,
    /// 调用被拒绝或提供方报告失败。
    Failed,
    /// 失联、超时或服务器重启导致执行结果不确定。
    Unknown,
}
impl CommandStatus {
    /// 是否已经结束。
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending | Self::Dispatched)
    }
}
/// 持久化调用记录；Accepted 响应也可能包含终态。
#[derive(Debug, Clone, Deserialize)]
pub struct CommandCall {
    /// 服务端调用 ID。
    pub id: String,
    /// 原调用请求 ID，作为去重键。
    pub request_id: String,
    /// 调用方种类：user 或 instance。
    pub caller_kind: String,
    /// 调用方 ID。
    pub caller_id: String,
    /// 调用方显示名称。
    pub caller_label: String,
    /// 目标实例 ID。
    pub target_instance_id: String,
    /// 目标服务 ID。
    pub target_service_id: String,
    /// 命令 ID。
    pub command_id: String,
    /// 输入 JSON。
    pub input: Value,
    /// 输出 JSON，无结果时为 null。
    pub output: Value,
    /// 当前状态。
    pub status: CommandStatus,
    /// 失败或未知结果的机器错误码。
    pub error_code: Option<String>,
    /// 服务端执行超时秒数。
    pub timeout_seconds: u64,
    /// 接受时间，Unix 秒。
    pub created_at: i64,
    /// 截止时间，Unix 秒。
    pub deadline: i64,
    /// 派发时间，Unix 秒。
    pub dispatched_at: Option<i64>,
    /// 结束时间，Unix 秒。
    pub completed_at: Option<i64>,
}
/// 提供方收到的执行请求。用 [`crate::Client::respond_command`] 在原连接提交结果。
#[derive(Debug, Clone, Deserialize)]
pub struct CommandExecution {
    /// 调用 ID。
    pub call_id: String,
    /// 命令 ID。
    pub command_id: String,
    /// 输入 JSON。
    pub input: Value,
    /// 接受结果的截止时间；SDK 不会取消你的业务任务。
    pub deadline: i64,
    #[serde(skip)]
    pub(crate) session: String,
}
/// 通知渠道投递状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeliveryStatus {
    /// 等待发送。
    Pending,
    /// 正在发送。
    Sending,
    /// 渠道接受；邮件不保证已进入用户收件箱。
    Delivered,
    /// 按设置或配置跳过。
    Skipped,
    /// 投递失败。
    Failed,
}
/// 同渠道、状态和错误码的投递汇总。
#[derive(Debug, Clone, Deserialize)]
pub struct Delivery {
    /// 渠道：inbox 或 email。
    pub notifier_id: String,
    /// 投递状态。
    pub status: DeliveryStatus,
    /// 机器错误码。
    pub error_code: Option<String>,
    /// 该分组的投递数量。
    pub count: u64,
}
/// 已接受通知的最新汇总，通过查询获取后续状态，没有完成推送。
#[derive(Debug, Clone, Deserialize)]
pub struct Notification {
    /// 服务端通知 ID。
    pub id: String,
    /// 原发送请求 ID，作为去重键。
    pub request_id: String,
    /// 通知类型 ID。
    pub notification_type: String,
    /// 接受时间，Unix 秒。
    pub created_at: i64,
    /// 符合设置的接收人数。
    pub recipient_count: u64,
    /// 渠道状态分组。
    pub deliveries: Vec<Delivery>,
}
/// 服务端推送流。队列满时接收者会收到 Tokio `RecvError::Lagged`。
#[derive(Debug, Clone)]
pub enum Message {
    /// 实时事件。
    Event(Event),
    /// 在接收连接执行命令；业务处理不会阻塞 SDK 心跳。
    CommandExecute(CommandExecution),
    /// 原调用连接上的尽力完成推送，丢失时使用查询。
    CommandCompleted(Box<CommandCall>),
    /// 无关联请求的连接错误，或重连失败。
    Error(crate::Error),
}
