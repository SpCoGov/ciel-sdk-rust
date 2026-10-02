use std::fmt;

/// SDK 操作结果。错误不会包含凭据、原始服务器消息或业务 JSON。
pub type Result<T> = std::result::Result<T, Error>;

/// 错误所属层。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// 本地参数非法。
    Input,
    /// 身份文件、私有权限或目录锁失败。
    Storage,
    /// TLS 证书、Pin 或握手验证失败，不自动重试。
    Tls,
    /// 网络连接中断。
    Connection,
    /// 请求或心跳超时。
    Timeout,
    /// 收到非法协议响应。
    Protocol,
    /// 服务器返回机器错误码。
    Server,
    /// 本地队列或并发请求容量不足。
    Capacity,
}

/// 请求是否已尝试发送；Attempted 不能证明服务器未执行。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendStage {
    /// 尚未调用 WebSocket 写入。
    NotSent,
    /// 已开始写入，结果可能未知。
    Attempted,
}

/// 可安全记录的结构化错误。`retryable` 只用于重建连接，不允许自动重放业务。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    /// 错误类别。
    pub kind: ErrorKind,
    /// 安全的机器错误码。
    pub code: String,
    /// 与请求相关的 ID。
    pub request_id: Option<String>,
    /// 等待命令超时等情形下的调用记录 ID。
    pub record_id: Option<String>,
    /// 已知的发送阶段。
    pub send_stage: SendStage,
    /// 是否允许尝试重建连接。
    pub retryable: bool,
    /// Upgrade 限流要求的最短等待秒数。
    pub retry_after_seconds: Option<u64>,
}
impl Error {
    pub(crate) fn new(kind: ErrorKind, code: &str) -> Self {
        Self {
            kind,
            code: code.into(),
            request_id: None,
            record_id: None,
            send_stage: SendStage::NotSent,
            retryable: false,
            retry_after_seconds: None,
        }
    }
    pub(crate) fn connection(code: &str) -> Self {
        let mut error = Self::new(ErrorKind::Connection, code);
        error.retryable = true;
        error
    }
    pub(crate) fn timeout() -> Self {
        let mut error = Self::new(ErrorKind::Timeout, "CLIENT_TIMEOUT");
        error.retryable = true;
        error
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.code)
    }
}
impl std::error::Error for Error {}
