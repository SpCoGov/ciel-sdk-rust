use crate::{
    CommandCall, CommandExecution, Error, ErrorKind, Event, Identity, Message, Notification,
    Publication, Result, SendStage, identity::Store, protocol, tls,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot, watch},
    time::Instant,
};
use tokio_tungstenite::tungstenite::Message as Wire;

/// 连接状态。通过 [`Client::states`] 订阅最新值。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    /// 已完成认证，可发送业务请求。
    Ready,
    /// 正在按退避间隔重连；此时业务请求返回 NOT_READY。
    Reconnecting,
    /// 遇到不能重试的错误；需关闭客户端并处理错误。
    Failed(Error),
    /// 已关闭，身份目录锁已释放。
    Closed,
}
/// 网络时限。队列容量固定为 64，业务发送间隔 200ms，心跳优先。
#[derive(Debug, Clone)]
pub struct Options {
    /// TLS、Upgrade 和认证操作超时，默认 10 秒。
    pub connect_timeout: Duration,
    /// 从本地入队到响应的总超时，默认 15 秒。
    pub request_timeout: Duration,
    /// 心跳写出后等待确认的超时，默认 10 秒。
    pub heartbeat_ack_timeout: Duration,
    /// 重连退避最大基准间隔，默认 30 秒；另加不超过 250ms 抖动。
    pub max_reconnect_delay: Duration,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(15),
            heartbeat_ack_timeout: Duration::from_secs(10),
            max_reconnect_delay: Duration::from_secs(30),
        }
    }
}
impl Options {
    fn validate(&self) -> Result<()> {
        for d in [
            self.connect_timeout,
            self.request_timeout,
            self.heartbeat_ack_timeout,
            self.max_reconnect_delay,
        ] {
            if d < Duration::from_millis(1) || d > Duration::from_secs(86400) {
                return Err(protocol::input("INVALID_TIMEOUT"));
            }
        }
        Ok(())
    }
}
struct Request {
    id: String,
    encoded: String,
    expected: &'static str,
    matches: Option<(&'static str, String)>,
    session: Option<String>,
    deadline: Instant,
    attempted: bool,
    reply: oneshot::Sender<Result<Value>>,
    _permit: OwnedSemaphorePermit,
}
impl Request {
    fn fail(self, mut error: Error) {
        error.request_id = Some(self.id);
        error.send_stage = if self.attempted {
            SendStage::Attempted
        } else {
            SendStage::NotSent
        };
        let _ = self.reply.send(Err(error));
    }
}
struct Inner {
    identity: Identity,
    options: Options,
    requests: mpsc::Sender<Request>,
    capacity: Arc<Semaphore>,
    messages: broadcast::Sender<Message>,
    state: watch::Receiver<State>,
    cancel: watch::Sender<bool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
    }
}
/// 可克隆的异步客户端。最后一个 handle 被丢弃时请求关闭；显式 close 会等待释放目录锁。
///
/// 内部后台任务负责有界请求队列、响应关联、心跳和连接恢复。
/// 业务请求不自动重放；推送消费不会阻塞网络任务。
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}
impl Client {
    /// 从已注册的身份目录连接并等待 READY；初次连接失败直接返回错误。
    pub async fn connect(directory: impl AsRef<Path>) -> Result<Self> {
        Self::connect_with_options(directory, Options::default()).await
    }
    /// 使用自定义时限连接，持有 agent.lock 至关闭。
    pub async fn connect_with_options(
        directory: impl AsRef<Path>,
        options: Options,
    ) -> Result<Self> {
        options.validate()?;
        let directory = directory.as_ref().to_path_buf();
        let (store, identity) = tokio::task::spawn_blocking(move || {
            let store = Store::open(&directory, false)?;
            let identity = store.identity()?;
            Ok::<_, Error>((store, identity))
        })
        .await
        .map_err(|_| Error::new(ErrorKind::Storage, "IDENTITY_TASK_FAILED"))??;
        let (socket, auth) = tls::authenticate(&identity, &options).await?;
        let (requests, rx) = mpsc::channel(64);
        let (messages, _) = broadcast::channel(64);
        let (state_tx, state) = watch::channel(State::Ready);
        let (cancel, cancel_rx) = watch::channel(false);
        let task = tokio::spawn(driver(
            store,
            identity.clone(),
            options.clone(),
            socket,
            auth,
            rx,
            messages.clone(),
            state_tx,
            cancel_rx,
        ));
        Ok(Self {
            inner: Arc::new(Inner {
                identity,
                options,
                requests,
                capacity: Arc::new(Semaphore::new(64)),
                messages,
                state,
                cancel,
                task: Mutex::new(Some(task)),
            }),
        })
    }
    /// 已注册身份的副本；Debug 隐藏凭据，显式序列化仍包含凭据。
    pub fn identity(&self) -> &Identity {
        &self.inner.identity
    }
    /// 最新连接状态。
    pub fn state(&self) -> State {
        self.inner.state.borrow().clone()
    }
    /// 订阅连接状态，使用 changed().await 后读取 borrow_and_update()。
    pub fn states(&self) -> watch::Receiver<State> {
        self.inner.state.clone()
    }
    /// 订阅后续推送。每个订阅者最多保留 64 条；必须处理 Lagged，命令执行消息也可能丢失。
    pub fn messages(&self) -> broadcast::Receiver<Message> {
        self.inner.messages.subscribe()
    }
    /// 停止重连、失败所有待处理请求、关闭连接并释放目录锁。所有 clone 都会关闭。
    pub async fn close(&self) {
        let _ = self.inner.cancel.send(true);
        let task = self
            .inner
            .task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(task) = task {
            let _ = task.await;
        } else {
            let mut state = self.states();
            while *state.borrow_and_update() != State::Closed {
                if state.changed().await.is_err() {
                    break;
                }
            }
        }
    }
    async fn request(
        &self,
        mut value: Value,
        expected: &'static str,
        matches: Option<(&'static str, String)>,
        session: Option<String>,
    ) -> Result<Value> {
        let id = protocol::string(&value, "request_id")?.to_owned();
        protocol::id(&id)?;
        if self.state() != State::Ready {
            let mut error = Error::new(ErrorKind::Connection, "NOT_READY");
            error.request_id = Some(id);
            return Err(error);
        }
        value["v"] = json!(1);
        let encoded = protocol::encode(&value)?;
        let permit = self
            .inner
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::new(ErrorKind::Capacity, "QUEUE_FULL"))?;
        let (reply, rx) = oneshot::channel();
        let request = Request {
            id,
            encoded,
            expected,
            matches,
            session,
            deadline: Instant::now() + self.inner.options.request_timeout,
            attempted: false,
            reply,
            _permit: permit,
        };
        if let Err(error) = self.inner.requests.try_send(request) {
            return Err(match error {
                mpsc::error::TrySendError::Full(_) => Error::new(ErrorKind::Capacity, "QUEUE_FULL"),
                _ => Error::new(ErrorKind::Connection, "NOT_READY"),
            });
        }
        rx.await
            .map_err(|_| Error::connection("CONNECTION_CLOSED"))?
    }
    async fn relation(
        &self,
        kind: &'static str,
        expected: &'static str,
        field: &'static str,
        id: &str,
        extra: Value,
    ) -> Result<()> {
        protocol::id(id)?;
        let mut value = json!({"type":kind,"request_id":protocol::new_request_id()?});
        value[field] = json!(id);
        if let Some(extra) = extra.as_object() {
            value.as_object_mut().unwrap().extend(extra.clone());
        }
        self.request(value, expected, Some((field, id.into())), None)
            .await?;
        Ok(())
    }
    /// 声明本实例可以发布事件，重复注册幂等，关系在服务端持久化。
    pub async fn register_event(&self, event: &str) -> Result<()> {
        self.relation(
            "event_register",
            "event_registered",
            "event_id",
            event,
            Value::Null,
        )
        .await
    }
    /// 注销发布资格，不删除历史。
    pub async fn unregister_event(&self, event: &str) -> Result<()> {
        self.relation(
            "event_unregister",
            "event_unregistered",
            "event_id",
            event,
            Value::Null,
        )
        .await
    }
    /// 订阅后续在线事件，可以早于事件注册。
    pub async fn subscribe_event(&self, event: &str) -> Result<()> {
        self.relation(
            "event_subscribe",
            "event_subscribed",
            "event_id",
            event,
            Value::Null,
        )
        .await
    }
    /// 取消订阅，不存在的关系也成功。
    pub async fn unsubscribe_event(&self, event: &str) -> Result<()> {
        self.relation(
            "event_unsubscribe",
            "event_unsubscribed",
            "event_id",
            event,
            Value::Null,
        )
        .await
    }
    /// 发布 JSON 事件。request_id 仅关联响应，每次重发都会产生新的发布记录。
    pub async fn publish_event(
        &self,
        event: &str,
        payload: Value,
        request_id: &str,
    ) -> Result<Publication> {
        protocol::id(event)?;
        protocol::payload(&payload)?;
        protocol::decode(self.request(json!({"type":"event_publish","event_id":event,"payload":payload,"request_id":request_id}),"event_published",Some(("event_id",event.into())),None).await?)
    }
    /// 注册或更新本实例的命令说明（最多 1024 字节 UTF-8），不会授予调用权限。
    pub async fn register_command(&self, command: &str, description: &str) -> Result<()> {
        protocol::text(description, 1024, false)?;
        self.relation(
            "command_register",
            "command_registered",
            "command_id",
            command,
            json!({"description":description}),
        )
        .await
    }
    /// 注销命令并删除其调用授权，历史记录仍保留。
    pub async fn unregister_command(&self, command: &str) -> Result<()> {
        self.relation(
            "command_unregister",
            "command_unregistered",
            "command_id",
            command,
            Value::Null,
        )
        .await
    }
    /// 调用已获管理员授权的目标命令。超时 1–300 秒；接受不代表执行成功。
    /// 相同 request_id 必须对应完全相同的目标、命令、输入和超时，SDK 不自动重试。
    pub async fn invoke_command(
        &self,
        target: &str,
        command: &str,
        input: Value,
        timeout_seconds: u64,
        request_id: &str,
    ) -> Result<CommandCall> {
        protocol::hex(target, 32)?;
        protocol::id(command)?;
        protocol::payload(&input)?;
        if !(1..=300).contains(&timeout_seconds) {
            return Err(protocol::input("INVALID_TIMEOUT"));
        }
        let reply = self.request(json!({"type":"command_invoke","target_instance_id":target,"command_id":command,"input":input,"timeout_seconds":timeout_seconds,"request_id":request_id}),"command_accepted",None,None).await?;
        let call = call(reply["call"].clone())?;
        if call.request_id != request_id
            || call.target_instance_id != target
            || call.command_id != command
        {
            return Err(protocol::bad());
        }
        Ok(call)
    }
    /// 查询原实例发起的调用，包括重连后的持久化结果。
    pub async fn get_command(&self, call_id: &str) -> Result<CommandCall> {
        protocol::hex(call_id, 32)?;
        let reply = self.request(json!({"type":"command_get","call_id":call_id,"request_id":protocol::new_request_id()?}),"command_status",None,None).await?;
        let call = call(reply["call"].clone())?;
        if call.id != call_id {
            return Err(protocol::bad());
        }
        Ok(call)
    }
    /// 等待终态，通过查询恢复丢失推送。客户端等待超时不会取消服务端调用。
    pub async fn await_command(&self, call_id: &str, timeout: Duration) -> Result<CommandCall> {
        protocol::hex(call_id, 32)?;
        if timeout.is_zero() || timeout > Duration::from_secs(86400) {
            return Err(protocol::input("INVALID_TIMEOUT"));
        }
        tokio::time::timeout(timeout, async {
            loop {
                let call = self.get_command(call_id).await?;
                if call.status.is_terminal() {
                    return Ok(call);
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            let mut error = Error::timeout();
            error.record_id = Some(call_id.into());
            Err(error)
        })
    }
    /// 在执行请求所属连接提交结果。连接已更换时返回 OLD_CONNECTION，绝不跨重连发送。
    /// 必须传入 messages() 收到的 CommandExecution，SDK 不负责执行或取消业务任务。
    pub async fn respond_command(
        &self,
        execution: &CommandExecution,
        success: bool,
        output: Value,
    ) -> Result<()> {
        protocol::hex(&execution.call_id, 32)?;
        protocol::payload(&output)?;
        let reply = self.request(json!({"type":"command_result","call_id":execution.call_id,"success":success,"output":output,"request_id":protocol::new_request_id()?}),"command_result_ack",Some(("call_id",execution.call_id.clone())),Some(execution.session.clone())).await?;
        let status: crate::CommandStatus = protocol::decode(reply["status"].clone())?;
        if !status.is_terminal() {
            return Err(protocol::bad());
        }
        Ok(())
    }
    /// 注册或更新通知类型。name 非空且最多 128 字节，description 最多 1024 字节。
    pub async fn register_notification(
        &self,
        kind: &str,
        name: &str,
        description: &str,
    ) -> Result<()> {
        protocol::text(name, 128, true)?;
        protocol::text(description, 1024, false)?;
        self.relation(
            "notification_register",
            "notification_registered",
            "notification_type",
            kind,
            json!({"name":name,"description":description}),
        )
        .await
    }
    /// 注销通知类型，不撤回已接受通知或清除用户偏好。
    pub async fn unregister_notification(&self, kind: &str) -> Result<()> {
        self.relation(
            "notification_unregister",
            "notification_unregistered",
            "notification_type",
            kind,
            Value::Null,
        )
        .await
    }
    /// 向符合用户设置的接收者发送纯文本通知。标题最多 200 字节，正文最多 2048 字节。
    /// 相同 request_id 和相同类型/标题/正文去重；返回接受汇总而非所有渠道最终成功。
    pub async fn send_notification(
        &self,
        kind: &str,
        title: &str,
        body: &str,
        request_id: &str,
    ) -> Result<Notification> {
        protocol::id(kind)?;
        protocol::text(title, 200, true)?;
        protocol::text(body, 2048, true)?;
        let reply = self.request(json!({"type":"notification_send","notification_type":kind,"title":title,"body":body,"request_id":request_id}),"notification_result",None,None).await?;
        let notification = notification(reply["notification"].clone())?;
        if notification.request_id != request_id || notification.notification_type != kind {
            return Err(protocol::bad());
        }
        Ok(notification)
    }
    /// 查询本实例已发送通知的最新投递汇总。
    pub async fn get_notification(&self, notification_id: &str) -> Result<Notification> {
        protocol::hex(notification_id, 32)?;
        let reply = self.request(json!({"type":"notification_get","notification_id":notification_id,"request_id":protocol::new_request_id()?}),"notification_status",None,None).await?;
        let notification = notification(reply["notification"].clone())?;
        if notification.id != notification_id {
            return Err(protocol::bad());
        }
        Ok(notification)
    }
}
fn call(value: Value) -> Result<CommandCall> {
    let call: CommandCall = protocol::decode(value)?;
    protocol::hex(&call.id, 32).map_err(|_| protocol::bad())?;
    protocol::id(&call.request_id).map_err(|_| protocol::bad())?;
    Ok(call)
}
fn notification(value: Value) -> Result<Notification> {
    let value: Notification = protocol::decode(value)?;
    protocol::hex(&value.id, 32).map_err(|_| protocol::bad())?;
    Ok(value)
}
async fn write<S>(
    writer: &mut S,
    message: Wire,
    timeout: Duration,
    cancel: &mut watch::Receiver<bool>,
) -> Result<()>
where
    S: futures_util::Sink<Wire, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    tokio::select! {
        biased;
        _ = cancel.changed() => Err(Error::new(ErrorKind::Connection,"CLIENT_CLOSED")),
        result = tokio::time::timeout(timeout,writer.send(message)) => result.map_err(|_| Error::timeout())?.map_err(tls::transport_error),
    }
}
fn sweep(
    pending: &mut HashMap<String, Request>,
    queue: &mut VecDeque<String>,
    abandoned: &mut HashSet<String>,
) -> Result<()> {
    let expired: Vec<_> = pending
        .iter()
        .filter(|(_, r)| r.deadline <= Instant::now() || r.reply.is_closed())
        .map(|(id, _)| id.clone())
        .collect();
    for id in expired {
        if let Some(r) = pending.remove(&id) {
            if r.attempted {
                abandoned.insert(id);
            }
            r.fail(Error::timeout());
        }
    }
    queue.retain(|id| pending.contains_key(id));
    // Bound late-response tombstones without confusing a new request with an old response.
    if abandoned.len() >= 64 {
        return Err(Error::connection("TOO_MANY_UNCONFIRMED_REQUESTS"));
    }
    Ok(())
}
// One network task serializes requests and prioritizes heartbeat over the bounded business queue.
async fn session(
    socket: tls::Socket,
    auth: Value,
    options: &Options,
    rx: &mut mpsc::Receiver<Request>,
    messages: &broadcast::Sender<Message>,
    cancel: &mut watch::Receiver<bool>,
) -> Error {
    let mut pending = HashMap::<String, Request>::new();
    let mut queue = VecDeque::new();
    let mut abandoned = HashSet::new();
    let result: Result<()> = async {
        let session_id = protocol::string(&auth,"session_id")?.to_owned();
        let interval = Duration::from_secs(auth["heartbeat_interval_seconds"].as_u64().ok_or_else(protocol::bad)?);
        let (mut writer,mut reader) = socket.split();
        let mut heartbeat = Instant::now()+interval;
        let mut ack: Option<Instant> = None;
        let mut next_write = Instant::now();
        loop {
            let expiry = pending.values().map(|r| r.deadline).min();
            tokio::select! {
                biased;
                _ = cancel.changed() => return Err(Error::new(ErrorKind::Connection,"CLIENT_CLOSED")),
                _ = wait(ack) => return Err(Error::connection("HEARTBEAT_TIMEOUT")),
                _ = tokio::time::sleep_until(heartbeat) => {
                    heartbeat = Instant::now()+interval;
                    if ack.is_none() {
                        write(&mut writer,Wire::Text("{\"v\":1,\"type\":\"heartbeat\"}".into()),options.connect_timeout,cancel).await?;
                        ack = Some(Instant::now()+options.heartbeat_ack_timeout.min(interval));
                        next_write = Instant::now()+Duration::from_millis(200);
                    }
                }
                _ = wait(expiry) => sweep(&mut pending,&mut queue,&mut abandoned)?,
                _ = tokio::time::sleep_until(next_write), if !queue.is_empty() => {
                    sweep(&mut pending,&mut queue,&mut abandoned)?;
                    if let Some(id) = queue.pop_front() && let Some(request) = pending.get_mut(&id) {
                        request.attempted = true;
                        write(&mut writer,Wire::Text(request.encoded.clone().into()),options.connect_timeout,cancel).await?;
                        next_write = Instant::now()+Duration::from_millis(200);
                    }
                }
                request = rx.recv() => {
                    let Some(request) = request else { return Err(Error::new(ErrorKind::Connection,"CLIENT_CLOSED")); };
                    if request.reply.is_closed() { continue; }
                    if request.session.as_ref().is_some_and(|s| *s != session_id) { request.fail(Error::new(ErrorKind::Connection,"OLD_CONNECTION")); continue; }
                    if pending.contains_key(&request.id) || abandoned.contains(&request.id) { request.fail(protocol::input("REQUEST_ALREADY_PENDING")); continue; }
                    queue.push_back(request.id.clone());
                    pending.insert(request.id.clone(),request);
                }
                incoming = reader.next() => {
                    match incoming.ok_or_else(|| Error::connection("CONNECTION_CLOSED"))?.map_err(tls::transport_error)? {
                        Wire::Text(text) => {
                            let reply = protocol::response(text.as_bytes())?;
                            match protocol::string(&reply,"type")? {
                                "heartbeat_ack" => {
                                    if reply["server_time"].as_i64().is_none() { return Err(protocol::bad()); }
                                    ack = None;
                                }
                                "event" => {
                                    let event: Event = protocol::decode(reply)?;
                                    protocol::id(&event.event_id).map_err(|_| protocol::bad())?;
                                    protocol::hex(&event.instance_id,32).map_err(|_| protocol::bad())?;
                                    let _ = messages.send(Message::Event(event));
                                }
                                "command_execute" => {
                                    let mut execution: CommandExecution = protocol::decode(reply)?;
                                    protocol::hex(&execution.call_id,32).map_err(|_| protocol::bad())?;
                                    protocol::id(&execution.command_id).map_err(|_| protocol::bad())?;
                                    execution.session = session_id.clone();
                                    let _ = messages.send(Message::CommandExecute(execution));
                                }
                                "command_completed" => { let _ = messages.send(Message::CommandCompleted(Box::new(call(reply["call"].clone())?))); }
                                _ => {
                                    if !matches!(protocol::string(&reply,"type")?,"error"|"event_registered"|"event_unregistered"|"event_subscribed"|"event_unsubscribed"|"event_published"|"command_registered"|"command_unregistered"|"command_accepted"|"command_status"|"command_result_ack"|"notification_registered"|"notification_unregistered"|"notification_result"|"notification_status") { return Err(protocol::bad()); }
                                    if let Some(id) = reply.get("request_id") {
                                        let id = id.as_str().ok_or_else(protocol::bad)?;
                                        protocol::id(id).map_err(|_| protocol::bad())?;
                                        if let Some(request) = pending.remove(id) {
                                            if !request.attempted { request.fail(protocol::bad()); return Err(protocol::bad()); }
                                            let response = protocol::expected(&reply,request.expected).and_then(|()| {
                                                if let Some((field,id)) = &request.matches && reply[field] != *id { return Err(protocol::bad()); }
                                                Ok(reply)
                                            });
                                            match response {
                                                Err(error) if error.kind == ErrorKind::Protocol => { request.fail(error.clone()); return Err(error); }
                                                Err(error) => request.fail(error),
                                                Ok(value) => { let _ = request.reply.send(Ok(value)); }
                                            }
                                        } else { abandoned.remove(id); } // Discard expired/cancelled replies.
                                    } else if reply["type"] == "error" { return Err(protocol::server_error(&reply)?); }
                                    else { return Err(protocol::bad()); }
                                }
                            }
                        }
                        Wire::Ping(_) => {
                            tokio::time::timeout(options.connect_timeout,writer.flush()).await.map_err(|_| Error::timeout())?.map_err(tls::transport_error)?;
                        }
                        Wire::Pong(_) => {},
                        Wire::Close(frame) => {
                            // A policy close is terminal even if its preceding JSON error was lost.
                            if frame.is_some_and(|f| matches!(u16::from(f.code),1002|1003|1007|1008|1009)) { return Err(protocol::bad()); }
                            return Err(Error::connection("CONNECTION_CLOSED"));
                        }
                        _ => return Err(protocol::bad()),
                    }
                }
            }
        }
    }.await;
    let error = result.unwrap_err();
    for (_, request) in pending {
        request.fail(error.clone());
    }
    while let Ok(request) = rx.try_recv() {
        request.fail(error.clone());
    }
    error
}
async fn wait(deadline: Option<Instant>) {
    match deadline {
        Some(time) => tokio::time::sleep_until(time).await,
        None => std::future::pending().await,
    }
}
#[allow(clippy::too_many_arguments)]
async fn driver(
    store: Store,
    identity: Identity,
    options: Options,
    mut socket: tls::Socket,
    mut auth: Value,
    mut rx: mpsc::Receiver<Request>,
    messages: broadcast::Sender<Message>,
    state: watch::Sender<State>,
    mut cancel: watch::Receiver<bool>,
) {
    let mut delay = Duration::from_secs(1).min(options.max_reconnect_delay);
    loop {
        if *cancel.borrow() {
            break;
        }
        let started = Instant::now();
        let mut error = session(socket, auth, &options, &mut rx, &messages, &mut cancel).await;
        if *cancel.borrow() {
            break;
        }
        if started.elapsed() >= Duration::from_secs(30) {
            delay = Duration::from_secs(1).min(options.max_reconnect_delay);
        }
        loop {
            let _ = messages.send(Message::Error(error.clone()));
            if !error.retryable {
                state.send_replace(State::Failed(error));
                loop {
                    tokio::select! {
                        _ = cancel.changed() => break,
                        request = rx.recv() => match request { Some(r) => r.fail(Error::new(ErrorKind::Connection,"NOT_READY")),None => break },
                    }
                }
                drop(store);
                state.send_replace(State::Closed);
                return;
            }
            state.send_replace(State::Reconnecting);
            let jitter = protocol::new_request_id()
                .ok()
                .and_then(|id| u8::from_str_radix(&id[..2], 16).ok())
                .unwrap_or(0)
                .min(250) as u64;
            let pause = delay.max(Duration::from_secs(
                error.retry_after_seconds.unwrap_or(0).min(86400),
            )) + Duration::from_millis(jitter);
            let end = Instant::now() + pause;
            loop {
                tokio::select! {
                    biased;
                    _ = cancel.changed() => { drop(store); state.send_replace(State::Closed); return; }
                    _ = tokio::time::sleep_until(end) => break,
                    request = rx.recv() => match request { Some(r) => r.fail(Error::new(ErrorKind::Connection,"NOT_READY")), None => { drop(store); state.send_replace(State::Closed); return; } },
                }
            }
            delay = (delay * 2).min(options.max_reconnect_delay);
            let result = tokio::select! {
                biased;
                _ = cancel.changed() => { drop(store); state.send_replace(State::Closed); return; }
                result = tls::authenticate(&identity,&options) => result,
            };
            match result {
                Ok((connected, reply)) => {
                    socket = connected;
                    auth = reply;
                    state.send_replace(State::Ready);
                    break;
                }
                Err(e) => error = e,
            }
        }
    }
    while let Ok(request) = rx.try_recv() {
        request.fail(Error::new(ErrorKind::Connection, "CLIENT_CLOSED"));
    }
    drop(store);
    state.send_replace(State::Closed);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn routes_out_of_order_bounds_queues_times_out_and_keeps_heartbeat_independent() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let heartbeats = Arc::new(AtomicUsize::new(0));
        let count = heartbeats.clone();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let first = protocol::response(
                socket
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_text()
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
            let second = protocol::response(
                socket
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_text()
                    .unwrap()
                    .as_bytes(),
            )
            .unwrap();
            for request in [second, first] {
                socket.send(Wire::Text(json!({"v":1,"type":"event_subscribed","event_id":request["event_id"],"request_id":request["request_id"]}).to_string().into())).await.unwrap();
            }
            while let Some(Ok(Wire::Text(text))) = socket.next().await {
                let request = protocol::response(text.as_bytes()).unwrap();
                if request["type"] == "heartbeat" {
                    count.fetch_add(1, Ordering::SeqCst);
                    socket
                        .send(Wire::Text(
                            json!({"v":1,"type":"heartbeat_ack","server_time":1})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                } else if request["type"] == "event_publish" {
                    // Leave the publication unconfirmed, then overflow a slow push consumer.
                    for publication_id in 0..70 {
                        socket.send(Wire::Text(json!({"v":1,"type":"event","event_id":"demo.changed","publication_id":publication_id,"service_id":"mock.server","instance_id":"a".repeat(32),"created_at":1,"payload":null}).to_string().into())).await.unwrap();
                    }
                } // Deliberately do not confirm the capacity-test subscription requests.
            }
        });
        let tcp = tokio::net::TcpStream::connect(address).await.unwrap();
        let (socket, _) = tokio_tungstenite::client_async(
            format!("ws://{address}/ws/service"),
            tokio_tungstenite::MaybeTlsStream::Plain(tcp),
        )
        .await
        .unwrap();
        let options = Options {
            request_timeout: Duration::from_millis(600),
            heartbeat_ack_timeout: Duration::from_millis(600),
            ..Options::default()
        };
        let (requests, mut rx) = mpsc::channel(64);
        let (messages, _) = broadcast::channel(64);
        let (state_tx, state) = watch::channel(State::Ready);
        let (cancel, mut cancel_rx) = watch::channel(false);
        let events = messages.clone();
        let configuration = options.clone();
        let task = tokio::spawn(async move {
            let error = session(
                socket,
                json!({"session_id":"b".repeat(32),"heartbeat_interval_seconds":1}),
                &configuration,
                &mut rx,
                &events,
                &mut cancel_rx,
            )
            .await;
            assert_eq!(error.code, "CLIENT_CLOSED");
            state_tx.send_replace(State::Closed);
        });
        let client = Client {
            inner: Arc::new(Inner {
                identity: Identity {
                    server: format!("wss://{address}/ws/service"),
                    server_spki_sha256: "a".repeat(64),
                    service_id: "mock.client".into(),
                    instance_id: "b".repeat(32),
                    credential: "c".repeat(64),
                },
                options,
                requests,
                capacity: Arc::new(Semaphore::new(64)),
                messages,
                state,
                cancel,
                task: Mutex::new(Some(task)),
            }),
        };
        let mut messages = client.messages();
        let (a, b) = tokio::join!(
            client.subscribe_event("first.event"),
            client.subscribe_event("second.event")
        );
        a.unwrap();
        b.unwrap();
        let timeout = client
            .publish_event("demo.changed", Value::Null, "reuse-once")
            .await
            .unwrap_err();
        assert_eq!(timeout.kind, ErrorKind::Timeout);
        assert_eq!(timeout.send_stage, SendStage::Attempted);
        assert_eq!(timeout.request_id.as_deref(), Some("reuse-once"));
        assert_eq!(
            client
                .publish_event("demo.changed", Value::Null, "reuse-once")
                .await
                .unwrap_err()
                .code,
            "REQUEST_ALREADY_PENDING"
        );
        assert!(matches!(
            messages.recv().await,
            Err(broadcast::error::RecvError::Lagged(_))
        ));
        let mut calls = Vec::new();
        for i in 0..65 {
            let client = client.clone();
            calls.push(tokio::spawn(async move {
                client.subscribe_event(&format!("overflow.{i}")).await
            }));
        }
        let mut full = 0;
        let mut unsent = 0;
        for request in calls {
            match request.await.unwrap().unwrap_err() {
                Error {
                    kind: ErrorKind::Capacity,
                    ..
                } => full += 1,
                Error {
                    kind: ErrorKind::Timeout,
                    send_stage: SendStage::NotSent,
                    ..
                } => unsent += 1,
                Error {
                    kind: ErrorKind::Timeout,
                    ..
                } => {}
                error => panic!("Unexpected error: {error}"),
            }
        }
        assert!(full >= 1);
        assert!(unsent >= 1);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(heartbeats.load(Ordering::SeqCst) >= 2);
        assert_eq!(client.state(), State::Ready);
        client.close().await;
        server.await.unwrap();
    }
}
