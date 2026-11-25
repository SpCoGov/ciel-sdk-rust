//! Optional acceptance check using an isolated CIEL process, never the user's live data.
use ciel_sdk::{Client, CommandStatus, ErrorKind, Message, State, enroll, new_request_id};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Server {
    binary: PathBuf,
    root: PathBuf,
    port: u16,
    process: Option<Child>,
    http: reqwest::Client,
    cookie: String,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}
impl Server {
    fn stop(&mut self) {
        if let Some(mut p) = self.process.take() {
            let _ = p.kill();
            let _ = p.wait();
        }
    }
    async fn start(&mut self) {
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("server.log"))
            .unwrap();
        let mut command = Command::new(&self.binary);
        command
            .current_dir(&self.root)
            .env("CIEL_DATA_DIR", self.root.join("server-data"))
            .env("CIEL_WEB_DIR", self.root.join("web"))
            .env("CIEL_LISTEN", format!("127.0.0.1:{}", self.port))
            .env("CIEL_ORIGIN", format!("https://localhost:{}", self.port))
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        self.process = Some(command.spawn().unwrap());
        let cert_path = self.root.join("server-data/tls/server.crt");
        let mut ready = false;
        for _ in 0..150 {
            assert!(
                self.process.as_mut().unwrap().try_wait().unwrap().is_none(),
                "Isolated CIEL exited"
            );
            if let Ok(pem) = fs::read(&cert_path) {
                self.http = reqwest::Client::builder()
                    .add_root_certificate(reqwest::Certificate::from_pem(&pem).unwrap())
                    .timeout(Duration::from_secs(3))
                    .build()
                    .unwrap();
                if self
                    .http
                    .get(format!("https://localhost:{}/", self.port))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
                {
                    ready = true;
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(ready, "Isolated CIEL failed to start");
    }
    async fn api(&self, method: &str, path: &str, body: Option<Value>) -> Value {
        let origin = format!("https://localhost:{}", self.port);
        let mut request = self
            .http
            .request(method.parse().unwrap(), format!("{origin}{path}"))
            .header("Origin", origin)
            .header("X-Ciel-Request", "1");
        if !self.cookie.is_empty() {
            request = request.header("Cookie", &self.cookie);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.unwrap();
        assert!(
            response.status().is_success(),
            "Administration API failed: {method} {path}: {}",
            response.status()
        );
        response.json().await.unwrap()
    }
    async fn grant(&self, service: &str) -> PathBuf {
        let grant = self
            .api(
                "POST",
                "/api/enrollments",
                Some(json!({"service_id":service})),
            )
            .await;
        let path = self.root.join(format!("{service}-grant.json"));
        fs::write(&path, serde_json::to_vec(&grant).unwrap()).unwrap();
        path
    }
}
async fn ready(client: &Client) {
    let mut states = client.states();
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            if *states.borrow_and_update() == State::Ready {
                break;
            }
            states.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}
async fn event(messages: &mut tokio::sync::broadcast::Receiver<Message>) -> ciel_sdk::Event {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Message::Event(v) = messages.recv().await.unwrap() {
                return v;
            }
        }
    })
    .await
    .unwrap()
}
async fn execution(
    messages: &mut tokio::sync::broadcast::Receiver<Message>,
) -> ciel_sdk::CommandExecution {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Message::CommandExecute(v) = messages.recv().await.unwrap() {
                return v;
            }
        }
    })
    .await
    .unwrap()
}
async fn delivery(
    messages: &mut tokio::sync::broadcast::Receiver<Message>,
) -> ciel_sdk::NotifierDelivery {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Message::NotifierDeliver(v) = messages.recv().await.unwrap() {
                return *v;
            }
        }
    })
    .await
    .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_server_events_commands_notifications_heartbeat_recovery_and_interop() {
    let Some(binary) = std::env::var_os("CIEL_SERVER_BINARY") else {
        eprintln!("Skipped real-server check: set CIEL_SERVER_BINARY");
        return;
    };
    assert!(Path::new(&binary).is_file());
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().to_path_buf();
    fs::create_dir(root.join("web")).unwrap();
    fs::write(root.join("web/index.html"), "SDK test").unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let mut server = Server {
        binary: binary.into(),
        root: root.clone(),
        port,
        process: None,
        http: reqwest::Client::new(),
        cookie: String::new(),
    };
    server.start().await;
    let bootstrap: Value =
        serde_json::from_slice(&fs::read(root.join("server-data/bootstrap-admin.json")).unwrap())
            .unwrap();
    let response = server
        .http
        .post(format!("https://localhost:{port}/api/auth/login"))
        .header("Origin", format!("https://localhost:{port}"))
        .header("X-Ciel-Request", "1")
        .json(&bootstrap)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    server.cookie = response
        .headers()
        .get("Set-Cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let grant = server.grant("rust.worker").await;
    let mut wrong: Value = serde_json::from_slice(&fs::read(&grant).unwrap()).unwrap();
    wrong["server_spki_sha256"] = json!("0".repeat(64));
    let bad = root.join("bad-grant.json");
    fs::write(&bad, serde_json::to_vec(&wrong).unwrap()).unwrap();
    assert_eq!(
        enroll(&bad, root.join("bad-pin")).await.unwrap_err().kind,
        ErrorKind::Tls
    );
    assert_eq!(server.api("GET", "/api/instances", None).await["total"], 0);
    let home = root.join("worker");
    let caller_home = root.join("caller");
    let worker_id = enroll(&grant, &home).await.unwrap();
    enroll(server.grant("rust.caller").await, &caller_home)
        .await
        .unwrap();
    assert_eq!(
        enroll(&grant, &home).await.unwrap_err().code,
        "ALREADY_ENROLLED"
    );
    let worker = Client::connect(&home).await.unwrap();
    let caller = Client::connect(&caller_home).await.unwrap();
    assert_eq!(
        Client::connect(&home).await.err().unwrap().code,
        "IDENTITY_DIRECTORY_IN_USE"
    );
    // An already-connected candidate is not evidence that commit failed.
    let recovery = root.join("recovery");
    fs::create_dir(&recovery).unwrap();
    let candidate = recovery.join(format!("candidate-{}.json", worker_id.instance_id));
    fs::copy(home.join("identity.json"), &candidate).unwrap();
    assert_eq!(
        enroll(&grant, &recovery).await.unwrap_err().code,
        "INSTANCE_ALREADY_CONNECTED"
    );
    assert!(candidate.exists());
    assert!(!recovery.join("identity.json").exists());
    let mut worker_messages = worker.messages();
    let mut caller_messages = caller.messages();
    worker.register_event("demo.changed").await.unwrap();
    caller.subscribe_event("demo.changed").await.unwrap();
    let payload = json!({"text":"你好 😀"});
    let request = new_request_id().unwrap();
    let p1 = worker
        .publish_event("demo.changed", payload.clone(), &request)
        .await
        .unwrap();
    assert_eq!(p1.queued_count, 1);
    assert_eq!(event(&mut caller_messages).await.payload, payload);
    let p2 = worker
        .publish_event("demo.changed", payload.clone(), &request)
        .await
        .unwrap();
    assert_ne!(p1.publication_id, p2.publication_id);
    event(&mut caller_messages).await;
    worker
        .register_command("demo.echo", "Echo JSON")
        .await
        .unwrap();
    server
        .api(
            "PUT",
            &format!(
                "/api/commands/{}/demo.echo/permissions",
                worker_id.instance_id
            ),
            Some(json!({"subjects":[{"kind":"instance","id":caller.identity().instance_id}]})),
        )
        .await;
    let request = new_request_id().unwrap();
    let accepted = caller
        .invoke_command(
            &worker_id.instance_id,
            "demo.echo",
            payload.clone(),
            120,
            &request,
        )
        .await
        .unwrap();
    let execute = execution(&mut worker_messages).await;
    worker
        .respond_command(&execute, true, execute.input.clone())
        .await
        .unwrap();
    let finished = caller
        .await_command(&accepted.id, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(finished.status, CommandStatus::Succeeded);
    assert_eq!(finished.output, payload);
    assert_eq!(
        caller
            .invoke_command(
                &worker_id.instance_id,
                "demo.echo",
                payload.clone(),
                120,
                &request
            )
            .await
            .unwrap()
            .id,
        accepted.id
    );
    assert_eq!(
        caller
            .invoke_command(
                &worker_id.instance_id,
                "demo.echo",
                json!("changed"),
                120,
                &request
            )
            .await
            .unwrap_err()
            .code,
        "REQUEST_ID_CONFLICT"
    );
    worker
        .register_notification("backup.done", "备份完成", "SDK test")
        .await
        .unwrap();
    server
        .api(
            "PUT",
            &format!(
                "/api/notification-services/{}/preferences",
                worker_id.instance_id
            ),
            Some(json!({"enabled":true,"default_channels":["inbox"]})),
        )
        .await;
    let request = new_request_id().unwrap();
    let sent = worker
        .send_notification("backup.done", "备份完成", "已保存", &request)
        .await
        .unwrap();
    assert_eq!(sent.recipient_count, 1);
    assert_eq!(
        sent.deliveries[0].status,
        ciel_sdk::DeliveryStatus::Delivered
    );
    assert_eq!(
        worker
            .send_notification("backup.done", "备份完成", "已保存", &request)
            .await
            .unwrap()
            .id,
        sent.id
    );
    assert_eq!(worker.get_notification(&sent.id).await.unwrap().id, sent.id);
    assert_eq!(
        worker
            .send_notification("backup.done", "changed", "已保存", &request)
            .await
            .unwrap_err()
            .code,
        "REQUEST_ID_CONFLICT"
    );
    // External notifier registration, conflict, admin test, delivery, error code, and no-replay.
    assert_eq!(
        worker
            .register_notifier("email", "Email", "", None)
            .await
            .unwrap_err()
            .code,
        "RESERVED_NOTIFIER"
    );
    worker
        .register_notifier("qq-bot", "QQ", "QQ bot notifier", Some("QQ 号"))
        .await
        .unwrap();
    assert_eq!(
        caller
            .register_notifier("qq-bot", "QQ Conflict", "", None)
            .await
            .unwrap_err()
            .code,
        "NOTIFIER_CONFLICT"
    );
    let test_http = server.http.clone();
    let test_origin = format!("https://localhost:{port}");
    let test_cookie = server.cookie.clone();
    let admin_test = tokio::spawn(async move {
        let response = test_http
            .post(format!("{test_origin}/api/notifiers/qq-bot/test"))
            .header("Origin", test_origin)
            .header("X-Ciel-Request", "1")
            .header("Cookie", test_cookie)
            .json(&json!({"target":"10001"}))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        response.json::<Value>().await.unwrap()
    });
    let test_delivery = delivery(&mut worker_messages).await;
    assert!(test_delivery.delivery_id.starts_with("test-"));
    assert_eq!(test_delivery.notifier_id, "qq-bot");
    assert_eq!(test_delivery.service_id, "ciel:test");
    assert_eq!(test_delivery.target, "10001");
    assert!(!format!("{test_delivery:?}").contains("10001"));
    worker
        .respond_notifier(&test_delivery, true, None)
        .await
        .unwrap();
    assert_eq!(admin_test.await.unwrap()["ok"], true);

    server
        .api(
            "PUT",
            "/api/notifiers/qq-bot",
            Some(json!({"enabled":true})),
        )
        .await;
    server
        .api(
            "PUT",
            "/api/notification-settings/notifiers/qq-bot",
            Some(json!({"target":"10002"})),
        )
        .await;
    server
        .api(
            "PUT",
            &format!(
                "/api/notification-services/{}/preferences",
                worker_id.instance_id
            ),
            Some(json!({"enabled":true,"default_channels":["inbox","qq-bot"]})),
        )
        .await;
    let ext_ok = worker
        .send_notification(
            "backup.done",
            "外部成功",
            "已推送",
            &new_request_id().unwrap(),
        )
        .await
        .unwrap();
    let d_ok = delivery(&mut worker_messages).await;
    assert_eq!(d_ok.notifier_id, "qq-bot");
    assert_eq!(d_ok.target, "10002");
    assert_eq!(d_ok.service_id, "rust.worker");
    worker.respond_notifier(&d_ok, true, None).await.unwrap();
    let ext_ok_status = worker.get_notification(&ext_ok.id).await.unwrap();
    assert!(
        ext_ok_status.deliveries.iter().any(|d| {
            d.notifier_id == "qq-bot" && d.status == ciel_sdk::DeliveryStatus::Delivered
        })
    );

    let ext_fail = worker
        .send_notification(
            "backup.done",
            "外部失败",
            "超时测试",
            &new_request_id().unwrap(),
        )
        .await
        .unwrap();
    let d_fail = delivery(&mut worker_messages).await;
    worker
        .respond_notifier(&d_fail, false, Some("EXTERNAL_TIMEOUT"))
        .await
        .unwrap();
    let ext_fail_status = worker.get_notification(&ext_fail.id).await.unwrap();
    assert!(ext_fail_status.deliveries.iter().any(|d| {
        d.notifier_id == "qq-bot"
            && d.status == ciel_sdk::DeliveryStatus::Failed
            && d.error_code.as_deref() == Some("EXTERNAL_TIMEOUT")
    }));
    let before = server
        .api(
            "GET",
            &format!("/api/instances/{}", worker_id.instance_id),
            None,
        )
        .await["last_seen"]
        .as_i64()
        .unwrap();
    // No consumer work runs during this wait: application traffic cannot replace heartbeat.
    tokio::time::sleep(Duration::from_secs(31)).await;
    let after = server
        .api(
            "GET",
            &format!("/api/instances/{}", worker_id.instance_id),
            None,
        )
        .await["last_seen"]
        .as_i64()
        .unwrap();
    assert!(after > before);
    assert_eq!(worker.state(), State::Ready);
    let accepted = caller
        .invoke_command(
            &worker_id.instance_id,
            "demo.echo",
            Value::Null,
            120,
            &new_request_id().unwrap(),
        )
        .await
        .unwrap();
    let old_execute = execution(&mut worker_messages).await;
    let ext_lost = worker
        .send_notification(
            "backup.done",
            "重启不重放",
            "断线测试",
            &new_request_id().unwrap(),
        )
        .await
        .unwrap();
    let old_delivery = delivery(&mut worker_messages).await;
    server.stop();
    let mut states = worker.states();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if *states.borrow_and_update() == State::Reconnecting {
                break;
            }
            states.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    server.start().await;
    ready(&worker).await;
    ready(&caller).await;
    assert_eq!(
        caller.get_command(&accepted.id).await.unwrap().status,
        CommandStatus::Unknown
    );
    assert_eq!(
        worker
            .respond_command(&old_execute, true, Value::Null)
            .await
            .unwrap_err()
            .code,
        "OLD_CONNECTION"
    );
    assert_eq!(
        worker
            .respond_notifier(&old_delivery, true, None)
            .await
            .unwrap_err()
            .code,
        "OLD_CONNECTION"
    );
    assert!(
        worker
            .get_notification(&ext_lost.id)
            .await
            .unwrap()
            .deliveries
            .iter()
            .any(|d| d.notifier_id == "qq-bot" && d.status == ciel_sdk::DeliveryStatus::Failed)
    );
    assert_eq!(
        caller.get_command(&accepted.id).await.unwrap().status,
        CommandStatus::Unknown
    );
    worker
        .publish_event("demo.changed", payload, &new_request_id().unwrap())
        .await
        .unwrap();
    event(&mut caller_messages).await;
    caller.unsubscribe_event("demo.changed").await.unwrap();
    worker.unregister_event("demo.changed").await.unwrap();
    worker.unregister_command("demo.echo").await.unwrap();
    worker.unregister_notification("backup.done").await.unwrap();
    worker.unregister_notifier("qq-bot").await.unwrap();
    worker.unregister_notifier("qq-bot").await.unwrap();
    // A Java SDK identity has the same on-disk fields, and the reference client shares agent.lock.
    if let Some(peer) = std::env::var_os("CIEL_REFERENCE_CLIENT_BINARY") {
        let scenario = root.join("scenario.json");
        fs::write(
            &scenario,
            r#"[{"send":{"v":1,"type":"heartbeat"},"expect":{"v":1,"type":"heartbeat_ack"}}]"#,
        )
        .unwrap();
        let output = Command::new(&peer)
            .args(["scenario"])
            .arg(&home)
            .arg(&scenario)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(!output.success());
        worker.close().await;
        let output = Command::new(&peer)
            .args(["scenario"])
            .arg(&home)
            .arg(&scenario)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(output.success());
    } else {
        worker.close().await;
    }
    caller.close().await;
    // Recover a committed candidate without consuming the old (already used) grant again.
    fs::rename(
        home.join("identity.json"),
        home.join(format!("candidate-{}.json", worker_id.instance_id)),
    )
    .unwrap();
    // Preserve all offers and skip only candidates explicitly rejected by authentication.
    let mut invalid: Value = serde_json::from_slice(
        &fs::read(home.join(format!("candidate-{}.json", worker_id.instance_id))).unwrap(),
    )
    .unwrap();
    invalid["instance_id"] = json!("0".repeat(32));
    invalid["credential"] = json!("0".repeat(64));
    let uncommitted = home.join(format!("candidate-{}.json", "0".repeat(32)));
    fs::write(&uncommitted, serde_json::to_vec(&invalid).unwrap()).unwrap();
    assert_eq!(
        enroll(&grant, &home).await.unwrap().instance_id,
        worker_id.instance_id
    );
    assert!(uncommitted.exists());
    assert_eq!(worker.state(), State::Closed);
}
