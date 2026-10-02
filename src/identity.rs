use crate::{Error, ErrorKind, Options, Result, protocol, tls};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

/// 已持久化的实例身份。Debug 隐藏 credential，序列化用于与 Java/参考客户端互通。
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    /// `wss://host:port/ws/service` 地址。
    pub server: String,
    /// 服务器 SPKI SHA-256，小写十六进制。
    pub server_spki_sha256: String,
    /// 服务 ID。
    pub service_id: String,
    /// 实例 ID。
    pub instance_id: String,
    pub(crate) credential: String,
}
impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identity")
            .field("service_id", &self.service_id)
            .field("instance_id", &self.instance_id)
            .finish_non_exhaustive()
    }
}
impl Identity {
    pub(crate) fn validate(&self) -> Result<()> {
        protocol::address(&self.server)?;
        protocol::hex(&self.server_spki_sha256, 64)?;
        protocol::id(&self.service_id)?;
        protocol::hex(&self.instance_id, 32)?;
        protocol::hex(&self.credential, 64)
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Grant {
    server: String,
    server_spki_sha256: String,
    service_id: String,
    enrollment_token: String,
    #[serde(default, rename = "id")]
    _id: Option<String>,
    #[serde(default, rename = "expires_at")]
    _expires_at: Option<i64>,
}
fn storage(code: &str) -> Error {
    Error::new(ErrorKind::Storage, code)
}
fn safe_path(path: &Path) -> Result<()> {
    for parent in path.ancestors() {
        match fs::symlink_metadata(parent) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(storage("SYMLINK_NOT_ALLOWED"));
                }
                #[cfg(windows)]
                {
                    use std::os::windows::fs::MetadataExt;
                    if meta.file_attributes() & 0x400 != 0 {
                        return Err(storage("SYMLINK_NOT_ALLOWED"));
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(storage("IDENTITY_IO_FAILED")),
        }
    }
    Ok(())
}
fn protect(path: &Path, directory: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            path,
            fs::Permissions::from_mode(if directory { 0o700 } else { 0o600 }),
        )
        .map_err(|_| storage("PRIVATE_PERMISSIONS_FAILED"))?;
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // Use the same owner-only ACL as CIEL, with a hidden native PowerShell process.
        let script = "$ErrorActionPreference='Stop'; $sid=[System.Security.Principal.WindowsIdentity]::GetCurrent().User; if($env:CIEL_SDK_IS_DIR -eq '1'){$acl=New-Object System.Security.AccessControl.DirectorySecurity; $rule=New-Object System.Security.AccessControl.FileSystemAccessRule($sid,'FullControl','ContainerInherit,ObjectInherit','None','Allow')}else{$acl=New-Object System.Security.AccessControl.FileSecurity; $rule=New-Object System.Security.AccessControl.FileSystemAccessRule($sid,'FullControl','Allow')}; $acl.SetAccessRuleProtection($true,$false); $acl.AddAccessRule($rule); if($env:CIEL_SDK_IS_DIR -eq '1'){[System.IO.Directory]::SetAccessControl($env:CIEL_SDK_PRIVATE_PATH,$acl)}else{[System.IO.File]::SetAccessControl($env:CIEL_SDK_PRIVATE_PATH,$acl)}";
        let output = std::process::Command::new("powershell.exe")
            .args(["-NoProfile", "-NonInteractive", "-Command", script])
            .env("CIEL_SDK_PRIVATE_PATH", path)
            .env("CIEL_SDK_IS_DIR", if directory { "1" } else { "0" })
            .creation_flags(0x08000000)
            .output()
            .map_err(|_| storage("PRIVATE_PERMISSIONS_FAILED"))?;
        if !output.status.success() {
            return Err(storage("PRIVATE_PERMISSIONS_FAILED"));
        }
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = (path, directory);
        return Err(storage("PRIVATE_PERMISSIONS_UNSUPPORTED"));
    }
    Ok(())
}
pub(crate) fn read<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    safe_path(path)?;
    let file = File::open(path).map_err(|_| storage("IDENTITY_READ_FAILED"))?;
    if !file
        .metadata()
        .map_err(|_| storage("IDENTITY_READ_FAILED"))?
        .is_file()
    {
        return Err(storage("INVALID_IDENTITY_FILE"));
    }
    let mut bytes = Vec::new();
    file.take(16385)
        .read_to_end(&mut bytes)
        .map_err(|_| storage("IDENTITY_READ_FAILED"))?;
    if bytes.len() > 16384 {
        return Err(storage("IDENTITY_FILE_TOO_LARGE"));
    }
    let value = protocol::parse(&bytes).map_err(|_| storage("INVALID_IDENTITY_FILE"))?;
    serde_json::from_value(value).map_err(|_| storage("INVALID_IDENTITY_FILE"))
}
pub(crate) struct Store {
    pub(crate) dir: PathBuf,
    _lock: File,
}
impl Store {
    pub(crate) fn open(dir: &Path, create: bool) -> Result<Self> {
        let dir = std::path::absolute(dir).map_err(|_| storage("INVALID_IDENTITY_DIRECTORY"))?;
        if dir.parent().is_none() {
            return Err(storage("INVALID_IDENTITY_DIRECTORY"));
        }
        safe_path(&dir)?;
        if create {
            fs::create_dir_all(&dir).map_err(|_| storage("IDENTITY_IO_FAILED"))?;
        }
        if !dir.is_dir() {
            return Err(storage("IDENTITY_DIRECTORY_MISSING"));
        }
        protect(&dir, true)?;
        let path = dir.join("agent.lock");
        safe_path(&path)?;
        let mut options = OpenOptions::new();
        options.create(true).truncate(false).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options
            .open(&path)
            .map_err(|_| storage("IDENTITY_LOCK_FAILED"))?;
        protect(&path, false)?;
        lock.try_lock()
            .map_err(|_| storage("IDENTITY_DIRECTORY_IN_USE"))?;
        Ok(Self { dir, _lock: lock })
    }
    pub(crate) fn identity(&self) -> Result<Identity> {
        let path = self.dir.join("identity.json");
        safe_path(&path)?;
        protect(&path, false)?;
        let identity: Identity = read(&path)?;
        identity.validate()?;
        Ok(identity)
    }
    fn save(&self, identity: &Identity) -> Result<PathBuf> {
        identity.validate()?;
        let target = self
            .dir
            .join(format!("candidate-{}.json", identity.instance_id));
        safe_path(&target)?;
        if target.exists() {
            return Err(storage("CANDIDATE_ALREADY_EXISTS"));
        }
        let temp = self
            .dir
            .join(format!("pending-{}.tmp", protocol::new_request_id()?));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temp)
            .map_err(|_| storage("IDENTITY_WRITE_FAILED"))?;
        protect(&temp, false)?; // ACL must be private before any credential is written.
        file.write_all(
            &serde_json::to_vec_pretty(identity).map_err(|_| storage("IDENTITY_WRITE_FAILED"))?,
        )
        .map_err(|_| storage("IDENTITY_WRITE_FAILED"))?;
        file.sync_all()
            .map_err(|_| storage("IDENTITY_WRITE_FAILED"))?;
        drop(file);
        fs::rename(&temp, &target).map_err(|_| storage("IDENTITY_WRITE_FAILED"))?;
        self.sync()?;
        Ok(target)
    }
    fn sync(&self) -> Result<()> {
        #[cfg(unix)]
        File::open(&self.dir)
            .and_then(|f| f.sync_all())
            .map_err(|_| storage("IDENTITY_WRITE_FAILED"))?;
        Ok(())
    }
    fn activate(&self, candidate: &Path) -> Result<()> {
        let target = self.dir.join("identity.json");
        safe_path(&target)?;
        if target.exists() {
            return Err(storage("ALREADY_ENROLLED"));
        }
        fs::rename(candidate, target).map_err(|_| storage("IDENTITY_WRITE_FAILED"))?;
        self.sync()
    }
}
/// 使用 WebUI 导出的授权 JSON 注册并保存身份。
///
/// 在发送 commit 前私有、持久化保存 candidate。确认丢失时再次调用本方法：
/// 先逐个认证所有 candidate，只有明确 AUTHENTICATION_FAILED 才继续注册。
/// 已有 identity.json 时返回 ALREADY_ENROLLED，不覆盖凭据。
pub async fn enroll(grant: impl AsRef<Path>, directory: impl AsRef<Path>) -> Result<Identity> {
    let grant = grant.as_ref().to_path_buf();
    let dir = directory.as_ref().to_path_buf();
    let (store, grant, candidates) = tokio::task::spawn_blocking(move || -> Result<_> {
        let store = Store::open(&dir, true)?;
        let active = store.dir.join("identity.json");
        safe_path(&active)?;
        if active.exists() {
            return Err(storage("ALREADY_ENROLLED"));
        }
        let grant: Grant = read(&grant)?;
        protocol::address(&grant.server)?;
        protocol::hex(&grant.server_spki_sha256, 64)?;
        protocol::id(&grant.service_id)?;
        protocol::hex(&grant.enrollment_token, 64)?;
        let mut candidates = Vec::new();
        for entry in fs::read_dir(&store.dir).map_err(|_| storage("IDENTITY_READ_FAILED"))? {
            let entry = entry.map_err(|_| storage("IDENTITY_READ_FAILED"))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("candidate-") && name.ends_with(".json") {
                safe_path(&entry.path())?;
                protect(&entry.path(), false)?;
                let identity: Identity = read(&entry.path())?;
                identity.validate()?;
                if name != format!("candidate-{}.json", identity.instance_id)
                    || identity.server != grant.server
                    || identity.server_spki_sha256 != grant.server_spki_sha256
                    || identity.service_id != grant.service_id
                {
                    return Err(storage("CANDIDATE_TARGET_MISMATCH"));
                }
                candidates.push((entry.path(), identity));
            }
        }
        candidates.sort_by(|a, b| a.0.cmp(&b.0));
        Ok((store, grant, candidates))
    })
    .await
    .map_err(|_| storage("IDENTITY_TASK_FAILED"))??;
    for (path, identity) in candidates {
        match tls::authenticate(&identity, &Options::default()).await {
            Ok((mut socket, _)) => {
                let identity = tokio::task::spawn_blocking(move || {
                    store.activate(&path)?;
                    Ok(identity)
                })
                .await
                .map_err(|_| storage("IDENTITY_TASK_FAILED"))?;
                let _ = socket.close(None).await;
                return identity;
            }
            Err(e) if e.kind == ErrorKind::Server && e.code == "AUTHENTICATION_FAILED" => {}
            Err(e) => return Err(e),
        }
    }
    let mut socket = tls::connect(
        &grant.server,
        &grant.server_spki_sha256,
        Options::default().connect_timeout,
    )
    .await?;
    let offer = tls::exchange(
        &mut socket,
        json!({"v":1,"type":"enroll","service_id":grant.service_id,"token":grant.enrollment_token}),
    )
    .await?;
    protocol::expected(&offer, "enrollment_offer")?;
    if offer["service_id"] != grant.service_id {
        return Err(protocol::bad());
    }
    let identity = Identity {
        server: grant.server,
        server_spki_sha256: grant.server_spki_sha256,
        service_id: grant.service_id,
        instance_id: protocol::string(&offer, "instance_id")?.into(),
        credential: protocol::string(&offer, "credential")?.into(),
    };
    identity.validate().map_err(|_| protocol::bad())?;
    let (store, path) = {
        let pending = identity.clone();
        tokio::task::spawn_blocking(move || {
            let path = store.save(&pending)?;
            Ok::<_, Error>((store, path))
        })
        .await
        .map_err(|_| storage("IDENTITY_TASK_FAILED"))??
    };
    let reply = tls::exchange(
        &mut socket,
        json!({"v":1,"type":"enroll_commit","credential":identity.credential}),
    )
    .await?;
    protocol::expected(&reply, "enrolled")?;
    if reply["service_id"] != identity.service_id || reply["instance_id"] != identity.instance_id {
        return Err(protocol::bad());
    }
    tokio::task::spawn_blocking(move || {
        store.activate(&path)?;
        Ok(identity)
    })
    .await
    .map_err(|_| storage("IDENTITY_TASK_FAILED"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn durable_identity_and_shared_lock() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("identity");
        let store = Store::open(&dir, true).unwrap();
        assert_eq!(
            Store::open(&dir, false).err().unwrap().code,
            "IDENTITY_DIRECTORY_IN_USE"
        );
        let identity = Identity {
            server: "wss://localhost:7443/ws/service".into(),
            server_spki_sha256: "a".repeat(64),
            service_id: "rust.test".into(),
            instance_id: "b".repeat(32),
            credential: "c".repeat(64),
        };
        let candidate = store.save(&identity).unwrap();
        assert!(!dir.join("identity.json").exists());
        assert!(!format!("{identity:?}").contains(&identity.credential));
        store.activate(&candidate).unwrap();
        assert_eq!(store.identity().unwrap().instance_id, identity.instance_id);
        assert_eq!(
            store
                .save(&identity)
                .and_then(|p| store.activate(&p))
                .unwrap_err()
                .code,
            "ALREADY_ENROLLED"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(dir.join("identity.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        #[cfg(windows)]
        {
            let output = std::process::Command::new("powershell.exe").args(["-NoProfile","-NonInteractive","-Command","$a=[System.IO.File]::GetAccessControl($env:CIEL_TEST_IDENTITY); if(-not $a.AreAccessRulesProtected -or $a.Access.Count -ne 1){exit 1}"])
                .env("CIEL_TEST_IDENTITY",dir.join("identity.json")).output().unwrap();
            assert!(output.status.success());
        }
        drop(store);
        assert!(Store::open(&dir, false).is_ok());
    }
}
