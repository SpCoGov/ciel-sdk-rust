use ciel_sdk::{Client, ErrorKind, managed_config, managed_work_dir};
use serde_json::{Value, json};
use std::{env, fs, process::Command};

#[tokio::test]
async fn managed_environment() {
    const CHILD: &str = "CIEL_SDK_MANAGED_TEST_CHILD";
    if let Ok(mode) = env::var(CHILD) {
        if mode == "missing" {
            assert_eq!(
                managed_work_dir().unwrap_err().code,
                "MANAGED_WORK_DIR_MISSING"
            );
            assert_eq!(
                managed_config::<Value>().unwrap_err().code,
                "MANAGED_CONFIG_PATH_MISSING"
            );
            let error = Client::connect_managed().await.err().unwrap();
            assert_eq!(error.kind, ErrorKind::Storage);
            assert_eq!(error.code, "MANAGED_IDENTITY_DIR_MISSING");
        } else {
            let dir = managed_work_dir().unwrap();
            let config = dir.join("config.json");
            let mut bytes = br#"{"interval":7}"#.to_vec();
            bytes.resize(65536, b' ');
            fs::write(&config, &bytes).unwrap();
            assert_eq!(managed_config::<Value>().unwrap(), json!({"interval":7}));
            assert_eq!(
                managed_config::<String>().unwrap_err().code,
                "INVALID_CONFIG_FILE"
            );
            bytes.push(b' ');
            fs::write(&config, bytes).unwrap();
            assert_eq!(
                managed_config::<Value>().unwrap_err().code,
                "CONFIG_FILE_TOO_LARGE"
            );
            fs::write(&config, b"private-invalid-json").unwrap();
            assert_eq!(
                managed_config::<Value>().unwrap_err().code,
                "INVALID_CONFIG_FILE"
            );
            #[cfg(unix)]
            {
                fs::rename(&config, dir.join("real")).unwrap();
                std::os::unix::fs::symlink(dir.join("real"), &config).unwrap();
                assert_eq!(
                    managed_config::<Value>().unwrap_err().code,
                    "INVALID_CONFIG_FILE"
                );
            }
        }
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    for mode in ["missing", "present"] {
        let mut cmd = Command::new(env::current_exe().unwrap());
        cmd.args(["--exact", "managed_environment"])
            .env(CHILD, mode)
            .env_remove("CIEL_IDENTITY_DIR")
            .env_remove("CIEL_WORK_DIR")
            .env_remove("CIEL_CONFIG_PATH");
        if mode == "present" {
            cmd.env("CIEL_WORK_DIR", temp.path())
                .env("CIEL_CONFIG_PATH", temp.path().join("config.json"));
        }
        assert!(cmd.status().unwrap().success());
    }
}
