//! Enrollment, event publishing, echo command provider and caller using the SDK.
use ciel_sdk::{Client, Message, Result, enroll, new_request_id};
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("enroll") if args.len() == 4 => {
            let identity = enroll(&args[2], &args[3]).await?;
            println!("{} {}", identity.service_id, identity.instance_id);
        }
        Some("publish") if args.len() == 3 => {
            let client = Client::connect(&args[2]).await?;
            client.register_event("demo.changed").await?;
            println!(
                "{:?}",
                client
                    .publish_event("demo.changed", json!({"value":42}), &new_request_id()?)
                    .await?
            );
            client.close().await;
        }
        Some("serve") if args.len() == 3 => {
            let client = Client::connect(&args[2]).await?;
            let mut messages = client.messages();
            client
                .register_command("demo.echo", "Return input JSON unchanged")
                .await?;
            println!("READY {}", client.identity().instance_id);
            loop {
                let message = tokio::select! { _=tokio::signal::ctrl_c()=>break, message=messages.recv()=>message };
                match message {
                    Ok(Message::CommandExecute(execution)) => {
                        if let Err(error) = client
                            .respond_command(&execution, true, execution.input.clone())
                            .await
                        {
                            eprintln!("{error}");
                        }
                    }
                    Ok(Message::Error(error)) => eprintln!("{error}"),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("Missed {n} messages")
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    _ => {}
                }
            }
            client.close().await;
        }
        Some("call") if args.len() == 4 => {
            let client = Client::connect(&args[2]).await?;
            // Persist this request ID in your application before submitting a real business call.
            let request_id = new_request_id()?;
            let accepted = client
                .invoke_command(&args[3], "demo.echo", json!({"value":42}), 30, &request_id)
                .await?;
            println!(
                "{:?}",
                client
                    .await_command(&accepted.id, Duration::from_secs(35))
                    .await?
            );
            client.close().await;
        }
        _ => eprintln!(
            "Usage: cargo run --example client -- enroll <grant.json> <identity-dir> | publish <identity-dir> | serve <identity-dir> | call <identity-dir> <target-instance-id>"
        ),
    }
    Ok(())
}
