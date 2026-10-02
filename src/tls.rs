use crate::{Error, ErrorKind, Identity, Options, Result, protocol};
use futures_util::{SinkExt, StreamExt};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async_tls_with_config,
    tungstenite::{self, Message},
};
pub(crate) type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

// Adapted from CIEL's src/tls.rs: the provisioned SPKI is the trust anchor.
#[derive(Debug)]
struct PinVerifier(String);
impl rustls::client::danger::ServerCertVerifier for PinVerifier {
    fn verify_server_cert(
        &self,
        cert: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let (rest, parsed) = x509_parser::parse_x509_certificate(cert).map_err(|_| {
            rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding)
        })?;
        if !rest.is_empty() || format!("{:x}", Sha256::digest(parsed.public_key().raw)) != self.0 {
            return Err(rustls::Error::General(
                "TLS_SERVER_IDENTITY_MISMATCH".into(),
            ));
        }
        let now = now.as_secs() as i64;
        if now < parsed.validity().not_before.timestamp() {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidYet,
            ));
        }
        if now > parsed.validity().not_after.timestamp() {
            return Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::Expired,
            ));
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("TLS_1_3_REQUIRED".into()))
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
fn config(pin: &str) -> Result<Arc<rustls::ClientConfig>> {
    protocol::hex(pin, 64)?;
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .map_err(|_| Error::new(ErrorKind::Tls, "TLS_CONFIGURATION"))?
    .dangerous()
    .with_custom_certificate_verifier(Arc::new(PinVerifier(pin.into())))
    .with_no_client_auth();
    config.enable_early_data = false;
    config.resumption = rustls::client::Resumption::disabled();
    Ok(Arc::new(config))
}
pub(crate) fn transport_error(error: tungstenite::Error) -> Error {
    use std::io::ErrorKind as Io;
    match error {
        tungstenite::Error::Io(e) if matches!(e.kind(), Io::InvalidData | Io::InvalidInput) => {
            Error::new(ErrorKind::Tls, "TLS_HANDSHAKE_FAILED")
        }
        tungstenite::Error::Io(e)
            if matches!(
                e.kind(),
                Io::ConnectionRefused
                    | Io::ConnectionReset
                    | Io::ConnectionAborted
                    | Io::NotConnected
                    | Io::BrokenPipe
                    | Io::TimedOut
                    | Io::UnexpectedEof
                    | Io::NetworkUnreachable
                    | Io::HostUnreachable
            ) =>
        {
            Error::connection("CONNECTION_LOST")
        }
        tungstenite::Error::ConnectionClosed
        | tungstenite::Error::AlreadyClosed
        | tungstenite::Error::Protocol(
            tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
        ) => Error::connection("CONNECTION_CLOSED"),
        tungstenite::Error::Http(response) => {
            let status = response.status();
            let mut error = Error::new(
                ErrorKind::Connection,
                &format!("UPGRADE_{}", status.as_u16()),
            );
            error.retryable = status.is_server_error() || status.as_u16() == 429;
            error.retry_after_seconds = response
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            error
        }
        tungstenite::Error::Tls(_) => Error::new(ErrorKind::Tls, "TLS_HANDSHAKE_FAILED"),
        _ => protocol::bad(),
    }
}
pub(crate) async fn connect(server: &str, pin: &str, timeout: Duration) -> Result<Socket> {
    protocol::address(server)?;
    let config = config(pin)?;
    let ws = tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(32768))
        .max_frame_size(Some(32768));
    let (socket, _) = tokio::time::timeout(
        timeout,
        connect_async_tls_with_config(server, Some(ws), false, Some(Connector::Rustls(config))),
    )
    .await
    .map_err(|_| Error::timeout())?
    .map_err(transport_error)?;
    Ok(socket)
}
pub(crate) async fn send(
    socket: &mut Socket,
    request: serde_json::Value,
    timeout: Duration,
) -> Result<()> {
    tokio::time::timeout(
        timeout,
        socket.send(Message::Text(protocol::encode(&request)?.into())),
    )
    .await
    .map_err(|_| Error::timeout())?
    .map_err(transport_error)
}
pub(crate) async fn receive(socket: &mut Socket, timeout: Duration) -> Result<serde_json::Value> {
    tokio::time::timeout(timeout, async {
        loop {
            match socket
                .next()
                .await
                .ok_or_else(|| Error::connection("CONNECTION_CLOSED"))?
                .map_err(transport_error)?
            {
                Message::Text(text) => return protocol::response(text.as_bytes()),
                Message::Ping(_) => {
                    socket.flush().await.map_err(transport_error)?;
                }
                Message::Pong(_) => {}
                Message::Close(_) => return Err(Error::connection("CONNECTION_CLOSED")),
                _ => return Err(protocol::bad()),
            }
        }
    })
    .await
    .map_err(|_| Error::timeout())?
}
pub(crate) async fn exchange(
    socket: &mut Socket,
    request: serde_json::Value,
) -> Result<serde_json::Value> {
    send(socket, request, Duration::from_secs(10)).await?;
    receive(socket, Duration::from_secs(15)).await
}
pub(crate) async fn authenticate(
    identity: &Identity,
    options: &Options,
) -> Result<(Socket, serde_json::Value)> {
    let mut socket = connect(
        &identity.server,
        &identity.server_spki_sha256,
        options.connect_timeout,
    )
    .await?;
    send(&mut socket, serde_json::json!({"v":1,"type":"authenticate","service_id":identity.service_id,"instance_id":identity.instance_id,"credential":identity.credential}), options.connect_timeout).await?;
    let reply = receive(&mut socket, options.connect_timeout).await?;
    protocol::expected(&reply, "authenticated")?;
    protocol::hex(protocol::string(&reply, "session_id")?, 32).map_err(|_| protocol::bad())?;
    let interval = reply["heartbeat_interval_seconds"]
        .as_u64()
        .ok_or_else(protocol::bad)?;
    let timeout = reply["heartbeat_timeout_seconds"]
        .as_u64()
        .ok_or_else(protocol::bad)?;
    if interval == 0 || interval >= timeout || timeout > 86400 {
        return Err(protocol::bad());
    }
    Ok((socket, reply))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn rejects_wrong_pin_expired_certificate_and_tls12_before_application_data() {
        for (expired, tls12, wrong_pin) in [
            (false, false, true),
            (true, false, false),
            (false, true, false),
        ] {
            let mut params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
            if expired {
                params.not_before = rcgen::date_time_ymd(2020, 1, 1);
                params.not_after = rcgen::date_time_ymd(2021, 1, 1);
            }
            let key = rcgen::KeyPair::generate().unwrap();
            let cert = params.self_signed(&key).unwrap();
            let (_, parsed) = x509_parser::parse_x509_certificate(cert.der()).unwrap();
            let pin = if wrong_pin {
                "0".repeat(64)
            } else {
                format!("{:x}", Sha256::digest(parsed.public_key().raw))
            };
            let versions = if tls12 {
                vec![&rustls::version::TLS12]
            } else {
                vec![&rustls::version::TLS13]
            };
            let config = rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_protocol_versions(&versions)
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
            )
            .unwrap();
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = tokio::spawn(async move {
                let (tcp, _) = listener.accept().await.unwrap();
                // A TLS verification failure must prevent both Upgrade and any credential bytes.
                assert!(acceptor.accept(tcp).await.is_err());
            });
            let error = connect(
                &format!("wss://localhost:{port}/ws/service"),
                &pin,
                Duration::from_secs(3),
            )
            .await
            .err()
            .unwrap();
            assert_eq!(error.kind, ErrorKind::Tls);
            assert!(!error.retryable);
            server.await.unwrap();
        }
    }
}
