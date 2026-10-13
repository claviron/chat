use std::sync::Arc;

use claviron::runtime::gate::{GateDrainOutcome, create_gate};
use claviron::runtime::registry::Error;
use claviron::runtime::{AppState, RunContext};
use claviron::ssl::parsers::take_first_cert_from_der;
use claviron::sys::net::{PortSharing, create_udp_socket};
use claviron::tls::auth::create_auth_config;
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Endpoint, ServerConfig, TransportConfig};
use tracing::{debug, info, trace, warn};

use crate::config::ChatConfig;
use crate::svc::quic::ALPN;
use crate::svc::quic::session::handle_quic_chat_stream;
use crate::svc::shared::{derive_chat_resolver, validate_idle_timeout};

pub(crate) async fn start_quic_service(
    state: AppState,
    cfg: ChatConfig,
    context: RunContext,
) -> claviron::runtime::registry::Result<()> {
    if !cfg.enabled {
        info!("Chat service disabled in config");
        return Ok(());
    }

    // Chat over Quic transport is starting

    let Some(quic_cfg) = cfg.quic_cfg() else {
        warn!("Raw QUIC config for chat service is not configured/enabled");
        return Ok(());
    };
    let tls_timeout = quic_cfg.handshake_timeout;
    let transport_cfg = &quic_cfg.transport;

    let chat_resolver = derive_chat_resolver(&state, &quic_cfg.hosts)?;

    let cert_names = chat_resolver.hostnames();

    let ca_path = cfg.ca_path().ok_or_else(|| {
        Error::msg("CA Path for Quic Chat Service is missing")
    })?;

    let tls_config = create_auth_config(
        ca_path,
        chat_resolver,
        vec![ALPN.to_vec()],
        cfg.policy_cfg(),
    )?;

    let addr = cfg.address;

    let quic = QuicServerConfig::try_from(Arc::new(tls_config))
        .map_err(|e| Error::msg(format!("Quic Setup Error {}", e)))?;

    trace!("Raw chat QUIC loading Quinn configuration");
    let mut server_cfg = ServerConfig::with_crypto(Arc::new(quic));

    // Configure QUIC transport keep-alive and idle timeout using chat config.
    // We reuse WebSocket ping settings for QUIC too.

    // Validate: require keep_alive_interval and enforce relation with idle_timeout
    let keep_alive = transport_cfg.keep_alive_interval.ok_or_else(|| {
        Error::msg("QUIC keep_alive_interval must be configured (non-optional) for chat")
    })?;

    validate_idle_timeout(
        "quic",
        "keep_alive_interval",
        keep_alive,
        transport_cfg.idle_timeout,
    )?;

    let mut transport = TransportConfig::default();
    transport
        .max_concurrent_bidi_streams(
            transport_cfg.max_concurrent_bidi_streams.into(),
        )
        .max_concurrent_uni_streams(
            transport_cfg.max_concurrent_uni_streams.into(),
        )
        .max_idle_timeout(Some(transport_cfg.idle_timeout.try_into()?))
        .keep_alive_interval(Some(keep_alive));

    server_cfg.transport_config(Arc::new(transport));

    let std_socket =
        create_udp_socket(addr, PortSharing::Shared).map_err(|e| {
            Error::msg(format!("QUIC Chat service bind {addr}: {e}"))
        })?;
    let endpoint = Endpoint::new(
        Default::default(),
        Some(server_cfg.clone()),
        std_socket,
        Arc::new(quinn::TokioRuntime),
    )
    .map_err(|e| {
        Error::msg(format!("QUIC Chat service endpoint {addr}: {e}"))
    })?;

    info!(
        "🌐 QUIC Chat service started cert_hosts={:?} address={}",
        cert_names, addr
    );

    // Shared connection handle (global across all endpoints)
    let handle = create_gate(
        context.cancellation_token(),
        quic_cfg.graceful_timeout,
        quic_cfg.max_connections.map(|limit| limit.get()),
        quic_cfg.acquire_timeout,
    );

    // Run accept loop until shutdown signal arrives
    let state_clone = state.clone();
    let mut connections = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            biased;
            // Shutdown signal
            _ = context.cancelled() => {
                debug!("🔻 Chat service (quic) generation stopping");
                // Close the endpoint to stop accepting new connections
                endpoint.close(0u32.into(), b"service stopping");
                break;
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = completed {
                    warn!(%error, "QUIC connection task failed");
                }
            }
            // Accept new incoming connections
            maybe_conn = endpoint.accept() => {
                let Some(connecting) = maybe_conn else {
                    break; // endpoint was closed
                };

                let state = state_clone.clone();
                let handle = handle.clone();
                connections.spawn(async move {
                    // QUIC handshake with timeout (QUIC includes TLS handshake)
                    let conn_result = tokio::time::timeout(tls_timeout, connecting).await;

                    let connection = match conn_result {
                        Ok(Ok(c)) =>c,
                        Ok(Err(e)) => {
                            warn!(%e, "QUIC handshake failed");
                            return;
                        }
                        Err(_) => {
                            warn!(timeout = ?tls_timeout, "QUIC handshake timeout");
                            return;
                        }
                    };

                    let cn = match extract_client_cn(&connection) {
                        Some(c)  => {
                                info!(cn = c.as_str(), "🔐 Client certificate CN (handshake)");
                                c
                        }
                        None =>  {
                            warn!("🚫 Client certificate identity missing; closing connection");
                            connection.close(0u32.into(), b"client cert required");
                            return;
                        }
                    };

                    // Accept incoming bidi streams for this connection
                    let mut streams = tokio::task::JoinSet::new();
                    loop {
                        tokio::select! {
                            completed = streams.join_next(), if !streams.is_empty() => {
                                if let Some(Err(error)) = completed {
                                    warn!(%error, "QUIC stream task failed");
                                }
                            }
                            accepted = connection.accept_bi() => match accepted {
                            Ok(( send, recv)) => {
                                let state = state.clone();
                                match handle.enter().await {
                                    Ok(watcher) => {
                                        let cn = cn.clone();
                                        streams.spawn(async move {
                                            if let Err(e) = handle_quic_chat_stream(send, recv, state, cn,watcher).await {
                                                warn!(%e, "QUIC stream handler failed");
                                            }});
                                    }
                                    Err(e) => {
                                        warn!("Connection/stream limit reached; dropping incoming stream :{:?}",e );
                                        // dropping send/recv here rejects the stream gracefully
                                    }
                                }
                            }
                            Err(quinn::ConnectionError::ApplicationClosed { .. })
                            | Err(quinn::ConnectionError::LocallyClosed)
                            | Err(quinn::ConnectionError::ConnectionClosed(_)) => {
                                debug!("QUIC connection closed");
                                break;
                            }
                            Err(e) => {
                                warn!(%e, "accept_bi failed");
                                break;
                            }
                            }
                        }
                    }
                    while let Some(completed) = streams.join_next().await {
                        if let Err(error) = completed {
                            warn!(%error, "QUIC stream task failed during drain");
                        }
                    }
                });
            }
        }
    }
    if handle.count() > 0
        && let GateDrainOutcome::Forced { remaining } =
            handle.wait_all_done().await
    {
        warn!(
            remaining,
            "QUIC chat graceful drain expired; forced shutdown signalled"
        );
    }
    // Drain existing connections before returning
    endpoint.wait_idle().await;
    while let Some(completed) = connections.join_next().await {
        if let Err(error) = completed {
            warn!(%error, "QUIC connection task failed during drain");
        }
    }

    info!("❎ Chat service (quic) stopped");
    Ok(())
}

fn extract_client_cn(conn: &quinn::Connection) -> Option<String> {
    let id = conn.peer_identity()?;

    // downcast to cert chain
    let chain = id
        .downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
        .ok()?;
    let cert = chain.first()?;

    // parse and extract CN
    let x509 = take_first_cert_from_der(cert).ok()?;
    let cn = x509
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())?;
    Some(cn.to_string())
}
