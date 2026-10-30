//! Independent framed input and output with joined application/output drain.
use std::future::Future;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use super::super::framing::{
    FrameError, read_client_message, write_server_message,
};
use crate::types::messages::{ClientMessage, ServerMessage};

fn session_ended(error: &FrameError) -> bool {
    match error {
        FrameError::Io(error) => {
            claviron::edge::webtransport_session_ended(error)
        }
        FrameError::Invalid(_) => false,
    }
}

pub(super) async fn drive<S>(
    stream: S,
    queued: mpsc::Receiver<ServerMessage>,
    events: mpsc::Sender<ClientMessage>,
    max_frame: usize,
    shutdown: impl Future<Output = ()>,
    valid: impl Fn() -> bool,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut reader, writer) = tokio::io::split(stream);
    let (finish, closing) = oneshot::channel();
    let writing = write(writer, queued, max_frame, closing, &valid);
    tokio::pin!(writing, shutdown);
    let mut writer_done = false;
    let mut inbound = BytesMut::new();
    let mut pending = None;
    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => break,
            () = events.closed() => break,
            () = &mut writing => { writer_done = true; break; },
            permit = events.reserve(), if pending.is_some() => {
                match permit {
                    Ok(permit) => permit.send(pending.take().expect("pending message")),
                    Err(_) => break,
                }
            },
            message = read_client_message(&mut reader, &mut inbound, max_frame), if pending.is_none() => {
                match message {
                    Ok(Some(message)) if valid() => pending = Some(message),
                    Ok(_) => break,
                    Err(error) => {
                        if session_ended(&error) {
                            tracing::debug!(%error, "WebTransport chat input ended");
                        } else {
                            tracing::warn!(%error, "WebTransport chat input failed");
                        }
                        break;
                    }
                }
            },
        }
    }
    let _ = finish.send(());
    let notifying = async move {
        if let Some(message) = pending {
            let _ = events.send(message).await;
        }
        // Dropping the sender ends input only after the pending frame.
        drop(events);
    };
    let draining = async {
        if !writer_done {
            writing.await;
        }
    };
    // Closing output wakes blocked application senders even when the peer
    // cannot read. Never stop polling the writer while input handlers drain.
    tokio::join!(notifying, draining);
}

async fn write<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut queued: mpsc::Receiver<ServerMessage>,
    max_frame: usize,
    mut closing: oneshot::Receiver<()>,
    valid: impl Fn() -> bool,
) {
    let mut draining = false;
    loop {
        let next = tokio::select! {
            biased;
            _ = &mut closing, if !draining => {
                queued.close();
                draining = true;
                continue;
            },
            next = queued.recv() => next,
        };
        let Some(message) = next else {
            break;
        };
        if !valid() {
            return;
        }
        let sending = write_server_message(&mut writer, message, max_frame);
        tokio::pin!(sending);
        let result = tokio::select! {
            biased;
            _ = &mut closing, if !draining => {
                queued.close();
                draining = true;
                // Preserve the exact in-flight frame, including partial writes.
                sending.await
            },
            result = &mut sending => result,
        };
        if let Err(error) = result {
            if session_ended(&error) {
                tracing::debug!(%error, "WebTransport chat output ended");
            } else {
                tracing::error!(%error, "WebTransport chat output failed");
            }
            return;
        }
        #[cfg(feature = "metrics")]
        crate::svc::metrics::CHAT_METRICS.message_sent();
    }
    if let Err(error) = writer.shutdown().await {
        tracing::debug!(%error, "WebTransport chat output shutdown failed");
    }
}

#[cfg(test)]
#[path = "../../../../tests/svc/web/web_transport/io.rs"]
mod tests;
