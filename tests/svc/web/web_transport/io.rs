use std::future::{Future, pending};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::svc::web::framing::encode_server_message;

fn assert_pending(future: Pin<&mut impl Future>) {
    assert!(future.poll(&mut Context::from_waker(Waker::noop())).is_pending());
}

fn command(room: &str) -> Vec<u8> {
    let payload = rmp_serde::to_vec_named(&serde_json::json!({
        "type": "join_room", "room": room,
    }))
    .unwrap();
    let mut bytes = (payload.len() as u32).to_be_bytes().to_vec();
    bytes.extend(payload);
    bytes
}

#[tokio::test]
async fn slow_output_keeps_input_and_shutdown_live_and_drains_exact_frames() {
    let (stream, mut peer) = tokio::io::duplex(16);
    let (outbound, queued) = mpsc::channel(1);
    let (events, mut happened) = mpsc::channel(1);
    let (stop, stopped) = oneshot::channel();
    let first = ServerMessage::error("x".repeat(64));
    let second = ServerMessage::error("second");
    let mut expected = encode_server_message(&first, 128).unwrap().to_vec();
    expected.extend(encode_server_message(&second, 128).unwrap());
    outbound.send(first).await.unwrap();
    let driving = drive(
        stream,
        queued,
        events,
        128,
        async {
            stopped.await.unwrap();
        },
        || true,
    );
    tokio::pin!(driving);
    assert_pending(driving.as_mut()); // First frame partially written.
    outbound.send(second).await.unwrap();
    let blocked = outbound.send(ServerMessage::error("not admitted"));
    tokio::pin!(blocked);
    assert_pending(blocked.as_mut());

    {
        let bytes = command("room");
        let input = peer.write_all(&bytes);
        tokio::pin!(input);
        // Explicitly drive both halves; no timer is needed to release input.
        std::future::poll_fn(|cx| {
            assert!(driving.as_mut().poll(cx).is_pending());
            match input.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    result.unwrap();
                    Poll::Ready(())
                }
                Poll::Pending => Poll::Pending,
            }
        })
        .await;
    }
    assert_pending(driving.as_mut());
    assert!(
        matches!(happened.try_recv(), Ok(ClientMessage::JoinRoom { room }) if room == "room")
    );

    stop.send(()).unwrap();
    assert_pending(driving.as_mut());
    assert!(outbound.is_closed());
    assert!(blocked.await.is_err());
    assert!(happened.recv().await.is_none());
    let receiving = async {
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, expected);
    };
    tokio::join!(driving, receiving);
}

#[tokio::test]
async fn full_input_queue_preserves_pending_frame_and_stops_reading_on_drain() {
    let (stream, mut peer) = tokio::io::duplex(512);
    let (_outbound, queued) = mpsc::channel(1);
    let (events, mut happened) = mpsc::channel(1);
    let (stop, stopped) = oneshot::channel();
    let driving = drive(
        stream,
        queued,
        events,
        128,
        async {
            stopped.await.unwrap();
        },
        || true,
    );
    tokio::pin!(driving);
    for room in ["first", "pending", "not admitted"] {
        peer.write_all(&command(room)).await.unwrap();
    }
    assert_pending(driving.as_mut());
    stop.send(()).unwrap();
    assert_pending(driving.as_mut());
    let receiving = async {
        for expected in ["first", "pending"] {
            assert!(
                matches!(happened.recv().await, Some(ClientMessage::JoinRoom { room }) if room == expected)
            );
        }
        assert!(happened.recv().await.is_none());
    };
    tokio::join!(driving, receiving);
}

#[tokio::test]
async fn writer_failure_releases_blocked_senders_and_input_owner() {
    let (stream, peer) = tokio::io::duplex(16);
    let (outbound, queued) = mpsc::channel(1);
    let (events, mut happened) = mpsc::channel(1);
    outbound.send(ServerMessage::error("x".repeat(64))).await.unwrap();
    let driving = drive(stream, queued, events, 128, pending(), || true);
    tokio::pin!(driving);
    assert_pending(driving.as_mut());
    outbound.send(ServerMessage::error("queued")).await.unwrap();
    let blocked = outbound.send(ServerMessage::error("blocked"));
    tokio::pin!(blocked);
    assert_pending(blocked.as_mut());
    drop(peer);
    driving.await;
    assert!(outbound.is_closed());
    assert!(blocked.await.is_err());
    assert!(happened.recv().await.is_none());
}

#[tokio::test]
async fn handler_exit_releases_the_stream_even_without_peer_input() {
    let (stream, _peer) = tokio::io::duplex(16);
    let (outbound, queued) = mpsc::channel(1);
    let (events, happened) = mpsc::channel(1);
    drop(happened);
    drive(stream, queued, events, 128, pending(), || true).await;
    assert!(outbound.is_closed());
}

#[tokio::test]
async fn revoked_authorization_does_not_write_queued_output() {
    let (stream, mut peer) = tokio::io::duplex(64);
    let (outbound, queued) = mpsc::channel(1);
    let (events, mut happened) = mpsc::channel(1);
    outbound.send(ServerMessage::error("secret")).await.unwrap();
    drive(stream, queued, events, 128, pending(), || false).await;
    let mut bytes = Vec::new();
    peer.read_to_end(&mut bytes).await.unwrap();
    assert!(bytes.is_empty());
    assert!(outbound.is_closed());
    assert!(happened.recv().await.is_none());
}

#[tokio::test]
async fn shutdown_retains_accepted_handler_work_until_it_finishes() {
    let (stream, mut peer) = tokio::io::duplex(128);
    let (_outbound, queued) = mpsc::channel(1);
    let (events, mut happened) = mpsc::channel(1);
    let (stop, stopped) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let (started, mut admitted) = oneshot::channel();
    peer.write_all(&command("room")).await.unwrap();
    let handling = async {
        assert!(happened.recv().await.is_some());
        started.send(()).unwrap();
        released.await.unwrap(); // Models an accepted durable operation.
        assert!(happened.recv().await.is_none());
    };
    let owner = async {
        tokio::join!(
            handling,
            drive(
                stream,
                queued,
                events,
                128,
                async {
                    stopped.await.unwrap();
                },
                || true
            )
        );
    };
    tokio::pin!(owner);
    assert_pending(owner.as_mut());
    assert_pending(owner.as_mut());
    assert!(admitted.try_recv().is_ok());
    stop.send(()).unwrap();
    assert_pending(owner.as_mut());
    release.send(()).unwrap();
    owner.await;
}
