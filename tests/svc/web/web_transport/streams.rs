use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::pending;
use std::pin::Pin;
use std::task::{Context, Waker};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

use super::super::io::drive;
use super::*;
use crate::svc::web::framing::encode_server_message;
use crate::types::messages::{ClientMessage, ServerMessage};

fn assert_pending(future: Pin<&mut impl Future>) {
    assert!(future.poll(&mut Context::from_waker(Waker::noop())).is_pending());
}

fn incoming<T>(
    queue: &RefCell<VecDeque<T>>
) -> impl Future<Output = Option<T>> + use<T> {
    let next = queue.borrow_mut().pop_front();
    async move {
        match next {
            Some(next) => Some(next),
            None => pending().await,
        }
    }
}

#[tokio::test]
async fn admission_is_bounded_and_shutdown_joins_each_stream() {
    let (release1, done1) = oneshot::channel();
    let (release2, done2) = oneshot::channel();
    let (release3, done3) = oneshot::channel();
    let (_release4, done4) = oneshot::channel();
    let queue = RefCell::new(VecDeque::from([
        (1, done1),
        (2, done2),
        (3, done3),
        (4, done4),
    ]));
    let started = RefCell::new(Vec::new());
    let stopped = RefCell::new(Vec::new());
    let completed = RefCell::new(Vec::new());
    let (stop, stopping) = oneshot::channel();
    let running = run(
        || incoming(&queue),
        NonZeroUsize::new(2).unwrap(),
        async {
            stopping.await.unwrap();
        },
        |(id, done), ended| {
            started.borrow_mut().push(id);
            let stopped = &stopped;
            let completed = &completed;
            async move {
                tokio::pin!(done);
                tokio::select! {
                    _ = &mut done => {},
                    () = wait_end(ended) => {
                        stopped.borrow_mut().push(id);
                        let _ = done.await;
                    }
                }
                completed.borrow_mut().push(id);
            }
        },
    );
    tokio::pin!(running);
    assert_pending(running.as_mut());
    assert_eq!(*started.borrow(), [1, 2]);
    assert_eq!(queue.borrow().len(), 2);
    release1.send(()).unwrap();
    assert_pending(running.as_mut());
    assert_eq!(*started.borrow(), [1, 2, 3]);
    stop.send(()).unwrap();
    assert_pending(running.as_mut());
    assert_eq!(*started.borrow(), [1, 2, 3]);
    assert_eq!(queue.borrow().len(), 1);
    assert_eq!(*completed.borrow(), [1]);
    assert_eq!(stopped.borrow().len(), 2);
    release2.send(()).unwrap();
    release3.send(()).unwrap();
    running.await;
    assert_eq!(completed.borrow().len(), 3);
}

#[tokio::test]
async fn second_stream_replies_while_first_is_incomplete_and_survives_its_eof()
{
    let (first, mut peer1) = tokio::io::duplex(128);
    let (second, mut peer2) = tokio::io::duplex(128);
    let queue = RefCell::new(VecDeque::from([first, second]));
    let (stop, stopped) = oneshot::channel();
    let running = run(
        || incoming(&queue),
        NonZeroUsize::new(2).unwrap(),
        async {
            stopped.await.unwrap();
        },
        |stream, ended| async move {
            let (outbound, queued) = tokio::sync::mpsc::channel(1);
            let (events, mut happened) = tokio::sync::mpsc::channel(1);
            let handling = async {
                while let Some(ClientMessage::JoinRoom { room }) =
                    happened.recv().await
                {
                    let _ = outbound.send(ServerMessage::error(room)).await;
                }
            };
            tokio::join!(
                handling,
                drive(stream, queued, events, 128, wait_end(ended), || true)
            );
        },
    );
    let clients = async {
        // A has only part of its frame prefix; B uses the real Chat codec.
        peer1.write_all(&[0, 0]).await.unwrap();
        async fn exchange(
            peer: &mut tokio::io::DuplexStream,
            room: &str,
        ) {
            let payload = rmp_serde::to_vec_named(&serde_json::json!({
                "type": "join_room", "room": room,
            }))
            .unwrap();
            peer.write_all(&(payload.len() as u32).to_be_bytes())
                .await
                .unwrap();
            peer.write_all(&payload).await.unwrap();
            let expected =
                encode_server_message(&ServerMessage::error(room), 128)
                    .unwrap();
            let mut actual = vec![0; expected.len()];
            peer.read_exact(&mut actual).await.unwrap();
            assert_eq!(actual, expected);
        }
        exchange(&mut peer2, "first").await;
        drop(peer1);
        exchange(&mut peer2, "second").await;
        stop.send(()).unwrap();
    };
    tokio::join!(running, clients);
}

#[tokio::test]
async fn driver_end_signals_and_joins_existing_streams() {
    let (release, released) = oneshot::channel();
    let queue = RefCell::new(VecDeque::from([released]));
    let stopped = RefCell::new(false);
    let running = run(
        || async { queue.borrow_mut().pop_front() },
        NonZeroUsize::new(2).unwrap(),
        pending(),
        |released, ended| {
            let stopped = &stopped;
            async move {
                wait_end(ended).await;
                *stopped.borrow_mut() = true;
                released.await.unwrap();
            }
        },
    );
    tokio::pin!(running);
    assert_pending(running.as_mut());
    assert!(*stopped.borrow());
    release.send(()).unwrap();
    running.await;
}
