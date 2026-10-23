//! Bounded session ownership; stream completion never closes another stream.
use std::future::Future;
use std::num::NonZeroUsize;

use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::sync::watch;

pub(super) async fn run<S, A, N, R, F>(
    mut incoming: A,
    capacity: NonZeroUsize,
    shutdown: impl Future<Output = ()>,
    mut serve: R,
) where
    A: FnMut() -> N,
    N: Future<Output = Option<S>>,
    R: FnMut(S, watch::Receiver<bool>) -> F,
    F: Future<Output = ()>,
{
    let (end, ended) = watch::channel(false);
    let mut streams = FuturesUnordered::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => break,
            Some(()) = streams.next(), if !streams.is_empty() => {},
            next = async { incoming().await }, if streams.len() < capacity.get() => {
                match next {
                    Some(stream) => streams.push(serve(stream, ended.clone())),
                    None => break,
                }
            }
        }
    }
    end.send_replace(true);
    // Native grace policy remains with the listener. Retain accepted work.
    while streams.next().await.is_some() {}
}

pub(super) async fn wait_end(mut ended: watch::Receiver<bool>) {
    while !*ended.borrow_and_update() {
        ended.changed().await.expect("session owns shutdown through drain");
    }
}

#[cfg(test)]
#[path = "../../../../tests/svc/web/web_transport/streams.rs"]
mod tests;
