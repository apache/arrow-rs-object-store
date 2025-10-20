use std::{future::Future, time::Duration};

use futures::FutureExt;
use futures_timer::Delay;

/// A future that resolves after `duration`.
///
/// Not intended for use when high resolution is required.
pub(crate) struct Sleep {
    delay: Delay,
}

/// Create a new [`Sleep`] instance that will resolve once `duration` has passed since its first awaited.
pub(crate) fn sleep(duration: Duration) -> Sleep {
    Sleep {
        delay: Delay::new(duration),
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.as_mut().delay.poll_unpin(cx)
    }
}
