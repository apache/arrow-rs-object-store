// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
