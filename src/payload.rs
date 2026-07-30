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

use bytes::Bytes;
use futures_util::Stream;
use futures_util::stream::{BoxStream, StreamExt};
use std::error::Error;
use std::fmt::{Debug, Formatter};
#[cfg(feature = "fs")]
use std::path::Path;
use std::sync::Arc;

/// The error type yielded by a streaming [`PutPayload`].
pub type PutPayloadError = Box<dyn Error + Send + Sync + 'static>;

/// A stream of bytes used by a streaming [`PutPayload`].
pub type PutPayloadStream = BoxStream<'static, Result<Bytes, PutPayloadError>>;

type StreamFactory = Arc<dyn Fn() -> PutPayloadStream + Send + Sync>;

/// The default chunk size used by [`PutPayload::from_file`].
#[cfg(feature = "fs")]
pub const DEFAULT_FILE_CHUNK_SIZE: usize = 16 * 1024;

/// A cheaply cloneable payload for a put request.
///
/// A payload can either contain an ordered collection of [`Bytes`] or a
/// replayable stream with a known content length.
///
/// Streaming put support is tracked in
/// [apache/arrow-rs-object-store#281](https://github.com/apache/arrow-rs-object-store/issues/281).
#[derive(Clone)]
pub struct PutPayload(PutPayloadInner);

#[derive(Clone)]
enum PutPayloadInner {
    Bytes(Arc<[Bytes]>),
    Streaming {
        factory: StreamFactory,
        content_length: usize,
    },
}

impl Debug for PutPayload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            PutPayloadInner::Bytes(bytes) => f.debug_tuple("PutPayload").field(bytes).finish(),
            PutPayloadInner::Streaming { content_length, .. } => f
                .debug_struct("PutPayload")
                .field("streaming", &true)
                .field("content_length", content_length)
                .finish(),
        }
    }
}

impl Default for PutPayload {
    fn default() -> Self {
        Self(PutPayloadInner::Bytes(Arc::new([])))
    }
}

impl PutPayload {
    /// Create a new empty [`PutPayload`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a [`PutPayload`] from a static slice
    pub fn from_static(s: &'static [u8]) -> Self {
        s.into()
    }

    /// Creates a [`PutPayload`] from a [`Bytes`]
    pub fn from_bytes(s: Bytes) -> Self {
        s.into()
    }

    /// Creates a replayable streaming [`PutPayload`].
    ///
    /// `stream_factory` is invoked for every upload attempt and must return a
    /// new stream starting at the beginning of the payload. The
    /// `content_length` must exactly match the number of bytes yielded.
    pub fn from_stream<F, S, E>(stream_factory: F, content_length: usize) -> Self
    where
        F: Fn() -> S + Send + Sync + 'static,
        S: Stream<Item = Result<Bytes, E>> + Send + 'static,
        E: Error + Send + Sync + 'static,
    {
        let factory = Arc::new(move || {
            stream_factory()
                .map(|result| result.map_err(|e| Box::new(e) as PutPayloadError))
                .boxed()
        });
        Self(PutPayloadInner::Streaming {
            factory,
            content_length,
        })
    }

    /// Creates a replayable streaming payload from a file.
    ///
    /// The file is read in 16 KiB chunks and reopened for every upload attempt.
    /// It must remain available and unchanged until the put request completes.
    #[cfg(feature = "fs")]
    pub async fn from_file(path: impl AsRef<Path>) -> std::io::Result<Self> {
        Self::from_file_with_chunk_size(path, DEFAULT_FILE_CHUNK_SIZE).await
    }

    /// Creates a replayable streaming payload from a file using `chunk_size`.
    ///
    /// The file is reopened for every upload attempt. It must remain available
    /// and unchanged until the put request completes.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_size` is zero.
    #[cfg(feature = "fs")]
    pub async fn from_file_with_chunk_size(
        path: impl AsRef<Path>,
        chunk_size: usize,
    ) -> std::io::Result<Self> {
        assert!(chunk_size > 0, "chunk size must be greater than zero");

        let path = Arc::new(path.as_ref().to_owned());
        let content_length = tokio::fs::metadata(path.as_ref()).await?.len();
        let content_length = usize::try_from(content_length).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "file length does not fit in usize",
            )
        })?;

        Ok(Self::from_stream(
            move || {
                let path = Arc::clone(&path);
                futures_util::stream::try_unfold(
                    FileStreamState::Open(path),
                    move |state| async move {
                        use tokio::io::AsyncReadExt;

                        let mut file = match state {
                            FileStreamState::Open(path) => {
                                tokio::fs::File::open(path.as_ref()).await?
                            }
                            FileStreamState::Reading(file) => file,
                        };

                        let mut buffer = vec![0; chunk_size];
                        let read = file.read(&mut buffer).await?;
                        if read == 0 {
                            return Ok::<_, std::io::Error>(None);
                        }
                        buffer.truncate(read);
                        Ok(Some((Bytes::from(buffer), FileStreamState::Reading(file))))
                    },
                )
            },
            content_length,
        ))
    }

    /// Returns the total length of this payload
    pub fn content_length(&self) -> usize {
        match &self.0 {
            PutPayloadInner::Bytes(bytes) => bytes.iter().map(|b| b.len()).sum(),
            PutPayloadInner::Streaming { content_length, .. } => *content_length,
        }
    }

    /// Returns `true` if this is a streaming payload.
    pub fn is_streaming(&self) -> bool {
        matches!(self.0, PutPayloadInner::Streaming { .. })
    }

    /// Returns a new stream over this payload.
    pub fn stream(&self) -> PutPayloadStream {
        match &self.0 {
            PutPayloadInner::Bytes(bytes) => {
                futures_util::stream::iter(bytes.as_ref().to_vec().into_iter().map(Ok)).boxed()
            }
            PutPayloadInner::Streaming { factory, .. } => factory(),
        }
    }

    /// Collects this payload into a contiguous [`Bytes`].
    pub async fn bytes(&self) -> Result<Bytes, PutPayloadError> {
        match &self.0 {
            PutPayloadInner::Bytes(bytes) => Ok(match bytes.len() {
                0 => Bytes::new(),
                1 => bytes[0].clone(),
                _ => {
                    let mut buffer = Vec::with_capacity(self.content_length());
                    for chunk in bytes.iter() {
                        buffer.extend_from_slice(chunk);
                    }
                    buffer.into()
                }
            }),
            PutPayloadInner::Streaming { .. } => {
                let mut stream = self.stream();
                let mut buffer = Vec::with_capacity(self.content_length());
                while let Some(chunk) = futures_util::TryStreamExt::try_next(&mut stream).await? {
                    buffer.extend_from_slice(&chunk);
                }
                Ok(buffer.into())
            }
        }
    }

    /// Returns an iterator over the [`Bytes`] in this payload
    ///
    /// # Panics
    ///
    /// Panics if this is a streaming payload. Use [`Self::stream`] instead.
    pub fn iter(&self) -> PutPayloadIter<'_> {
        match &self.0 {
            PutPayloadInner::Bytes(bytes) => PutPayloadIter(bytes.iter()),
            PutPayloadInner::Streaming { .. } => {
                panic!("cannot synchronously iterate over a streaming PutPayload")
            }
        }
    }
}

#[cfg(feature = "fs")]
enum FileStreamState {
    Open(Arc<std::path::PathBuf>),
    Reading(tokio::fs::File),
}

impl AsRef<[Bytes]> for PutPayload {
    fn as_ref(&self) -> &[Bytes] {
        match &self.0 {
            PutPayloadInner::Bytes(bytes) => bytes.as_ref(),
            PutPayloadInner::Streaming { .. } => {
                panic!("cannot borrow bytes from a streaming PutPayload")
            }
        }
    }
}

impl<'a> IntoIterator for &'a PutPayload {
    type Item = &'a Bytes;
    type IntoIter = PutPayloadIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for PutPayload {
    type Item = Bytes;
    type IntoIter = PutPayloadIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        PutPayloadIntoIter {
            payload: self,
            idx: 0,
        }
    }
}

/// An iterator over [`PutPayload`]
#[derive(Debug)]
pub struct PutPayloadIter<'a>(std::slice::Iter<'a, Bytes>);

impl<'a> Iterator for PutPayloadIter<'a> {
    type Item = &'a Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

/// An owning iterator of [`PutPayload`]
#[derive(Debug)]
pub struct PutPayloadIntoIter {
    payload: PutPayload,
    idx: usize,
}

impl Iterator for PutPayloadIntoIter {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        let p = match &self.payload.0 {
            PutPayloadInner::Bytes(bytes) => bytes.get(self.idx)?.clone(),
            PutPayloadInner::Streaming { .. } => {
                panic!("cannot synchronously iterate over a streaming PutPayload")
            }
        };
        self.idx += 1;
        Some(p)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let l = match &self.payload.0 {
            PutPayloadInner::Bytes(bytes) => bytes.len() - self.idx,
            PutPayloadInner::Streaming { .. } => 0,
        };
        (l, Some(l))
    }
}

impl From<Bytes> for PutPayload {
    fn from(value: Bytes) -> Self {
        Self(PutPayloadInner::Bytes(Arc::new([value])))
    }
}

impl From<Vec<u8>> for PutPayload {
    fn from(value: Vec<u8>) -> Self {
        Self(PutPayloadInner::Bytes(Arc::new([value.into()])))
    }
}

impl From<&'static str> for PutPayload {
    fn from(value: &'static str) -> Self {
        Bytes::from(value).into()
    }
}

impl From<&'static [u8]> for PutPayload {
    fn from(value: &'static [u8]) -> Self {
        Bytes::from(value).into()
    }
}

impl From<String> for PutPayload {
    fn from(value: String) -> Self {
        Bytes::from(value).into()
    }
}

impl FromIterator<u8> for PutPayload {
    fn from_iter<T: IntoIterator<Item = u8>>(iter: T) -> Self {
        Bytes::from_iter(iter).into()
    }
}

impl FromIterator<Bytes> for PutPayload {
    fn from_iter<T: IntoIterator<Item = Bytes>>(iter: T) -> Self {
        Self(PutPayloadInner::Bytes(iter.into_iter().collect()))
    }
}

impl From<PutPayload> for Bytes {
    fn from(value: PutPayload) -> Self {
        let bytes = match value.0 {
            PutPayloadInner::Bytes(bytes) => bytes,
            PutPayloadInner::Streaming { .. } => {
                panic!("cannot synchronously collect a streaming PutPayload")
            }
        };
        match bytes.len() {
            0 => Self::new(),
            1 => bytes[0].clone(),
            _ => {
                let mut buf = Vec::with_capacity(bytes.iter().map(|x| x.len()).sum());
                bytes.iter().for_each(|x| buf.extend_from_slice(x));
                buf.into()
            }
        }
    }
}

/// A builder for [`PutPayload`] that avoids reallocating memory
///
/// Data is allocated in fixed blocks, which are flushed to [`Bytes`] once full.
/// Unlike [`Vec`] this avoids needing to repeatedly reallocate blocks of memory,
/// which typically involves copying all the previously written data to a new
/// contiguous memory region.
#[derive(Debug)]
pub struct PutPayloadMut {
    len: usize,
    completed: Vec<Bytes>,
    in_progress: Vec<u8>,
    block_size: usize,
}

impl Default for PutPayloadMut {
    fn default() -> Self {
        Self {
            len: 0,
            completed: vec![],
            in_progress: vec![],

            block_size: 8 * 1024,
        }
    }
}

impl PutPayloadMut {
    /// Create a new [`PutPayloadMut`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Configures the minimum allocation size
    ///
    /// Defaults to 8KB
    pub fn with_block_size(self, block_size: usize) -> Self {
        Self { block_size, ..self }
    }

    /// Write bytes into this [`PutPayloadMut`]
    ///
    /// If there is an in-progress block, data will be first written to it, flushing
    /// it to [`Bytes`] once full. If data remains to be written, a new block of memory
    /// of at least the configured block size will be allocated, to hold the remaining data.
    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        let remaining = self.in_progress.capacity() - self.in_progress.len();
        let to_copy = remaining.min(slice.len());

        self.in_progress.extend_from_slice(&slice[..to_copy]);
        if self.in_progress.capacity() == self.in_progress.len() {
            let new_cap = self.block_size.max(slice.len() - to_copy);
            let completed = std::mem::replace(&mut self.in_progress, Vec::with_capacity(new_cap));
            if !completed.is_empty() {
                self.completed.push(completed.into())
            }
            self.in_progress.extend_from_slice(&slice[to_copy..])
        }
        self.len += slice.len();
    }

    /// Append a [`Bytes`] to this [`PutPayloadMut`] without copying
    ///
    /// This will close any currently buffered block populated by [`Self::extend_from_slice`],
    /// and append `bytes` to this payload without copying.
    pub fn push(&mut self, bytes: Bytes) {
        if !self.in_progress.is_empty() {
            let completed = std::mem::take(&mut self.in_progress);
            self.completed.push(completed.into())
        }
        self.len += bytes.len();
        self.completed.push(bytes);
    }

    /// Returns `true` if this [`PutPayloadMut`] contains no bytes
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the total length of the [`Bytes`] in this payload
    #[inline]
    pub fn content_length(&self) -> usize {
        self.len
    }

    /// Convert into [`PutPayload`]
    pub fn freeze(mut self) -> PutPayload {
        if !self.in_progress.is_empty() {
            let completed = std::mem::take(&mut self.in_progress).into();
            self.completed.push(completed);
        }
        PutPayload(PutPayloadInner::Bytes(self.completed.into()))
    }
}

impl From<PutPayloadMut> for PutPayload {
    fn from(value: PutPayloadMut) -> Self {
        value.freeze()
    }
}

#[cfg(test)]
mod test {
    use crate::PutPayloadMut;
    #[cfg(feature = "fs")]
    use crate::{DEFAULT_FILE_CHUNK_SIZE, PutPayload};
    #[cfg(feature = "fs")]
    use futures_util::TryStreamExt;

    #[test]
    fn test_put_payload() {
        let mut chunk = PutPayloadMut::new().with_block_size(23);
        chunk.extend_from_slice(&[1; 16]);
        chunk.extend_from_slice(&[2; 32]);
        chunk.extend_from_slice(&[2; 5]);
        chunk.extend_from_slice(&[2; 21]);
        chunk.extend_from_slice(&[2; 40]);
        chunk.extend_from_slice(&[0; 0]);
        chunk.push("foobar".into());

        let payload = chunk.freeze();
        assert_eq!(payload.content_length(), 120);

        let chunks = payload.as_ref();
        assert_eq!(chunks.len(), 6);

        assert_eq!(chunks[0].len(), 23);
        assert_eq!(chunks[1].len(), 25); // 32 - (23 - 16)
        assert_eq!(chunks[2].len(), 23);
        assert_eq!(chunks[3].len(), 23);
        assert_eq!(chunks[4].len(), 20);
        assert_eq!(chunks[5].len(), 6);
    }

    #[test]
    fn test_content_length() {
        let mut chunk = PutPayloadMut::new();
        chunk.push(vec![0; 23].into());
        assert_eq!(chunk.content_length(), 23);
        chunk.extend_from_slice(&[0; 4]);
        assert_eq!(chunk.content_length(), 27);
        chunk.push(vec![0; 121].into());
        assert_eq!(chunk.content_length(), 148);
        let payload = chunk.freeze();
        assert_eq!(payload.content_length(), 148);
    }

    #[cfg(feature = "fs")]
    #[tokio::test]
    async fn test_put_payload_from_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("payload");
        let data = vec![42; DEFAULT_FILE_CHUNK_SIZE * 2 + 7];
        tokio::fs::write(&path, &data).await.unwrap();

        let payload = PutPayload::from_file(&path).await.unwrap();
        assert_eq!(payload.content_length(), data.len());

        for _ in 0..2 {
            let chunks: Vec<_> = payload.stream().try_collect().await.unwrap();
            assert_eq!(
                chunks.iter().map(|chunk| chunk.len()).collect::<Vec<_>>(),
                vec![DEFAULT_FILE_CHUNK_SIZE, DEFAULT_FILE_CHUNK_SIZE, 7]
            );
            assert_eq!(chunks.concat(), data);
        }
    }
}
