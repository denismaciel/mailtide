//! Reading one part of a stored message by its part reference (SPEC §11):
//! a ranged read of the blob and a transfer decode, whole or streamed. No
//! parse, no store read, no Gmail call.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::stream::{BoxStream, Stream, StreamExt as _};
use mailtide::gmail::PartRef;
use mailtide::ids::{AccountId, MessageId};
use mailtide_mime::{PartDecoder, PartInfo};
use mailtide_store::Store;

use crate::auth::TokenSource;
use crate::blobs::{BlobError, BlobStore, blob_key};
use crate::quota_gate::QuotaGate;
use crate::runner::Runner;
use crate::transport::GmailTransport;

/// Why a part could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartError {
    /// The message has no stored content (never imported, or deleted).
    BlobNotFound,
    /// The blob store failed; retry.
    BlobUnavailable(String),
    /// The reference names no part of this message: unreadable, outside
    /// the blob, made for a blob of another length, or the bytes there do
    /// not decode to the size it names.
    InvalidPart(String),
    /// The part is larger than allowed; refused before any read. Stream it
    /// instead.
    TooLarge {
        /// Its decoded size.
        size: u64,
        /// The limit it exceeds.
        limit: u64,
    },
}

impl PartError {
    /// A stable name for logs.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::BlobNotFound => "blob_not_found",
            Self::BlobUnavailable(_) => "blob_unavailable",
            Self::InvalidPart(_) => "invalid_part",
            Self::TooLarge { .. } => "too_large",
        }
    }
}

impl From<BlobError> for PartError {
    fn from(error: BlobError) -> Self {
        match error {
            BlobError::NotFound => Self::BlobNotFound,
            BlobError::Unavailable(detail) | BlobError::Misconfigured(detail) => {
                Self::BlobUnavailable(detail)
            }
            BlobError::OutOfRange { total } => Self::InvalidPart(format!(
                "the range is outside the stored message ({total} bytes)"
            )),
        }
    }
}

/// The blob holds the bytes `part` was computed against, as far as its
/// length tells.
pub(crate) fn check_length(part: &PartRef, total: u64) -> Result<(), PartError> {
    if total == part.total {
        Ok(())
    } else {
        Err(PartError::InvalidPart(format!(
            "the reference is for a message of {} bytes; the stored one has {total}",
            part.total
        )))
    }
}

/// The decoded part has the size its reference names.
pub(crate) fn check_size(part: &PartRef, decoded: u64) -> Result<(), PartError> {
    if decoded == part.size {
        Ok(())
    } else {
        Err(PartError::InvalidPart(format!(
            "the bytes decode to {decoded} bytes, not the {} the reference names",
            part.size
        )))
    }
}

/// The decoded bytes of one part, in chunks of at most a chosen size, as the
/// blob store delivers the encoded range. Memory stays near one chunk (a
/// part malformed for its encoding is decoded whole, at the end). An error
/// item ends the stream: a failing store or a part whose bytes do not
/// decode to its size never yields a short part silently. Dropping it stops
/// the read.
pub struct PartStream {
    part: PartRef,
    chunk_size: usize,
    source: BoxStream<'static, Result<Bytes, BlobError>>,
    /// The store chunk being decoded, and how far.
    current: Bytes,
    at: usize,
    decoder: Option<PartDecoder>,
    /// Decoded bytes not yet yielded.
    out: Vec<u8>,
    yielded: u64,
    done: bool,
}

impl std::fmt::Debug for PartStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartStream")
            .field("part", &self.part)
            .field("chunk_size", &self.chunk_size)
            .field("yielded", &self.yielded)
            .finish_non_exhaustive()
    }
}

impl PartStream {
    /// The part's decoded size: what the chunks add up to.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.part.size
    }

    /// The next chunk, `None` at the end.
    pub async fn next_chunk(&mut self) -> Option<Result<Vec<u8>, PartError>> {
        self.next().await
    }

    /// End the stream with `error`.
    fn fail(&mut self, error: PartError) -> Poll<Option<Result<Vec<u8>, PartError>>> {
        self.done = true;
        self.out = Vec::new();
        self.decoder = None;
        Poll::Ready(Some(Err(error)))
    }

    /// Yield up to `chunk_size` decoded bytes.
    fn emit(&mut self) -> Poll<Option<Result<Vec<u8>, PartError>>> {
        let rest = self.out.split_off(self.out.len().min(self.chunk_size));
        let chunk = std::mem::replace(&mut self.out, rest);
        self.yielded += u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        Poll::Ready(Some(Ok(chunk)))
    }
}

impl Stream for PartStream {
    type Item = Result<Vec<u8>, PartError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            let decoded = this.yielded + u64::try_from(this.out.len()).unwrap_or(u64::MAX);
            if decoded > this.part.size {
                return this.fail(PartError::InvalidPart(
                    "the bytes decode to more than the size the reference names".into(),
                ));
            }
            if this.out.len() >= this.chunk_size || (this.decoder.is_none() && !this.out.is_empty())
            {
                return this.emit();
            }
            if this.done || this.decoder.is_none() {
                this.done = true;
                return Poll::Ready(None);
            }
            // Decode the store's chunk a slice at a time, so the decoded
            // bytes waiting never exceed about one chunk.
            if this.at < this.current.len() {
                let end = this.current.len().min(this.at + this.chunk_size);
                if let (Some(decoder), Some(slice)) =
                    (this.decoder.as_mut(), this.current.get(this.at..end))
                {
                    decoder.feed(slice, &mut this.out);
                }
                this.at = end;
                continue;
            }
            match this.source.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(chunk))) => {
                    this.current = chunk;
                    this.at = 0;
                }
                Poll::Ready(Some(Err(error))) => return this.fail(error.into()),
                Poll::Ready(None) => {
                    if let Some(decoder) = this.decoder.take() {
                        decoder.finish(&mut this.out);
                    }
                    let decoded = this.yielded + u64::try_from(this.out.len()).unwrap_or(u64::MAX);
                    if let Err(error) = check_size(&this.part, decoded) {
                        return this.fail(error);
                    }
                }
            }
        }
    }
}

impl<S, B, T, Q, A> Runner<S, B, T, Q, A>
where
    S: Store,
    B: BlobStore,
    T: GmailTransport,
    Q: QuotaGate,
    A: TokenSource,
{
    /// Every leaf part of a stored message, nested messages included, with
    /// its reference and role: the blob is read whole and parsed here, off
    /// the async workers; only the metadata is returned.
    pub async fn parts(
        &self,
        account: &AccountId,
        message: &MessageId,
    ) -> Result<Vec<PartInfo>, PartError> {
        let raw = self.blobs().get(&blob_key(account, message)).await?;
        tokio::task::spawn_blocking(move || mailtide_mime::parts(&raw))
            .await
            .map_err(|_| PartError::InvalidPart("the stored message could not be parsed".into()))
    }

    /// The transfer-decoded bytes of one part, with one ranged read of its
    /// encoded body. A part larger than `max_bytes` is refused from its
    /// reference, before any read; read it with `open_part` instead.
    pub async fn read_part(
        &self,
        account: &AccountId,
        message: &MessageId,
        part: &PartRef,
        max_bytes: u64,
    ) -> Result<Vec<u8>, PartError> {
        if part.size > max_bytes {
            return Err(PartError::TooLarge {
                size: part.size,
                limit: max_bytes,
            });
        }
        let read = self
            .blobs()
            .get_range(&blob_key(account, message), part.range())
            .await?;
        check_length(part, read.total)?;
        let mut decoder = PartDecoder::new(part);
        let mut out = Vec::new();
        decoder.feed(&read.bytes, &mut out);
        decoder.finish(&mut out);
        check_size(part, u64::try_from(out.len()).unwrap_or(u64::MAX))?;
        Ok(out)
    }

    /// Stream one part's decoded bytes in chunks of at most `chunk_size`
    /// (at least 1), from a streamed ranged read of its encoded body. The
    /// blob's length is checked against the reference before the first
    /// chunk.
    pub async fn open_part(
        &self,
        account: &AccountId,
        message: &MessageId,
        part: &PartRef,
        chunk_size: usize,
    ) -> Result<PartStream, PartError> {
        let read = self
            .blobs()
            .get_chunks(&blob_key(account, message), part.range())
            .await?;
        check_length(part, read.total)?;
        Ok(PartStream {
            part: *part,
            chunk_size: chunk_size.max(1),
            source: read.chunks,
            current: Bytes::new(),
            at: 0,
            decoder: Some(PartDecoder::new(part)),
            out: Vec::new(),
            yielded: 0,
            done: false,
        })
    }
}
