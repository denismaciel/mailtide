//! Reading one part of a stored message by its part reference (SPEC §11):
//! a ranged read of the blob and a transfer decode. No parse, no store
//! read, no Gmail call.

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
}
