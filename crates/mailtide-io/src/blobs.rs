//! Raw message content, written once per message at a deterministic key.

use std::future::Future;
use std::sync::Arc;

use mailtide::ids::{AccountId, BlobRef, MessageId};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

/// A blob store failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobError {
    /// The store couldn't be reached or failed.
    Unavailable(String),
    /// The configuration (URL, credentials) is invalid.
    Misconfigured(String),
}

/// Where raw content lives.
pub trait BlobStore: Send + Sync {
    /// Write `bytes` at `key` durably; overwriting identical content is fine.
    fn put(
        &self,
        key: &BlobRef,
        bytes: Vec<u8>,
    ) -> impl Future<Output = Result<(), BlobError>> + Send;
    /// Read the content at `key`.
    fn get(&self, key: &BlobRef) -> impl Future<Output = Result<Vec<u8>, BlobError>> + Send;
    /// Delete the content at `key`; deleting a missing key is fine.
    fn delete(&self, key: &BlobRef) -> impl Future<Output = Result<(), BlobError>> + Send;
    /// Delete everything stored for `account`; returns how many objects.
    fn delete_account(
        &self,
        account: &AccountId,
    ) -> impl Future<Output = Result<u64, BlobError>> + Send;
}

/// The deterministic key for a message's raw content, so a retried import
/// writes the same object and cleanup can find it.
#[must_use]
pub fn blob_key(account: &AccountId, message: &MessageId) -> BlobRef {
    BlobRef(format!("{}/{}.eml", account.as_str(), message.0))
}

/// Blobs in any `object_store` backend: GCS, S3, local disk or memory.
#[derive(Debug, Clone)]
pub struct ObjectBlobs {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl ObjectBlobs {
    /// Blobs at a URL: `gs://bucket/prefix`, `s3://bucket/prefix`,
    /// `file:///dir`, or `memory:///`.
    pub fn from_url(url: &str) -> Result<Self, BlobError> {
        let parsed = url::Url::parse(url).map_err(|e| BlobError::Misconfigured(e.to_string()))?;
        let (store, prefix) = object_store::parse_url(&parsed)
            .map_err(|e| BlobError::Misconfigured(e.to_string()))?;
        Ok(Self {
            store: Arc::from(store),
            prefix,
        })
    }

    /// Blobs held in memory.
    #[must_use]
    pub fn memory() -> Self {
        Self {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: Path::default(),
        }
    }

    fn path(&self, key: &BlobRef) -> Path {
        key.0
            .split('/')
            .fold(self.prefix.clone(), |path, part| path.clone().join(part))
    }
}

fn unavailable(error: &object_store::Error) -> BlobError {
    BlobError::Unavailable(error.to_string())
}

impl BlobStore for ObjectBlobs {
    async fn put(&self, key: &BlobRef, bytes: Vec<u8>) -> Result<(), BlobError> {
        self.store
            .put(&self.path(key), PutPayload::from(bytes))
            .await
            .map(|_| ())
            .map_err(|e| unavailable(&e))
    }

    async fn get(&self, key: &BlobRef) -> Result<Vec<u8>, BlobError> {
        let result = self
            .store
            .get(&self.path(key))
            .await
            .map_err(|e| unavailable(&e))?;
        result
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| unavailable(&e))
    }

    async fn delete_account(&self, account: &AccountId) -> Result<u64, BlobError> {
        use futures_util::StreamExt as _;
        let prefix = self.prefix.clone().join(account.as_str());
        let mut listing = self.store.list(Some(&prefix));
        let mut deleted = 0;
        while let Some(meta) = listing.next().await {
            let meta = meta.map_err(|e| unavailable(&e))?;
            match self.store.delete(&meta.location).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => deleted += 1,
                Err(e) => return Err(unavailable(&e)),
            }
        }
        Ok(deleted)
    }

    async fn delete(&self, key: &BlobRef) -> Result<(), BlobError> {
        match self.store.delete(&self.path(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(unavailable(&e)),
        }
    }
}
