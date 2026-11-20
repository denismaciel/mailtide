//! Raw message content, written once per message at a deterministic key.

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;

use mailtide::ids::{AccountId, BlobRef, MessageId};
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutPayload};

/// A blob store failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobError {
    /// The store couldn't be reached or failed.
    Unavailable(String),
    /// The configuration (URL, credentials) is invalid.
    Misconfigured(String),
    /// No content at that key (never written, or cleaned up after the
    /// message was deleted).
    NotFound,
    /// The range asked for is not inside the content, which is `total`
    /// bytes long.
    OutOfRange {
        /// The content's length.
        total: u64,
    },
}

/// One range of a blob, read with the length of the whole blob (so a reader
/// can tell the blob is the one it computed the range against).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeRead {
    /// The bytes of the range.
    pub bytes: Vec<u8>,
    /// The length of the whole blob.
    pub total: u64,
}

/// `range` of `whole`, the way a store without ranged reads serves it.
fn slice(whole: &[u8], range: Range<u64>) -> Result<RangeRead, BlobError> {
    let total = u64::try_from(whole.len()).unwrap_or(u64::MAX);
    let outside = BlobError::OutOfRange { total };
    let start = usize::try_from(range.start).map_err(|_| outside.clone())?;
    let end = usize::try_from(range.end).map_err(|_| outside.clone())?;
    let bytes = whole.get(start..end).ok_or(outside)?.to_vec();
    Ok(RangeRead { bytes, total })
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
    /// Read `range` of the content at `key`, with its whole length. The
    /// default reads the whole content and slices it; a store that serves
    /// ranges reads only the range.
    fn get_range(
        &self,
        key: &BlobRef,
        range: Range<u64>,
    ) -> impl Future<Output = Result<RangeRead, BlobError>> + Send {
        async move { slice(&self.get(key).await?, range) }
    }
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

/// The settings a store is built with: backend environment variables
/// (lowercased, as `object_store` config keys), then explicit options.
fn store_options(
    env: impl IntoIterator<Item = (String, String)>,
    options: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut settings: BTreeMap<String, String> = env
        .into_iter()
        .filter(|(key, _)| {
            ["AWS_", "GOOGLE_", "AZURE_"]
                .iter()
                .any(|p| key.starts_with(p))
        })
        .map(|(key, value)| (key.to_ascii_lowercase(), value))
        .collect();
    settings.extend(
        options
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.clone())),
    );
    settings
}

/// Blobs in any `object_store` backend: GCS, S3, local disk or memory.
#[derive(Debug, Clone)]
pub struct ObjectBlobs {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl ObjectBlobs {
    /// Blobs at a URL: `gs://bucket/prefix`, `s3://bucket/prefix`,
    /// `file:///dir`, or `memory:///`, configured from the environment
    /// (see [`Self::from_url_with`]).
    pub fn from_url(url: &str) -> Result<Self, BlobError> {
        Self::from_url_with(url, &BTreeMap::new())
    }

    /// Blobs at a URL, configured by the backend's standard environment
    /// variables (`AWS_*`, `GOOGLE_*`, `AZURE_*`, as `object_store` names
    /// them) and then `options` (e.g. `aws_endpoint`,
    /// `google_service_account_path`), which take precedence. Unknown keys
    /// are ignored.
    pub fn from_url_with(url: &str, options: &BTreeMap<String, String>) -> Result<Self, BlobError> {
        let parsed = url::Url::parse(url).map_err(|e| BlobError::Misconfigured(e.to_string()))?;
        let settings = store_options(std::env::vars(), options);
        let (store, prefix) = object_store::parse_url_opts(&parsed, settings)
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

    /// The length of the object at `path`.
    async fn length(&self, path: &Path) -> Result<u64, BlobError> {
        match self.store.head(path).await {
            Ok(meta) => Ok(meta.size),
            Err(object_store::Error::NotFound { .. }) => Err(BlobError::NotFound),
            Err(e) => Err(unavailable(&e)),
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
        let result = match self.store.get(&self.path(key)).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Err(BlobError::NotFound),
            Err(e) => return Err(unavailable(&e)),
        };
        result
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| unavailable(&e))
    }

    async fn get_range(&self, key: &BlobRef, range: Range<u64>) -> Result<RangeRead, BlobError> {
        let path = self.path(key);
        // An empty range is no request object_store serves: ask its length.
        if range.is_empty() {
            let total = self.length(&path).await?;
            return if range.start <= total {
                Ok(RangeRead {
                    bytes: Vec::new(),
                    total,
                })
            } else {
                Err(BlobError::OutOfRange { total })
            };
        }
        let options = GetOptions {
            range: Some(GetRange::Bounded(range.clone())),
            ..GetOptions::default()
        };
        match self.store.get_opts(&path, options).await {
            // A range past the end comes back cut short.
            Ok(result) if result.range != range => Err(BlobError::OutOfRange {
                total: result.meta.size,
            }),
            Ok(result) => {
                let total = result.meta.size;
                let bytes = result.bytes().await.map_err(|e| unavailable(&e))?;
                Ok(RangeRead {
                    bytes: bytes.to_vec(),
                    total,
                })
            }
            Err(object_store::Error::NotFound { .. }) => Err(BlobError::NotFound),
            // Backends report a range starting past the end in their own
            // words: tell it apart from a failure by the length.
            Err(error) => match self.length(&path).await? {
                total if range.end > total => Err(BlobError::OutOfRange { total }),
                _ => Err(unavailable(&error)),
            },
        }
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

#[cfg(test)]
mod option_tests {
    use super::*;

    #[test]
    fn environment_then_explicit_options() {
        let env = [
            ("AWS_ENDPOINT".to_owned(), "http://env".to_owned()),
            ("AWS_REGION".to_owned(), "us-east-1".to_owned()),
            ("HOME".to_owned(), "/home/x".to_owned()),
        ];
        let explicit = BTreeMap::from([("aws_endpoint".to_owned(), "http://explicit".to_owned())]);
        let settings = store_options(env, &explicit);
        assert_eq!(
            settings.get("aws_endpoint").map(String::as_str),
            Some("http://explicit")
        );
        assert_eq!(
            settings.get("aws_region").map(String::as_str),
            Some("us-east-1")
        );
        assert!(!settings.contains_key("home"));
    }
}

/// Blobs are per account on purpose (SPEC §3): the same message in two
/// mirrored mailboxes is two objects, so deleting one account's content,
/// one message at a time or all at once, never touches another's.
#[cfg(test)]
mod isolation_tests {
    use super::*;

    fn account(address: &str) -> AccountId {
        AccountId::parse(address).expect("an address")
    }

    /// A fresh directory for the local-disk backend, removed on drop.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "mailtide-blobs-{name}-{}-{}",
                std::process::id(),
                crate::now().0
            ));
            std::fs::create_dir_all(&dir).expect("scratch directory");
            Self(dir)
        }

        fn blobs(&self) -> ObjectBlobs {
            let url = url::Url::from_directory_path(&self.0).expect("an absolute path");
            ObjectBlobs::from_url(url.as_str()).expect("local disk")
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn the_same_message_id_in_two_accounts_has_two_keys() {
        let message = MessageId("18c2f0a1b2c3d4e5".into());
        let alice = blob_key(&account("alice@example.com"), &message);
        let bob = blob_key(&account("bob@example.com"), &message);
        assert_eq!(alice.0, "alice@example.com/18c2f0a1b2c3d4e5.eml");
        assert_eq!(bob.0, "bob@example.com/18c2f0a1b2c3d4e5.eml");
        assert_ne!(alice, bob);
    }

    async fn two_objects(blobs: &ObjectBlobs) {
        let message = MessageId("m1".into());
        let bytes = b"Message-ID: <same@example.com>\r\n\r\nSame bytes\r\n".to_vec();
        let alice = blob_key(&account("alice@example.com"), &message);
        let bob = blob_key(&account("bob@example.com"), &message);
        blobs.put(&alice, bytes.clone()).await.expect("put");
        blobs.put(&bob, bytes.clone()).await.expect("put");
        blobs.delete(&alice).await.expect("delete");
        assert_eq!(blobs.get(&alice).await, Err(BlobError::NotFound));
        assert_eq!(blobs.get(&bob).await.expect("still there"), bytes);
    }

    #[tokio::test]
    async fn identical_bytes_in_two_accounts_are_two_objects() {
        two_objects(&ObjectBlobs::memory()).await;
        let scratch = Scratch::new("two-objects");
        two_objects(&scratch.blobs()).await;
    }

    async fn delete_one_account(blobs: &ObjectBlobs) {
        let short = account("a@x.co");
        let longer = account("a@x.com");
        let deeper = account("a@x.co.uk");
        for who in [&short, &longer, &deeper] {
            for id in ["m1", "m2"] {
                let key = blob_key(who, &MessageId(id.into()));
                blobs.put(&key, b"raw".to_vec()).await.expect("put");
            }
        }
        assert_eq!(blobs.delete_account(&short).await, Ok(2));
        for id in ["m1", "m2"] {
            let id = MessageId(id.into());
            assert_eq!(
                blobs.get(&blob_key(&short, &id)).await,
                Err(BlobError::NotFound)
            );
            for kept in [&longer, &deeper] {
                assert_eq!(
                    blobs.get(&blob_key(kept, &id)).await.as_deref(),
                    Ok(&b"raw"[..]),
                    "{} lost {}",
                    kept.as_str(),
                    id.0
                );
            }
        }
    }

    #[tokio::test]
    async fn deleting_an_account_spares_an_account_whose_name_extends_it() {
        delete_one_account(&ObjectBlobs::memory()).await;
        let scratch = Scratch::new("prefix");
        delete_one_account(&scratch.blobs()).await;
    }
}
