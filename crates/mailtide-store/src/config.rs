//! Store configuration, parsed and validated once at the boundary.

use std::num::NonZeroU32;
use std::time::Duration;

/// A configuration a store can't be opened with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// The URL's scheme isn't `postgres://`, `postgresql://` or `sqlite:`.
    UnsupportedUrl(String),
    /// `min_connections` exceeds `max_connections`.
    MinAboveMax {
        /// The minimum asked for.
        min: u32,
        /// The maximum asked for.
        max: u32,
    },
    /// A timeout that must be positive was zero.
    ZeroTimeout(&'static str),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedUrl(scheme) => write!(
                f,
                "unsupported database URL scheme {scheme:?}: use postgres://, postgresql:// or sqlite:"
            ),
            Self::MinAboveMax { min, max } => {
                write!(f, "min_connections {min} exceeds max_connections {max}")
            }
            Self::ZeroTimeout(what) => write!(f, "{what} must be positive"),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Where the store lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseUrl {
    /// Postgres. Unix sockets: `postgres:///db?host=/socket/dir` (sqlx syntax,
    /// not a `SQLAlchemy` `postgresql+driver://` URL).
    Postgres(String),
    /// A `SQLite` file.
    SqliteFile(String),
    /// An in-memory `SQLite` database; it lives as long as the store.
    SqliteMemory(String),
}

impl DatabaseUrl {
    /// Parse a URL, rejecting schemes the store doesn't speak.
    pub fn parse(url: &str) -> Result<Self, ConfigError> {
        let url = url.trim();
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Ok(Self::Postgres(url.to_owned()))
        } else if let Some(rest) = url.strip_prefix("sqlite:") {
            let memory = rest.starts_with(":memory:") || rest.contains("mode=memory");
            if memory {
                Ok(Self::SqliteMemory(url.to_owned()))
            } else {
                Ok(Self::SqliteFile(url.to_owned()))
            }
        } else {
            let scheme = url.split(':').next().unwrap_or_default();
            Err(ConfigError::UnsupportedUrl(scheme.to_owned()))
        }
    }

    /// The URL as sqlx takes it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Postgres(url) | Self::SqliteFile(url) | Self::SqliteMemory(url) => url,
        }
    }
}

/// When the pool opens its connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Connect {
    /// On first use: building a store opens nothing (API processes that
    /// rarely touch the mirror hold no connection).
    Lazy,
    /// At construction, failing fast when the database is unreachable.
    Eager,
}

/// Connection pool limits. Postgres only: `SQLite` always uses one connection
/// (it serializes writers, and an in-memory database lives in it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolConfig {
    max_connections: NonZeroU32,
    min_connections: u32,
    acquire_timeout: Duration,
    idle_timeout: Option<Duration>,
    max_lifetime: Option<Duration>,
    connect: Connect,
}

impl Default for PoolConfig {
    /// Two connections (a run uses one at a time), none held while idle,
    /// opened lazily.
    fn default() -> Self {
        Self {
            max_connections: NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN),
            min_connections: 0,
            acquire_timeout: Duration::from_secs(10),
            idle_timeout: Some(Duration::from_mins(10)),
            max_lifetime: Some(Duration::from_mins(30)),
            connect: Connect::Lazy,
        }
    }
}

impl PoolConfig {
    /// A validated pool configuration.
    pub fn new(
        max_connections: NonZeroU32,
        min_connections: u32,
        acquire_timeout: Duration,
        idle_timeout: Option<Duration>,
        max_lifetime: Option<Duration>,
        connect: Connect,
    ) -> Result<Self, ConfigError> {
        if min_connections > max_connections.get() {
            return Err(ConfigError::MinAboveMax {
                min: min_connections,
                max: max_connections.get(),
            });
        }
        if acquire_timeout.is_zero() {
            return Err(ConfigError::ZeroTimeout("acquire_timeout"));
        }
        if idle_timeout.is_some_and(|t| t.is_zero()) {
            return Err(ConfigError::ZeroTimeout("idle_timeout"));
        }
        if max_lifetime.is_some_and(|t| t.is_zero()) {
            return Err(ConfigError::ZeroTimeout("max_lifetime"));
        }
        Ok(Self {
            max_connections,
            min_connections,
            acquire_timeout,
            idle_timeout,
            max_lifetime,
            connect,
        })
    }

    /// The most connections the pool opens.
    #[must_use]
    pub fn max_connections(&self) -> u32 {
        self.max_connections.get()
    }

    /// Connections kept open while idle.
    #[must_use]
    pub fn min_connections(&self) -> u32 {
        self.min_connections
    }

    /// How long a caller waits for a free connection.
    #[must_use]
    pub fn acquire_timeout(&self) -> Duration {
        self.acquire_timeout
    }

    /// Idle connections close after this long.
    #[must_use]
    pub fn idle_timeout(&self) -> Option<Duration> {
        self.idle_timeout
    }

    /// Connections are recycled after this long.
    #[must_use]
    pub fn max_lifetime(&self) -> Option<Duration> {
        self.max_lifetime
    }

    /// When connections open.
    #[must_use]
    pub fn connect(&self) -> Connect {
        self.connect
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_parse_at_the_boundary() {
        assert!(matches!(
            DatabaseUrl::parse("postgres:///app?host=/cloudsql/p:r:i"),
            Ok(DatabaseUrl::Postgres(_))
        ));
        assert!(matches!(
            DatabaseUrl::parse("postgresql://u@h/db"),
            Ok(DatabaseUrl::Postgres(_))
        ));
        assert!(matches!(
            DatabaseUrl::parse("sqlite::memory:"),
            Ok(DatabaseUrl::SqliteMemory(_))
        ));
        assert!(matches!(
            DatabaseUrl::parse("sqlite:mail.db?mode=rwc"),
            Ok(DatabaseUrl::SqliteFile(_))
        ));
        assert_eq!(
            DatabaseUrl::parse("postgresql+asyncpg://h/db"),
            Err(ConfigError::UnsupportedUrl("postgresql+asyncpg".into()))
        );
    }

    #[test]
    fn pools_are_validated() {
        let two = NonZeroU32::new(2).expect("2");
        assert_eq!(
            PoolConfig::new(two, 3, Duration::from_secs(1), None, None, Connect::Lazy),
            Err(ConfigError::MinAboveMax { min: 3, max: 2 })
        );
        assert_eq!(
            PoolConfig::new(two, 0, Duration::ZERO, None, None, Connect::Lazy),
            Err(ConfigError::ZeroTimeout("acquire_timeout"))
        );
        assert_eq!(PoolConfig::default().max_connections(), 2);
    }
}
