//! The runner's configuration: parsed and validated once, at the boundary.
//!
//! `RawConfig` mirrors the Python `mailtide.config.Config` dataclasses field
//! for field (plain numbers and strings, as Python holds them). `validate`
//! turns it into typed `Settings` or names the first offending field. Pure,
//! so it is tested without Python.

use std::num::NonZeroU32;
use std::time::Duration;

use mailtide::EngineConfig;
use mailtide::purge::PurgePolicy;
use mailtide::quota::Budget;
use mailtide_io::runner::{Access, LogAccounts, WatchMode};
use mailtide_io::transport::HttpConfig;
use mailtide_store::config::{Connect, DatabaseUrl, PoolConfig};

/// `mailtide.config.Database`.
#[derive(Debug, Clone, PartialEq)]
pub struct RawDatabase {
    pub url: String,
    pub max_connections: i64,
    pub min_connections: i64,
    pub acquire_timeout_s: f64,
    pub idle_timeout_s: Option<f64>,
    pub max_lifetime_s: Option<f64>,
    pub connect: String,
}

/// `mailtide.config.Http`.
#[derive(Debug, Clone, PartialEq)]
pub struct RawHttp {
    pub request_timeout_s: f64,
    pub connect_timeout_s: f64,
    pub user_agent: Option<String>,
    pub gmail_origin: Option<String>,
}

/// `mailtide.config.Engine`. Field names match the Python attributes.
#[allow(clippy::struct_field_names)]
#[derive(Debug, Clone, PartialEq)]
pub struct RawEngine {
    pub poll_active_s: f64,
    pub poll_idle_s: f64,
    pub idle_after_s: f64,
    pub watch_renew_margin_s: f64,
    pub draft_settle_s: f64,
    pub confirm_timeout_s: f64,
    pub label_refresh_s: f64,
    pub draft_refresh_delay_s: f64,
    pub draft_refresh_every_s: f64,
    pub draft_send_grace_s: f64,
}

/// `mailtide.config.Quota`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawQuota {
    pub account_per_minute: i64,
    pub project_per_minute: i64,
    pub project_per_day: i64,
    pub account_concurrency: i64,
    pub project_concurrency: i64,
    pub account_bytes_per_day: i64,
}

/// `mailtide.config.Poll` or `mailtide.config.Topic`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawWatch {
    pub kind: String,
    pub name: Option<String>,
}

/// `mailtide.config.Log`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawLog {
    pub level: String,
    pub accounts: String,
    pub logger: String,
}

/// `mailtide.config.Config`.
#[derive(Debug, Clone, PartialEq)]
pub struct RawConfig {
    pub access: String,
    pub purge: String,
    pub database: RawDatabase,
    pub blobs_url: String,
    pub blobs_options: std::collections::BTreeMap<String, String>,
    pub watch: RawWatch,
    pub http: RawHttp,
    pub engine: RawEngine,
    pub quota: RawQuota,
    pub log: RawLog,
    pub token_cache_margin_s: f64,
    pub max_message_bytes: i64,
}

/// A validated configuration.
#[derive(Debug, Clone)]
pub struct Settings {
    pub access: Access,
    pub purge: PurgePolicy,
    pub database_url: DatabaseUrl,
    pub pool: PoolConfig,
    pub blobs_url: String,
    pub blobs_options: std::collections::BTreeMap<String, String>,
    pub watch: WatchMode,
    pub http: HttpConfig,
    pub engine: EngineConfig,
    pub writes: mailtide_io::runner::WriteSettings,
    pub budget: Budget,
    pub token_cache_margin: Duration,
    pub log_verbosity: crate::logbridge::Verbosity,
    pub log_accounts: LogAccounts,
    pub logger: String,
}

/// The first invalid field and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invalid {
    /// Dotted path, e.g. `database.max_connections`.
    pub field: &'static str,
    /// What is wrong.
    pub reason: String,
}

impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.reason)
    }
}

fn invalid(field: &'static str, reason: impl Into<String>) -> Invalid {
    Invalid {
        field,
        reason: reason.into(),
    }
}

/// A non-negative, finite number of seconds.
fn duration(field: &'static str, seconds: f64) -> Result<Duration, Invalid> {
    Duration::try_from_secs_f64(seconds).map_err(|_| {
        invalid(
            field,
            format!("{seconds} is not a non-negative number of seconds"),
        )
    })
}

/// A positive, finite number of seconds.
fn positive(field: &'static str, seconds: f64) -> Result<Duration, Invalid> {
    let value = duration(field, seconds)?;
    if value.is_zero() {
        return Err(invalid(field, "must be positive"));
    }
    Ok(value)
}

fn count<T: TryFrom<i64>>(field: &'static str, value: i64) -> Result<T, Invalid> {
    T::try_from(value).map_err(|_| invalid(field, format!("{value} is out of range")))
}

fn nonzero(field: &'static str, value: i64) -> Result<NonZeroU32, Invalid> {
    NonZeroU32::new(count(field, value)?).ok_or_else(|| invalid(field, "must be at least 1"))
}

fn origin(value: &str) -> Result<String, Invalid> {
    let loopback = ["http://127.0.0.1", "http://localhost", "http://[::1]"]
        .iter()
        .any(|prefix| value.starts_with(prefix));
    if value.starts_with("https://") || loopback {
        Ok(value.trim_end_matches('/').to_owned())
    } else {
        Err(invalid(
            "http.gmail_origin",
            "must be https:// (plain http only for loopback test servers)",
        ))
    }
}

fn pool(raw: &RawDatabase) -> Result<(DatabaseUrl, PoolConfig), Invalid> {
    let url = DatabaseUrl::parse(&raw.url).map_err(|e| invalid("database.url", e.to_string()))?;
    let connect = match raw.connect.as_str() {
        "lazy" => Connect::Lazy,
        "eager" => Connect::Eager,
        other => {
            return Err(invalid(
                "database.connect",
                format!("{other:?} is not \"lazy\" or \"eager\""),
            ));
        }
    };
    let pool = PoolConfig::new(
        nonzero("database.max_connections", raw.max_connections)?,
        count("database.min_connections", raw.min_connections)?,
        positive("database.acquire_timeout_s", raw.acquire_timeout_s)?,
        raw.idle_timeout_s
            .map(|s| positive("database.idle_timeout_s", s))
            .transpose()?,
        raw.max_lifetime_s
            .map(|s| positive("database.max_lifetime_s", s))
            .transpose()?,
        connect,
    )
    .map_err(|e| invalid("database", e.to_string()))?;
    Ok((url, pool))
}

fn watch(raw: RawWatch) -> Result<WatchMode, Invalid> {
    match (raw.kind.as_str(), raw.name) {
        ("poll", _) => Ok(WatchMode::Disabled),
        ("topic", Some(name)) if !name.trim().is_empty() => Ok(WatchMode::Topic(name)),
        ("topic", _) => Err(invalid("watch.name", "a topic needs a name")),
        (other, _) => Err(invalid("watch", format!("unknown watch kind {other:?}"))),
    }
}

fn http(raw: RawHttp) -> Result<HttpConfig, Invalid> {
    let mut http = HttpConfig::new(positive("http.request_timeout_s", raw.request_timeout_s)?);
    http.connect_timeout = positive("http.connect_timeout_s", raw.connect_timeout_s)?;
    if let Some(agent) = raw.user_agent {
        http.user_agent = agent;
    }
    if let Some(value) = raw.gmail_origin {
        http.origin = origin(&value)?;
    }
    Ok(http)
}

fn engine(raw: &RawEngine, watch: &WatchMode) -> Result<EngineConfig, Invalid> {
    let engine = EngineConfig {
        watch: watch.policy(),
        poll_active: positive("engine.poll_active_s", raw.poll_active_s)?,
        poll_idle: positive("engine.poll_idle_s", raw.poll_idle_s)?,
        idle_after: positive("engine.idle_after_s", raw.idle_after_s)?,
        watch_renew_margin: duration("engine.watch_renew_margin_s", raw.watch_renew_margin_s)?,
        draft_settle: duration("engine.draft_settle_s", raw.draft_settle_s)?,
        confirm_timeout: duration("engine.confirm_timeout_s", raw.confirm_timeout_s)?,
        label_refresh: positive("engine.label_refresh_s", raw.label_refresh_s)?,
        draft_refresh_delay: duration("engine.draft_refresh_delay_s", raw.draft_refresh_delay_s)?,
        draft_refresh_every: positive("engine.draft_refresh_every_s", raw.draft_refresh_every_s)?,
        draft_send_grace: duration("engine.draft_send_grace_s", raw.draft_send_grace_s)?,
    };
    if engine.poll_active > engine.poll_idle {
        return Err(invalid(
            "engine.poll_active_s",
            "must not exceed engine.poll_idle_s",
        ));
    }
    Ok(engine)
}

fn positive_u64(field: &'static str, value: i64) -> Result<u64, Invalid> {
    match count::<u64>(field, value)? {
        0 => Err(invalid(field, "must be at least 1")),
        v => Ok(v),
    }
}

fn budget(raw: &RawQuota) -> Result<Budget, Invalid> {
    let budget = Budget {
        account_per_minute: nonzero("quota.account_per_minute", raw.account_per_minute)?.get(),
        project_per_minute: nonzero("quota.project_per_minute", raw.project_per_minute)?.get(),
        project_per_day: positive_u64("quota.project_per_day", raw.project_per_day)?,
        account_concurrency: nonzero("quota.account_concurrency", raw.account_concurrency)?.get(),
        project_concurrency: nonzero("quota.project_concurrency", raw.project_concurrency)?.get(),
        account_bytes_per_day: positive_u64(
            "quota.account_bytes_per_day",
            raw.account_bytes_per_day,
        )?,
    };
    // The largest request (an import, 60 units) must fit the smallest share
    // (backfill gets 70% of a minute's budget), or imports never run.
    if u64::from(budget.account_per_minute) * 70 / 100 < 60 {
        return Err(invalid(
            "quota.account_per_minute",
            "too small for one import (needs at least 86)",
        ));
    }
    Ok(budget)
}

fn log(raw: &RawLog) -> Result<(crate::logbridge::Verbosity, LogAccounts, String), Invalid> {
    use crate::logbridge::Verbosity;
    use tracing::Level;
    let verbosity = match raw.level.as_str() {
        "off" => Verbosity::Off,
        "error" => Verbosity::Up(Level::ERROR),
        "warning" => Verbosity::Up(Level::WARN),
        "info" => Verbosity::Up(Level::INFO),
        "debug" => Verbosity::Up(Level::DEBUG),
        "trace" => Verbosity::Up(Level::TRACE),
        other => {
            return Err(invalid(
                "log.level",
                format!("{other:?} is not off, error, warning, info, debug or trace"),
            ));
        }
    };
    let accounts = match raw.accounts.as_str() {
        "hashed" => LogAccounts::Hashed,
        "plain" => LogAccounts::Plain,
        other => {
            return Err(invalid(
                "log.accounts",
                format!("{other:?} is not \"hashed\" or \"plain\""),
            ));
        }
    };
    if raw.logger.trim().is_empty() {
        return Err(invalid("log.logger", "needs a logger name"));
    }
    Ok((verbosity, accounts, raw.logger.clone()))
}

/// Validate the whole configuration, section by section.
pub fn validate(raw: RawConfig) -> Result<Settings, Invalid> {
    let (database_url, pool) = pool(&raw.database)?;
    let (log_verbosity, log_accounts, logger) = log(&raw.log)?;
    let access = match raw.access.as_str() {
        "read_write" => Access::ReadWrite,
        "read_only" => Access::ReadOnly,
        other => {
            return Err(invalid(
                "access",
                format!("{other:?} is not \"read_write\" or \"read_only\""),
            ));
        }
    };
    // Off unless switched on: the one operation that destroys mail.
    let purge = match raw.purge.as_str() {
        "off" => PurgePolicy::Off,
        "trash_and_spam" => PurgePolicy::TrashAndSpam,
        "anywhere" => PurgePolicy::Anywhere,
        other => {
            return Err(invalid(
                "purge",
                format!("{other:?} is not \"off\", \"trash_and_spam\" or \"anywhere\""),
            ));
        }
    };
    let watch = watch(raw.watch)?;
    let engine = engine(&raw.engine, &watch)?;
    let http = http(raw.http)?;
    // A send retry waits out the request timeout the transport applies.
    let writes = mailtide_io::runner::WriteSettings {
        max_message_bytes: positive_u64("max_message_bytes", raw.max_message_bytes)
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX))?,
        request_timeout: http.request_timeout,
    };
    Ok(Settings {
        access,
        purge,
        database_url,
        pool,
        blobs_url: raw.blobs_url,
        blobs_options: raw.blobs_options,
        watch,
        http,
        engine,
        writes,
        budget: budget(&raw.quota)?,
        token_cache_margin: duration("token_cache_margin_s", raw.token_cache_margin_s)?,
        log_verbosity,
        log_accounts,
        logger,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults() -> RawConfig {
        RawConfig {
            access: "read_write".into(),
            purge: "off".into(),
            database: RawDatabase {
                url: "postgres:///app?host=/cloudsql/p:r:i".into(),
                max_connections: 2,
                min_connections: 0,
                acquire_timeout_s: 10.0,
                idle_timeout_s: Some(600.0),
                max_lifetime_s: Some(1800.0),
                connect: "lazy".into(),
            },
            blobs_url: "memory:///".into(),
            blobs_options: std::collections::BTreeMap::new(),
            watch: RawWatch {
                kind: "poll".into(),
                name: None,
            },
            http: RawHttp {
                request_timeout_s: 60.0,
                connect_timeout_s: 10.0,
                user_agent: None,
                gmail_origin: None,
            },
            engine: RawEngine {
                poll_active_s: 60.0,
                poll_idle_s: 900.0,
                idle_after_s: 3600.0,
                watch_renew_margin_s: 3600.0,
                draft_settle_s: 120.0,
                confirm_timeout_s: 60.0,
                label_refresh_s: 900.0,
                draft_refresh_delay_s: 10.0,
                draft_refresh_every_s: 900.0,
                draft_send_grace_s: 3600.0,
            },
            quota: RawQuota {
                account_per_minute: 4_800,
                project_per_minute: 960_000,
                project_per_day: 64_000_000,
                account_concurrency: 8,
                project_concurrency: 128,
                account_bytes_per_day: 2_000_000_000,
            },
            log: RawLog {
                level: "info".into(),
                accounts: "hashed".into(),
                logger: "mailtide".into(),
            },
            token_cache_margin_s: 60.0,
            max_message_bytes: 35_651_584,
        }
    }

    #[test]
    fn defaults_validate() {
        let settings = validate(defaults()).expect("valid");
        assert_eq!(settings.pool.max_connections(), 2);
        assert_eq!(settings.watch, WatchMode::Disabled);
        assert!(settings.http.user_agent.starts_with("mailtide/"));
        assert_eq!(settings.purge, PurgePolicy::Off, "purge is off by default");
    }

    #[test]
    fn errors_name_the_field() {
        let mut raw = defaults();
        raw.database.url = "postgresql+asyncpg://h/db".into();
        assert_eq!(validate(raw).map(|_| ()).unwrap_err().field, "database.url");

        let mut raw = defaults();
        raw.database.max_connections = 0;
        assert_eq!(
            validate(raw).map(|_| ()).unwrap_err().field,
            "database.max_connections"
        );

        let mut raw = defaults();
        raw.engine.poll_active_s = 1000.0;
        assert_eq!(
            validate(raw).map(|_| ()).unwrap_err().field,
            "engine.poll_active_s"
        );

        let mut raw = defaults();
        raw.engine.label_refresh_s = 0.0;
        assert_eq!(
            validate(raw).map(|_| ()).unwrap_err().field,
            "engine.label_refresh_s"
        );

        let mut raw = defaults();
        raw.http.request_timeout_s = f64::NAN;
        assert_eq!(
            validate(raw).map(|_| ()).unwrap_err().field,
            "http.request_timeout_s"
        );

        let mut raw = defaults();
        raw.watch = RawWatch {
            kind: "topic".into(),
            name: None,
        };
        assert_eq!(validate(raw).map(|_| ()).unwrap_err().field, "watch.name");

        let mut raw = defaults();
        raw.http.gmail_origin = Some("http://evil.example".into());
        assert_eq!(
            validate(raw).map(|_| ()).unwrap_err().field,
            "http.gmail_origin"
        );

        let mut raw = defaults();
        raw.purge = "everything".into();
        assert_eq!(validate(raw).map(|_| ()).unwrap_err().field, "purge");

        let mut raw = defaults();
        raw.quota.account_per_minute = 50;
        assert_eq!(
            validate(raw).map(|_| ()).unwrap_err().field,
            "quota.account_per_minute"
        );
    }
}
