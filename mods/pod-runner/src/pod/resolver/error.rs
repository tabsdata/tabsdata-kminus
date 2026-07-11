// Copyright 2026 Tabsdata Inc.

//! Unified resolver error type.
//!
//! [`ResolverError`] is the single shape used by both the secret and
//! configmap resolvers. The `R` marker parameter ([`SecretResource`] or
//! [`ConfigMapResource`]) selects the human-readable noun used in the
//! `Display` impl and in the [`From<PollError<...>>`] timeout message,
//! while keeping the two error families distinct at the type level so
//! the compiler still catches a secret resolver returning a configmap
//! error.
//!
//! The public type aliases [`SecretResolverError`] /
//! [`ConfigMapResolverError`] (and their `Kind` aliases) preserve the
//! pre-unification API so existing callers compile unchanged.

use std::marker::PhantomData;

use super::backoff::PollError;

// ---------------------------------------------------------------------------
// Resource markers
// ---------------------------------------------------------------------------

/// Marker trait identifying the kind of resource a [`ResolverError`]
/// is for. Implementors carry only a human-readable label used in
/// error messages.
///
/// `Debug` is a supertrait so `#[derive(Debug)]` on
/// [`ResolverError<R>`] (which holds a `PhantomData<R>`) compiles
/// without per-impl `R: Debug` bounds.
pub trait ResolverResource: Send + Sync + 'static + std::fmt::Debug {
    /// Noun used in error messages (e.g. `"secret"`, `"configmap"`).
    const LABEL: &'static str;
}

/// Marker for errors raised by secret resolvers.
#[derive(Debug, Clone, Copy)]
pub struct SecretResource;

impl ResolverResource for SecretResource {
    const LABEL: &'static str = "secret";
}

/// Marker for errors raised by configmap resolvers.
#[derive(Debug, Clone, Copy)]
pub struct ConfigMapResource;

impl ResolverResource for ConfigMapResource {
    const LABEL: &'static str = "configmap";
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Classification of a [`ResolverError`]. Drives the retry loop in the
/// resolver trait impls and lets callers distinguish "missing" from
/// "permanently broken".
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ResolverErrorKind {
    /// The named resource is not (yet) defined. Retryable.
    NotFound,
    /// The wait exceeded the timeout. Pod should transition to Failed.
    Timeout,
    /// The cancel signal fired during the wait.
    Cancelled,
    /// Non-retryable failure (parse error, unreachable kube API, etc.).
    Other,
}

/// Error returned by a resolver, parameterized by the [`ResolverResource`]
/// marker so secret and configmap errors are distinct types but share
/// the same shape and behavior.
#[derive(Debug, thiserror::Error)]
#[error("{} '{name}': {message}", R::LABEL)]
pub struct ResolverError<R: ResolverResource> {
    pub name: String,
    pub message: String,
    pub kind: ResolverErrorKind,
    _marker: PhantomData<R>,
}

impl<R: ResolverResource> ResolverError<R> {
    /// Build a `NotFound` error.
    pub fn not_found(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self::with_kind(name, message, ResolverErrorKind::NotFound)
    }

    /// Build an `Other` (non-retryable) error.
    pub fn other(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self::with_kind(name, message, ResolverErrorKind::Other)
    }

    fn with_kind(
        name: impl Into<String>,
        message: impl Into<String>,
        kind: ResolverErrorKind,
    ) -> Self {
        Self {
            name: name.into(),
            message: message.into(),
            kind,
            _marker: PhantomData,
        }
    }

    /// True when this is a [`ResolverErrorKind::NotFound`].
    pub fn is_not_found(&self) -> bool {
        self.kind == ResolverErrorKind::NotFound
    }
}

impl<R: ResolverResource> From<PollError<ResolverError<R>>> for ResolverError<R> {
    fn from(err: PollError<ResolverError<R>>) -> Self {
        match err {
            PollError::Timeout { last_err, elapsed } => Self {
                name: last_err.name,
                message: format!(
                    "timed out after {:.1}s waiting for {} to become available: {}",
                    elapsed.as_secs_f64(),
                    R::LABEL,
                    last_err.message,
                ),
                kind: ResolverErrorKind::Timeout,
                _marker: PhantomData,
            },
            PollError::Cancelled { elapsed } => Self {
                name: String::new(),
                message: format!(
                    "{} resolution cancelled after {:.1}s",
                    R::LABEL,
                    elapsed.as_secs_f64(),
                ),
                kind: ResolverErrorKind::Cancelled,
                _marker: PhantomData,
            },
            PollError::Fatal(e) => e,
        }
    }
}

// ---------------------------------------------------------------------------
// Aliases — preserve the pre-unification API surface
// ---------------------------------------------------------------------------

pub type SecretResolverError = ResolverError<SecretResource>;
pub type ConfigMapResolverError = ResolverError<ConfigMapResource>;

// One Kind enum but two aliases so call sites of the form
// `SecretResolverErrorKind::Timeout` keep compiling. Type aliases make
// variants reachable via the alias name.
pub type SecretResolverErrorKind = ResolverErrorKind;
pub type ConfigMapResolverErrorKind = ResolverErrorKind;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn display_uses_resource_label() {
        let s: SecretResolverError = ResolverError::not_found("creds", "file missing");
        assert_eq!(format!("{s}"), "secret 'creds': file missing");

        let c: ConfigMapResolverError = ResolverError::not_found("settings", "file missing");
        assert_eq!(format!("{c}"), "configmap 'settings': file missing");
    }

    #[test]
    fn constructors_and_predicate() {
        let nf: SecretResolverError = ResolverError::not_found("s", "missing");
        assert!(nf.is_not_found());
        assert_eq!(nf.kind, ResolverErrorKind::NotFound);

        let oth: ConfigMapResolverError = ResolverError::other("c", "boom");
        assert!(!oth.is_not_found());
        assert_eq!(oth.kind, ResolverErrorKind::Other);
    }

    #[test]
    fn from_poll_timeout_uses_resource_label() {
        let last: SecretResolverError = ResolverError::not_found("creds", "still missing");
        let poll_err = PollError::Timeout {
            last_err: last,
            elapsed: Duration::from_millis(2500),
        };
        let err: SecretResolverError = poll_err.into();
        assert_eq!(err.kind, ResolverErrorKind::Timeout);
        assert!(err.message.contains("waiting for secret"));
        assert!(err.message.contains("2.5s"));
    }

    #[test]
    fn from_poll_cancelled_uses_resource_label() {
        let poll_err: PollError<ConfigMapResolverError> = PollError::Cancelled {
            elapsed: Duration::from_secs(1),
        };
        let err: ConfigMapResolverError = poll_err.into();
        assert_eq!(err.kind, ResolverErrorKind::Cancelled);
        assert!(err.message.contains("configmap resolution cancelled"));
    }

    #[test]
    fn from_poll_fatal_passes_through() {
        let inner: SecretResolverError = ResolverError::other("creds", "parse failed");
        let poll_err = PollError::Fatal(inner);
        let err: SecretResolverError = poll_err.into();
        assert_eq!(err.kind, ResolverErrorKind::Other);
        assert_eq!(err.name, "creds");
    }
}
