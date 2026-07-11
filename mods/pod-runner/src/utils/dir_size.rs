// Copyright 2026 Tabsdata Inc.

//! Disk usage scanner for pod work directories.
//!
//! Walks a directory tree and accumulates allocated bytes and block
//! counts, writing the result to a single `_size` JSON file at the
//! root passed to [`scan_dir`] — subdirectories are walked fresh each
//! call, no per-subtree caching. A `_skip` file in a directory
//! excludes it (and its descendants) from the count.
//!
//! Scans are interruptible: the shutdown signal is checked between
//! entries so cancellation is prompt on large trees.
//!
//! Both metrics are computed per-platform:
//! - Unix: `st_blocks` natively (in 512-byte units), bytes derived
//!   from it.
//! - Windows: bytes from `GetCompressedFileSizeW`, blocks derived as
//!   `ceil(bytes / 512)` (no native block count).
//! - Other targets: logical file size as bytes, blocks derived the
//!   same way as Windows.
//!
//! Reporting blocks in addition to bytes lets callers tell logical
//! size from real on-disk allocation when the platform exposes both.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// File written at the root of each scan, holding the total as a JSON
/// [`DirSizeStat`].
pub(crate) const SIZE_FILE: &str = "_size";

/// Sentinel that excludes a directory (and its descendants) from the count.
const SKIP_FILE: &str = "_skip";

/// Size of the block unit reported in [`DirSizeStat::blocks`]. Matches
/// POSIX `st_blocks` (512 bytes), used on every platform so totals are
/// comparable across them.
pub(crate) const BLOCK_BYTES: u64 = 512;

// ---------------------------------------------------------------------------
// DirSizeStat
// ---------------------------------------------------------------------------

/// Per-scan accumulated disk-usage metrics.
///
/// Serialized as JSON into the `_size` file at the scan root, e.g.
/// `{"bytes":28672,"blocks":56}`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DirSizeStat {
    /// Allocated bytes — what `du` reports.
    pub bytes: u64,
    /// Number of [`BLOCK_BYTES`]-byte blocks. On Unix this is the
    /// native `st_blocks`; on other platforms it's
    /// `ceil(bytes / BLOCK_BYTES)`.
    pub blocks: u64,
}

impl DirSizeStat {
    fn add(&mut self, other: DirSizeStat) {
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.blocks = self.blocks.saturating_add(other.blocks);
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned while scanning a directory tree.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ScanError {
    /// I/O error reading a path.
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// JSON serialization of the `_size` file failed.
    #[error("json error at {path}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

pub(crate) type ScanResult<T> = Result<T, ScanError>;

fn io_err(path: impl Into<PathBuf>, source: std::io::Error) -> ScanError {
    ScanError::Io {
        path: path.into(),
        source,
    }
}

// ---------------------------------------------------------------------------
// Per-entry allocation (platform-specific)
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn allocated_stat(_path: &Path, meta: &std::fs::Metadata) -> DirSizeStat {
    use std::os::unix::fs::MetadataExt;
    let blocks = meta.blocks();
    DirSizeStat {
        bytes: blocks.saturating_mul(BLOCK_BYTES),
        blocks,
    }
}

#[cfg(windows)]
fn allocated_stat(path: &Path, meta: &std::fs::Metadata) -> DirSizeStat {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::INVALID_FILE_SIZE;
    use windows_sys::Win32::Storage::FileSystem::GetCompressedFileSizeW;

    // GetCompressedFileSizeW is not meaningful for directories.
    if meta.is_dir() {
        return DirSizeStat::default();
    }

    let wide: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();

    let mut high: u32 = 0;
    // SAFETY: `wide` is null-terminated and `&mut high` is a valid pointer.
    let low = unsafe { GetCompressedFileSizeW(wide.as_ptr(), &mut high) };

    let bytes = if low == INVALID_FILE_SIZE {
        // Fall back to logical size rather than failing the scan.
        meta.len()
    } else {
        ((high as u64) << 32) | (low as u64)
    };
    DirSizeStat {
        bytes,
        // Windows has no native block count — derive from bytes.
        blocks: bytes.div_ceil(BLOCK_BYTES),
    }
}

#[cfg(not(any(unix, windows)))]
fn allocated_stat(_path: &Path, meta: &std::fs::Metadata) -> DirSizeStat {
    let bytes = meta.len();
    DirSizeStat {
        bytes,
        blocks: bytes.div_ceil(BLOCK_BYTES),
    }
}

// ---------------------------------------------------------------------------
// Size file I/O
// ---------------------------------------------------------------------------

async fn write_size_file(path: &Path, stat: DirSizeStat) -> ScanResult<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(SIZE_FILE);
    let tmp = parent.join(format!(".{file_name}.tmp.{}", std::process::id()));

    let json = serde_json::to_vec(&stat).map_err(|source| ScanError::Json {
        path: path.to_path_buf(),
        source,
    })?;

    {
        let mut f = fs::File::create(&tmp).await.map_err(|e| io_err(&tmp, e))?;
        f.write_all(&json).await.map_err(|e| io_err(&tmp, e))?;
        f.write_all(b"\n").await.map_err(|e| io_err(&tmp, e))?;
        f.sync_all().await.map_err(|e| io_err(&tmp, e))?;
    }
    fs::rename(&tmp, path).await.map_err(|e| io_err(path, e))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Scan
// ---------------------------------------------------------------------------

/// Scan a directory and return its total disk-usage metrics.
///
/// Writes the result as a JSON [`DirSizeStat`] to `_size` at `root`.
/// The walker recurses into every subdirectory and counts allocations
/// fresh on each call — it does **not** read or write `_size` files
/// in subdirectories, so the tree retains a single `_size` per
/// `scan_dir` call.
///
/// Rules:
/// - `_skip` present in a directory: that directory contributes 0, no
///   recursion.
/// - `_size` files encountered during the walk are counted toward the
///   total but are not consulted as a cache.
///
/// The shutdown signal is checked between entries so cancellation is
/// prompt on large trees.
pub(crate) async fn scan_dir(
    root: &Path,
    shutdown_rx: &mut watch::Receiver<()>,
) -> ScanResult<DirSizeStat> {
    if fs::try_exists(&root.join(SKIP_FILE))
        .await
        .map_err(|e| io_err(root.join(SKIP_FILE), e))?
    {
        return Ok(DirSizeStat::default());
    }

    let total = walk(root, shutdown_rx).await?;
    write_size_file(&root.join(SIZE_FILE), total).await?;
    Ok(total)
}

/// Recursive walker. Honors `_skip` markers at every level but does
/// not touch `_size` files in subdirectories — only [`scan_dir`]
/// writes the size file, and only at the root it was called on.
async fn walk(dir: &Path, shutdown_rx: &mut watch::Receiver<()>) -> ScanResult<DirSizeStat> {
    if shutdown_rx.has_changed().unwrap_or(true) {
        return Ok(DirSizeStat::default());
    }

    if fs::try_exists(&dir.join(SKIP_FILE))
        .await
        .map_err(|e| io_err(dir.join(SKIP_FILE), e))?
    {
        return Ok(DirSizeStat::default());
    }

    let mut total = DirSizeStat::default();

    // Count the directory entry's own allocation (matches `du`).
    let dir_meta = fs::symlink_metadata(dir)
        .await
        .map_err(|e| io_err(dir, e))?;
    total.add(allocated_stat(dir, &dir_meta));

    let mut entries = fs::read_dir(dir).await.map_err(|e| io_err(dir, e))?;
    while let Some(entry) = entries.next_entry().await.map_err(|e| io_err(dir, e))? {
        if shutdown_rx.has_changed().unwrap_or(true) {
            return Ok(total);
        }

        let path = entry.path();
        let meta = fs::symlink_metadata(&path)
            .await
            .map_err(|e| io_err(&path, e))?;

        if meta.file_type().is_dir() {
            let sub = Box::pin(walk(&path, shutdown_rx)).await?;
            total.add(sub);
        } else {
            // Regular file or symlink: count entry's own allocation,
            // do not follow.
            total.add(allocated_stat(&path, &meta));
        }
    }

    Ok(total)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs as stdfs;
    use std::sync::atomic::{AtomicU32, Ordering};

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    fn make_tmp_dir(name: &str) -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "pr-dirsize-test-{}-{}-{}",
            std::process::id(),
            name,
            n,
        ));
        let _ = stdfs::remove_dir_all(&dir);
        stdfs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn size_file_is_json_with_bytes_and_blocks() {
        let tmp = make_tmp_dir("json-shape");
        stdfs::write(tmp.join("payload"), vec![0u8; 4096]).unwrap();

        let (_tx, mut rx) = watch::channel(());
        let stat = scan_dir(&tmp, &mut rx).await.unwrap();

        let raw = stdfs::read_to_string(tmp.join(SIZE_FILE)).unwrap();
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["bytes"].as_u64().unwrap(), stat.bytes);
        assert_eq!(v["blocks"].as_u64().unwrap(), stat.blocks);
        // Bytes and blocks are consistent (blocks * BLOCK_BYTES >= bytes
        // because blocks rounds up on non-Unix; on Unix they're exactly
        // equal since bytes is derived from blocks * BLOCK_BYTES).
        assert!(stat.blocks.saturating_mul(BLOCK_BYTES) >= stat.bytes);

        let _ = stdfs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn scan_writes_size_file_for_empty_dir() {
        let tmp = make_tmp_dir("empty");
        let (_tx, mut rx) = watch::channel(());

        let stat = scan_dir(&tmp, &mut rx).await.unwrap();
        assert!(tmp.join(SIZE_FILE).exists(), "size file written");
        let cached: DirSizeStat =
            serde_json::from_str(&stdfs::read_to_string(tmp.join(SIZE_FILE)).unwrap()).unwrap();
        assert_eq!(cached, stat);

        let _ = stdfs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn scan_counts_file_allocations() {
        let tmp = make_tmp_dir("files");
        stdfs::write(tmp.join("a"), vec![0u8; 4096]).unwrap();
        stdfs::write(tmp.join("b"), vec![0u8; 4096]).unwrap();

        let (_tx, mut rx) = watch::channel(());
        let stat = scan_dir(&tmp, &mut rx).await.unwrap();

        assert!(stat.bytes > 0);
        assert!(stat.blocks > 0);
        let _ = stdfs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn skip_file_zeroes_subtree() {
        let tmp = make_tmp_dir("skip");
        let sub = tmp.join("sub");
        stdfs::create_dir_all(&sub).unwrap();
        stdfs::write(sub.join("file"), vec![0u8; 4096]).unwrap();
        stdfs::write(sub.join(SKIP_FILE), "").unwrap();

        let (_tx, mut rx) = watch::channel(());
        let with_skip = scan_dir(&tmp, &mut rx).await.unwrap();

        // Remove the skip marker and rescan — total should be larger.
        stdfs::remove_file(sub.join(SKIP_FILE)).unwrap();
        let _ = stdfs::remove_file(tmp.join(SIZE_FILE));
        let _ = stdfs::remove_file(sub.join(SIZE_FILE));

        let without_skip = scan_dir(&tmp, &mut rx).await.unwrap();
        assert!(without_skip.bytes > with_skip.bytes);
        assert!(without_skip.blocks > with_skip.blocks);

        let _ = stdfs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn root_skip_returns_zero() {
        let tmp = make_tmp_dir("root-skip");
        stdfs::write(tmp.join("file"), vec![0u8; 4096]).unwrap();
        stdfs::write(tmp.join(SKIP_FILE), "").unwrap();

        let (_tx, mut rx) = watch::channel(());
        let stat = scan_dir(&tmp, &mut rx).await.unwrap();
        assert_eq!(stat, DirSizeStat::default());

        let _ = stdfs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn subdir_size_files_are_not_written() {
        let tmp = make_tmp_dir("no-subtree-cache");
        let sub = tmp.join("sub");
        let deeper = sub.join("deeper");
        stdfs::create_dir_all(&deeper).unwrap();
        stdfs::write(deeper.join("payload"), vec![0u8; 4096]).unwrap();

        let (_tx, mut rx) = watch::channel(());
        scan_dir(&tmp, &mut rx).await.unwrap();

        assert!(tmp.join(SIZE_FILE).exists(), "root _size should exist");
        assert!(
            !sub.join(SIZE_FILE).exists(),
            "subdir _size should NOT exist"
        );
        assert!(
            !deeper.join(SIZE_FILE).exists(),
            "deeper subdir _size should NOT exist"
        );

        let _ = stdfs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn subtree_changes_reflected_in_root_total() {
        // Confirms there's no subtree cache: adding files in a subdir
        // shows up in the next root scan.
        let tmp = make_tmp_dir("no-cache-rescan");
        let sub = tmp.join("sub");
        stdfs::create_dir_all(&sub).unwrap();
        stdfs::write(sub.join("payload"), vec![0u8; 4096]).unwrap();

        let (_tx, mut rx) = watch::channel(());
        let first = scan_dir(&tmp, &mut rx).await.unwrap();

        stdfs::write(sub.join("extra"), vec![0u8; 64 * 1024]).unwrap();
        let second = scan_dir(&tmp, &mut rx).await.unwrap();

        assert!(
            second.bytes >= first.bytes + 64 * 1024,
            "subdir additions should be reflected in next scan (first={}, second={})",
            first.bytes,
            second.bytes,
        );

        let _ = stdfs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn root_size_file_is_rewritten() {
        let tmp = make_tmp_dir("root-size");
        // Plant a bogus _size that scan_dir must overwrite.
        stdfs::write(tmp.join(SIZE_FILE), r#"{"bytes":1,"blocks":1}"#).unwrap();
        stdfs::write(tmp.join("payload"), vec![0u8; 4096]).unwrap();

        let (_tx, mut rx) = watch::channel(());
        let stat = scan_dir(&tmp, &mut rx).await.unwrap();

        let written: DirSizeStat =
            serde_json::from_str(&stdfs::read_to_string(tmp.join(SIZE_FILE)).unwrap()).unwrap();
        assert_eq!(written, stat);
        assert!(stat.bytes > 1);

        let _ = stdfs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn scan_aborts_on_shutdown() {
        let tmp = make_tmp_dir("shutdown");
        for i in 0..10 {
            stdfs::write(tmp.join(format!("f{i}")), vec![0u8; 4096]).unwrap();
        }

        let (tx, mut rx) = watch::channel(());
        tx.send(()).unwrap();

        // Shutdown was signalled before the scan starts — the walker
        // exits between entries, so the returned total is partial.
        let stat = scan_dir(&tmp, &mut rx).await.unwrap();
        assert!(stat.bytes < 10 * 4096);

        let _ = stdfs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn nested_subdirs_are_recursed() {
        let tmp = make_tmp_dir("nested");
        let l1 = tmp.join("l1");
        let l2 = l1.join("l2");
        let l3 = l2.join("l3");
        stdfs::create_dir_all(&l3).unwrap();
        stdfs::write(l3.join("payload"), vec![0u8; 8192]).unwrap();

        let (_tx, mut rx) = watch::channel(());
        let stat = scan_dir(&tmp, &mut rx).await.unwrap();

        assert!(
            stat.bytes >= 8192,
            "deeply nested payload should be counted"
        );

        let _ = stdfs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn peer_subdirs_all_counted() {
        let tmp = make_tmp_dir("peers");
        for i in 0..3 {
            let sub = tmp.join(format!("sub{i}"));
            stdfs::create_dir_all(&sub).unwrap();
            stdfs::write(sub.join("payload"), vec![0u8; 8192]).unwrap();
        }

        let (_tx, mut rx) = watch::channel(());
        let stat = scan_dir(&tmp, &mut rx).await.unwrap();

        assert!(
            stat.bytes >= 3 * 8192,
            "all peer subtrees should be counted"
        );

        let _ = stdfs::remove_dir_all(&tmp);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_to_dir_is_not_followed() {
        use std::os::unix::fs::symlink;

        let tmp = make_tmp_dir("symlink");
        let target = tmp.join("target");
        let scanned = tmp.join("scanned");
        stdfs::create_dir_all(&target).unwrap();
        stdfs::create_dir_all(&scanned).unwrap();
        // Large payload behind the symlink — must not contribute.
        stdfs::write(target.join("payload"), vec![0u8; 1024 * 1024]).unwrap();
        symlink(&target, scanned.join("link-to-target")).unwrap();

        let (_tx, mut rx) = watch::channel(());
        let stat = scan_dir(&scanned, &mut rx).await.unwrap();

        assert!(stat.bytes < 1024 * 1024, "symlink should not be followed");

        let _ = stdfs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn nonexistent_root_returns_io_error() {
        let tmp = make_tmp_dir("missing");
        let missing = tmp.join("does-not-exist");

        let (_tx, mut rx) = watch::channel(());
        let err = scan_dir(&missing, &mut rx).await.unwrap_err();
        assert!(
            matches!(err, ScanError::Io { .. }),
            "expected Io error, got {err:?}",
        );

        let _ = stdfs::remove_dir_all(&tmp);
    }

    #[test]
    fn scan_error_display_includes_path_and_source() {
        let err = ScanError::Io {
            path: PathBuf::from("/tmp/foo"),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "missing"),
        };
        let rendered = format!("{err}");
        assert!(rendered.contains("/tmp/foo"), "{rendered}");
        assert!(rendered.contains("missing"), "{rendered}");
    }

    #[test]
    fn scan_error_source_chain_propagates() {
        use std::error::Error;

        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "nope");
        let err = ScanError::Io {
            path: PathBuf::from("/tmp/bar"),
            source: io_err,
        };
        assert!(err.source().is_some(), "Io error must expose source");
    }
}
