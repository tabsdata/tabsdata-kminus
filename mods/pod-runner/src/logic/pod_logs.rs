// Copyright 2026 Tabsdata Inc.

//! Pod logs by log pod id — the container logs of every pod filed under
//! the id (see [`crate::store::layout`]), read from the pod-state dirs
//! whether or not the runner still knows the pods.
//!
//! The stream is what reading the logs in order would show:
//!
//! - **incarnations** — the pods filed under the id — oldest first. Each
//!   opens with a separator naming its creation time and pod name
//!   ([`incarnation_header`]). With a pod selected, only that pod, with no
//!   separator: the caller reads one pod at a time and writes its own.
//! - **containers** in spec order, init containers first, one after the
//!   other — how `kubectl logs --all-containers` joins them. With more than
//!   one, each opens with a [`container_header`].
//! - **lines** as the kubelet serves them: stdout and stderr already
//!   interleaved in the CRI file, written as the message alone, or
//!   `<ts> <message>` with timestamps. Restarts go oldest first and, within
//!   a restart, rotated files (`N.log.4` … `N.log.1`) come before `N.log`.
//!   Probe logs are not container output and are left out.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::pod::container_log::CriFormatter;
use crate::server::config::{POD_LOG_SUBDIR, POD_STORE_SUBDIR};
use crate::spec::pod::{INCARNATION_STAMP_FORMAT, PodSpec};
use crate::store::layout::{PodLayout, is_kube_name, parse_incarnation};

/// Read buffer size for log files.
const CHUNK_SIZE: usize = 8192;

/// Channel depth between the reader and the HTTP body.
const CHANNEL_DEPTH: usize = 32;

/// Width of the separator rules.
const RULE_WIDTH: usize = 60;

/// File name of the spec each incarnation's store keeps.
const ORIGINAL_SPEC_FILE: &str = "original-spec.json";

/// Options for [`stream_pod_logs`].
#[derive(Debug, Clone, Default)]
pub struct PodLogsOptions {
    /// Only the pod with this name among those filed under the id, written
    /// without its `POD STARTED` separator — for a caller that reads one pod
    /// at a time and writes its own. `None` means every pod, separated.
    pub pod: Option<String>,
    /// Only this container. `None` means every container.
    pub container: Option<String>,
    /// Only the last N lines of each container, per incarnation.
    pub tail_lines: Option<u64>,
    /// Leave out the newest incarnation.
    pub previous: bool,
    /// Prefix each line with its CRI timestamp.
    pub timestamps: bool,
}

/// Why there are no logs to stream.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PodLogsError {
    /// A log pod id or container name that is not a Kubernetes name. Names
    /// become path components, so anything else is refused outright.
    #[error("invalid {kind} {name:?}: not a Kubernetes name")]
    InvalidName { kind: &'static str, name: String },
    /// No pod is filed under the log pod id.
    #[error("no logs for log pod id {0}")]
    NoPod(String),
    /// `previous` asked for an incarnation before the only one.
    #[error("log pod id {0} has no previous incarnation")]
    NoPrevious(String),
    /// No incarnation has logs for the container.
    #[error("no logs for container {container} of log pod id {pod}")]
    NoContainer { pod: String, container: String },
    /// No pod of that name is filed under the id.
    #[error("no logs for pod {pod} under log pod id {id}")]
    NoPodUnderId { id: String, pod: String },
}

/// One incarnation to stream: its dir name (`<stamp>_<pod_name>`) and the
/// log dirs of its containers, in output order.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Incarnation {
    name: String,
    containers: Vec<(String, PathBuf)>,
}

/// Stream the logs of every pod filed under `log_pod_id`, oldest first.
///
/// What to stream is settled before returning, so a pod or container with
/// nothing on disk is an error rather than an empty stream. The files are
/// then read on a blocking task; a read error ends the stream with it.
pub fn stream_pod_logs(
    layout: &PodLayout,
    log_pod_id: &str,
    options: PodLogsOptions,
) -> Result<mpsc::Receiver<Result<Bytes, io::Error>>, PodLogsError> {
    let plan = plan(layout, log_pod_id, &options)?;
    let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
    tokio::task::spawn_blocking(move || {
        if let Err(e) = write_all(&plan, &options, &tx) {
            let _ = tx.blocking_send(Err(e));
        }
    });
    Ok(rx)
}

/// Settle which incarnations and containers to stream.
fn plan(
    layout: &PodLayout,
    log_pod_id: &str,
    options: &PodLogsOptions,
) -> Result<Vec<Incarnation>, PodLogsError> {
    check_name("log pod id", log_pod_id)?;
    if let Some(container) = &options.container {
        check_name("container name", container)?;
    }
    if let Some(pod) = &options.pod {
        check_name("pod name", pod)?;
    }
    let mut incarnations: Vec<String> = subdir_names(&layout.log_pod_dir(log_pod_id))
        .into_iter()
        .filter(|name| parse_incarnation(name).is_some())
        .collect();
    // `<stamp>_<pod_name>` with a fixed-width UTC stamp: sorting the names
    // sorts by creation time, then by pod name within a second.
    incarnations.sort();
    if incarnations.is_empty() {
        return Err(PodLogsError::NoPod(log_pod_id.to_string()));
    }
    if let Some(pod) = &options.pod {
        // A bare pod recreated under its name has several incarnations;
        // they all match, oldest first.
        incarnations.retain(|name| parse_incarnation(name).is_some_and(|(_, p)| p == pod));
        if incarnations.is_empty() {
            return Err(PodLogsError::NoPodUnderId {
                id: log_pod_id.to_string(),
                pod: pod.clone(),
            });
        }
    }
    if options.previous {
        incarnations.pop();
        if incarnations.is_empty() {
            return Err(PodLogsError::NoPrevious(log_pod_id.to_string()));
        }
    }

    let plan: Vec<Incarnation> = incarnations
        .into_iter()
        .map(|name| {
            let state_dir = layout.state_dir_at(log_pod_id, &name);
            let log_dir = state_dir.join(POD_LOG_SUBDIR);
            let containers = container_order(&state_dir, &log_dir)
                .into_iter()
                .filter(|c| options.container.as_ref().is_none_or(|only| only == c))
                .map(|c| {
                    let dir = log_dir.join(&c);
                    (c, dir)
                })
                .collect();
            Incarnation { name, containers }
        })
        .collect();

    if let Some(container) = &options.container
        && plan.iter().all(|inc| inc.containers.is_empty())
    {
        return Err(PodLogsError::NoContainer {
            pod: log_pod_id.to_string(),
            container: container.clone(),
        });
    }
    Ok(plan)
}

/// Refuse anything but a Kubernetes object name, so a name can never step
/// out of its dir (`..`, `/`).
fn check_name(kind: &'static str, name: &str) -> Result<(), PodLogsError> {
    if is_kube_name(name) {
        Ok(())
    } else {
        Err(PodLogsError::InvalidName {
            kind,
            name: name.to_string(),
        })
    }
}

/// The containers of an incarnation that have a log dir, in spec order
/// (init containers first). A container on disk but not in the spec — the
/// spec unreadable, say — follows, by name.
fn container_order(state_dir: &Path, log_dir: &Path) -> Vec<String> {
    let mut on_disk = subdir_names(log_dir);
    on_disk.sort();
    let spec_order: Vec<String> =
        std::fs::read(state_dir.join(POD_STORE_SUBDIR).join(ORIGINAL_SPEC_FILE))
            .ok()
            .and_then(|raw| serde_json::from_slice::<PodSpec>(&raw).ok())
            .map(|spec| spec.all_container_names())
            .unwrap_or_default();

    let mut ordered: Vec<String> = spec_order
        .into_iter()
        .filter(|c| on_disk.contains(c))
        .collect();
    for c in on_disk {
        if !ordered.contains(&c) {
            ordered.push(c);
        }
    }
    ordered
}

/// A container's log files in the order they were written: restarts
/// oldest first, and within a restart the rotated files, oldest first,
/// before the current one.
fn log_files(container_dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(container_dir) else {
        return Vec::new();
    };
    let mut files: Vec<((u64, std::cmp::Reverse<u64>), PathBuf)> = entries
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            let key = log_file_key(&name)?;
            Some((key, e.path()))
        })
        .collect();
    files.sort();
    files.into_iter().map(|(_, path)| path).collect()
}

/// Sort key of a container log file name: `<restart>.log` is
/// `(restart, Reverse(0))`, `<restart>.log.<k>` is `(restart, Reverse(k))`.
/// `None` for anything else — probe logs included.
fn log_file_key(name: &str) -> Option<(u64, std::cmp::Reverse<u64>)> {
    let (restart, rest) = name.split_once(".log")?;
    let restart: u64 = restart.parse().ok()?;
    let rotation = match rest {
        "" => 0,
        r => r.strip_prefix('.')?.parse().ok()?,
    };
    Some((restart, std::cmp::Reverse(rotation)))
}

/// The separator opening an incarnation, from its dir name
/// (`<stamp>_<pod_name>`).
pub fn incarnation_header(incarnation: &str) -> String {
    let (stamp, pod) = parse_incarnation(incarnation).unwrap_or((incarnation, ""));
    let started = chrono::NaiveDateTime::parse_from_str(stamp, INCARNATION_STAMP_FORMAT)
        .map(|t| {
            t.and_utc()
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        })
        .unwrap_or_else(|_| stamp.to_string());
    let rule = "-".repeat(RULE_WIDTH);
    format!("{rule}\nPOD STARTED {started} {pod}\n{rule}\n")
}

/// The line opening a container, when the stream carries more than one.
pub fn container_header(container: &str) -> String {
    format!("--- container {container} ---\n")
}

/// Write the planned incarnations into `tx`. Stops quietly when the
/// receiver is gone.
fn write_all(
    plan: &[Incarnation],
    options: &PodLogsOptions,
    tx: &mpsc::Sender<Result<Bytes, io::Error>>,
) -> io::Result<()> {
    let send = |bytes: Vec<u8>| tx.blocking_send(Ok(Bytes::from(bytes))).is_ok();
    let label_containers = options.container.is_none();

    let separate_pods = options.pod.is_none();

    for incarnation in plan {
        if separate_pods && !send(incarnation_header(&incarnation.name).into_bytes()) {
            return Ok(());
        }
        for (container, dir) in &incarnation.containers {
            if label_containers && !send(container_header(container).into_bytes()) {
                return Ok(());
            }
            let files = log_files(dir);
            let (first, offset) = match options.tail_lines {
                Some(n) => tail_start(&files, n)?,
                None => (0, 0),
            };
            for (i, path) in files.iter().enumerate().skip(first) {
                let from = if i == first { offset } else { 0 };
                if !write_file(path, from, options.timestamps, &send)? {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

/// Write one log file from `offset`, CRI-reformatted. Returns `false` when
/// the receiver is gone.
fn write_file(
    path: &Path,
    offset: u64,
    timestamps: bool,
    send: &impl Fn(Vec<u8>) -> bool,
) -> io::Result<bool> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        // Rotated away between listing and opening.
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(true),
        Err(e) => return Err(e),
    };
    file.seek(SeekFrom::Start(offset))?;
    let mut formatter = CriFormatter::new(timestamps);
    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        let out = formatter.push(&buf[..n]);
        if !out.is_empty() && !send(out) {
            return Ok(false);
        }
    }
    let mut tail = formatter.finish();
    if !tail.is_empty() {
        // Files are joined back to back; a last line must not run into the
        // next file's first.
        tail.push(b'\n');
        if !send(tail) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Where the last `n` lines across `files` begin: the index of the file
/// and the byte offset in it. `n == 0` starts past the end.
fn tail_start(files: &[PathBuf], n: u64) -> io::Result<(usize, u64)> {
    if n == 0 {
        return Ok((files.len(), 0));
    }
    let mut needed = n;
    for (i, path) in files.iter().enumerate().rev() {
        let (offset, found) = last_lines(path, needed)?;
        if found >= needed {
            return Ok((i, offset));
        }
        needed -= found;
    }
    Ok((0, 0))
}

/// Find where the last `k` lines of a file begin. Returns that offset and
/// how many lines were found — fewer than `k` when the file holds fewer,
/// in which case the offset is 0.
fn last_lines(path: &Path, k: u64) -> io::Result<(u64, u64)> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(e) => return Err(e),
    };
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok((0, 0));
    }

    // The final newline ends the last line rather than starting another.
    let mut last = [0u8; 1];
    file.seek(SeekFrom::Start(len - 1))?;
    file.read_exact(&mut last)?;
    let mut end = if last[0] == b'\n' { len - 1 } else { len };

    let mut newlines = 0u64;
    let mut buf = vec![0u8; CHUNK_SIZE];
    while end > 0 {
        let start = end.saturating_sub(CHUNK_SIZE as u64);
        let size = (end - start) as usize;
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut buf[..size])?;
        for (j, &b) in buf[..size].iter().enumerate().rev() {
            if b == b'\n' {
                newlines += 1;
                if newlines == k {
                    return Ok((start + j as u64 + 1, k));
                }
            }
        }
        end = start;
    }
    Ok((0, newlines + 1))
}

/// The names of the subdirectories of `dir`; empty when it is absent.
fn subdir_names(dir: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::pod::{Container, PodMetadata, PodSpecBody};

    fn tmp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pod-runner-pod-logs-{}-{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn cri(ts: &str, stream: &str, msg: &str) -> String {
        format!("{ts} {stream} F {msg}\n")
    }

    /// Plant an incarnation: its spec (init + regular containers) and log
    /// files, each given as `(container, file name, contents)`.
    fn plant(
        layout: &PodLayout,
        name: &str,
        stamp: &str,
        init: &[&str],
        regular: &[&str],
        files: &[(&str, &str, &str)],
    ) {
        // Unlabeled pods: filed under their own name.
        let state = layout.state_dir_at(name, &format!("{stamp}_{name}"));
        let container = |n: &&str| Container {
            name: n.to_string(),
            ..Default::default()
        };
        let spec = PodSpec {
            metadata: PodMetadata {
                name: name.into(),
                ..Default::default()
            },
            spec: PodSpecBody {
                init_containers: init.iter().map(container).collect(),
                containers: regular.iter().map(container).collect(),
                ..Default::default()
            },
            ..Default::default()
        };
        let store = state.join(POD_STORE_SUBDIR);
        std::fs::create_dir_all(&store).unwrap();
        std::fs::write(
            store.join(ORIGINAL_SPEC_FILE),
            serde_json::to_vec(&spec).unwrap(),
        )
        .unwrap();
        for (c, file, contents) in files {
            let dir = state.join(POD_LOG_SUBDIR).join(c);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(file), contents).unwrap();
        }
    }

    async fn collect(
        layout: &PodLayout,
        name: &str,
        options: PodLogsOptions,
    ) -> Result<String, PodLogsError> {
        let mut rx = stream_pod_logs(layout, name, options)?;
        let mut out = Vec::new();
        while let Some(chunk) = rx.recv().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        Ok(String::from_utf8(out).unwrap())
    }

    const TS: &str = "2026-10-19T14:30:05.000000000Z";

    #[test]
    fn log_file_keys_order_restarts_then_rotations() {
        let mut names = vec![
            "1.log",
            "0.log.1",
            "0.log",
            "probe-startup.log",
            "0.log.2",
            "1.log.1",
            "10.log",
            "2.log",
            "junk",
            "0.log.x",
        ];
        names.retain(|n| log_file_key(n).is_some());
        names.sort_by_key(|n| log_file_key(n).unwrap());
        assert_eq!(
            names,
            [
                "0.log.2", "0.log.1", "0.log", "1.log.1", "1.log", "2.log", "10.log"
            ]
        );
    }

    #[test]
    fn header_shows_the_creation_time_as_rfc3339() {
        let h = incarnation_header("20261019T143005Z_web-7d9f-x7k2p");
        assert!(
            h.contains("POD STARTED 2026-10-19T14:30:05Z web-7d9f-x7k2p\n"),
            "{h}"
        );
        assert!(h.starts_with(&"-".repeat(RULE_WIDTH)));
        assert!(h.ends_with(&format!("{}\n", "-".repeat(RULE_WIDTH))));
    }

    #[tokio::test]
    async fn incarnations_oldest_first_each_with_a_separator() {
        let base = tmp("order");
        let layout = PodLayout::new(&base);
        plant(
            &layout,
            "app",
            "20261019T150000Z",
            &[],
            &["main"],
            &[("main", "0.log", &cri(TS, "stdout", "second"))],
        );
        plant(
            &layout,
            "app",
            "20261019T140000Z",
            &[],
            &["main"],
            &[("main", "0.log", &cri(TS, "stdout", "first"))],
        );

        let out = collect(&layout, "app", PodLogsOptions::default())
            .await
            .unwrap();

        let expected = format!(
            "{}{}first\n{}{}second\n",
            incarnation_header("20261019T140000Z_app"),
            container_header("main"),
            incarnation_header("20261019T150000Z_app"),
            container_header("main"),
        );
        assert_eq!(out, expected);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn containers_in_spec_order_and_files_in_write_order() {
        let base = tmp("containers");
        let layout = PodLayout::new(&base);
        plant(
            &layout,
            "app",
            "20261019T140000Z",
            &["setup"],
            &["web", "agent"],
            &[
                ("agent", "0.log", &cri(TS, "stdout", "agent")),
                ("web", "1.log", &cri(TS, "stdout", "web r1")),
                ("web", "0.log", &cri(TS, "stdout", "web r0 current")),
                ("web", "0.log.1", &cri(TS, "stderr", "web r0 rotated")),
                ("web", "probe-liveness.log", "probe noise\n"),
                ("setup", "0.log", &cri(TS, "stdout", "setup")),
            ],
        );

        let out = collect(&layout, "app", PodLogsOptions::default())
            .await
            .unwrap();

        let body: Vec<&str> = out.lines().skip(3).collect();
        assert_eq!(
            body,
            [
                "--- container setup ---",
                "setup",
                "--- container web ---",
                "web r0 rotated",
                "web r0 current",
                "web r1",
                "--- container agent ---",
                "agent",
            ]
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn one_container_has_no_container_header() {
        let base = tmp("one-container");
        let layout = PodLayout::new(&base);
        plant(
            &layout,
            "app",
            "20261019T140000Z",
            &[],
            &["web", "agent"],
            &[
                ("web", "0.log", &cri(TS, "stdout", "web")),
                ("agent", "0.log", &cri(TS, "stdout", "agent")),
            ],
        );

        let out = collect(
            &layout,
            "app",
            PodLogsOptions {
                container: Some("agent".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(
            out,
            format!("{}agent\n", incarnation_header("20261019T140000Z_app"))
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn timestamps_keep_the_cri_timestamp() {
        let base = tmp("timestamps");
        let layout = PodLayout::new(&base);
        plant(
            &layout,
            "app",
            "20261019T140000Z",
            &[],
            &["main"],
            &[("main", "0.log", &cri(TS, "stderr", "boom"))],
        );

        let out = collect(
            &layout,
            "app",
            PodLogsOptions {
                container: Some("main".into()),
                timestamps: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert!(out.ends_with(&format!("{TS} boom\n")), "{out}");
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn tail_lines_span_rotated_files_per_container_per_incarnation() {
        let base = tmp("tail");
        let layout = PodLayout::new(&base);
        let lines = |range: std::ops::Range<u32>| -> String {
            range.map(|i| cri(TS, "stdout", &format!("l{i}"))).collect()
        };
        plant(
            &layout,
            "app",
            "20261019T140000Z",
            &[],
            &["main"],
            &[
                ("main", "0.log.1", &lines(0..3)),
                ("main", "0.log", &lines(3..5)),
            ],
        );
        plant(
            &layout,
            "app",
            "20261019T150000Z",
            &[],
            &["main"],
            &[("main", "0.log", &lines(10..20))],
        );

        let out = collect(
            &layout,
            "app",
            PodLogsOptions {
                container: Some("main".into()),
                tail_lines: Some(3),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let body: Vec<&str> = out.lines().filter(|l| l.starts_with('l')).collect();
        assert_eq!(body, ["l2", "l3", "l4", "l17", "l18", "l19"]);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn last_lines_handles_short_files_and_missing_newlines() {
        let base = tmp("last-lines");
        let f = base.join("f");
        std::fs::write(&f, "a\nb\nc\n").unwrap();
        assert_eq!(last_lines(&f, 1).unwrap(), (4, 1));
        assert_eq!(last_lines(&f, 2).unwrap(), (2, 2));
        assert_eq!(last_lines(&f, 5).unwrap(), (0, 3));
        std::fs::write(&f, "a\nb").unwrap();
        assert_eq!(last_lines(&f, 1).unwrap(), (2, 1));
        std::fs::write(&f, "").unwrap();
        assert_eq!(last_lines(&f, 1).unwrap(), (0, 0));
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn tail_zero_writes_only_the_headers() {
        let base = tmp("tail-zero");
        let layout = PodLayout::new(&base);
        plant(
            &layout,
            "app",
            "20261019T140000Z",
            &[],
            &["main"],
            &[("main", "0.log", &cri(TS, "stdout", "x"))],
        );
        let out = collect(
            &layout,
            "app",
            PodLogsOptions {
                tail_lines: Some(0),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(!out.contains("\nx\n"), "{out}");
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn previous_leaves_out_the_newest_incarnation() {
        let base = tmp("previous");
        let layout = PodLayout::new(&base);
        plant(
            &layout,
            "app",
            "20261019T140000Z",
            &[],
            &["main"],
            &[("main", "0.log", &cri(TS, "stdout", "old"))],
        );
        plant(
            &layout,
            "app",
            "20261019T150000Z",
            &[],
            &["main"],
            &[("main", "0.log", &cri(TS, "stdout", "new"))],
        );

        let out = collect(
            &layout,
            "app",
            PodLogsOptions {
                previous: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        assert!(out.contains("old\n") && !out.contains("new\n"), "{out}");
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn an_incarnation_without_logs_still_gets_its_separator() {
        let base = tmp("no-logs");
        let layout = PodLayout::new(&base);
        // Failed in preparation: a store, no log dir.
        plant(&layout, "app", "20261019T140000Z", &[], &["main"], &[]);
        plant(
            &layout,
            "app",
            "20261019T150000Z",
            &[],
            &["main"],
            &[("main", "0.log", &cri(TS, "stdout", "ran"))],
        );

        let out = collect(&layout, "app", PodLogsOptions::default())
            .await
            .unwrap();

        assert!(out.starts_with(&incarnation_header("20261019T140000Z_app")));
        assert!(out.contains(&incarnation_header("20261019T150000Z_app")));
        assert!(out.ends_with("ran\n"));
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn nothing_on_disk_is_an_error_not_an_empty_stream() {
        let base = tmp("errors");
        let layout = PodLayout::new(&base);
        assert_eq!(
            collect(&layout, "ghost", PodLogsOptions::default())
                .await
                .unwrap_err(),
            PodLogsError::NoPod("ghost".into())
        );

        plant(
            &layout,
            "app",
            "20261019T140000Z",
            &[],
            &["main"],
            &[("main", "0.log", &cri(TS, "stdout", "x"))],
        );
        assert_eq!(
            collect(
                &layout,
                "app",
                PodLogsOptions {
                    previous: true,
                    ..Default::default()
                }
            )
            .await
            .unwrap_err(),
            PodLogsError::NoPrevious("app".into())
        );
        assert_eq!(
            collect(
                &layout,
                "app",
                PodLogsOptions {
                    container: Some("nope".into()),
                    ..Default::default()
                }
            )
            .await
            .unwrap_err(),
            PodLogsError::NoContainer {
                pod: "app".into(),
                container: "nope".into()
            }
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    /// Plant one pod's incarnation under a log pod id it does not share its
    /// name with, as a labeled pod is filed.
    fn plant_under(layout: &PodLayout, id: &str, pod: &str, stamp: &str, out: &str) {
        let dir = layout
            .state_dir_at(id, &format!("{stamp}_{pod}"))
            .join(POD_LOG_SUBDIR)
            .join("main");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("0.log"), cri(TS, "stdout", out)).unwrap();
    }

    #[tokio::test]
    async fn pod_selector_reads_one_pod_without_its_separator() {
        let base = tmp("pod-selector");
        let layout = PodLayout::new(&base);
        plant_under(
            &layout,
            "nightly",
            "nightly-1-aaaaa",
            "20261019T000003Z",
            "run-one",
        );
        plant_under(
            &layout,
            "nightly",
            "nightly-2-bbbbb",
            "20261020T000002Z",
            "run-two",
        );

        let all = collect(&layout, "nightly", PodLogsOptions::default())
            .await
            .unwrap();
        assert_eq!(all.matches("POD STARTED").count(), 2, "{all}");

        let one = collect(
            &layout,
            "nightly",
            PodLogsOptions {
                pod: Some("nightly-2-bbbbb".into()),
                container: Some("main".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(one, "run-two\n", "only that pod, no separator");

        let one_all_containers = collect(
            &layout,
            "nightly",
            PodLogsOptions {
                pod: Some("nightly-1-aaaaa".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(
            one_all_containers,
            format!("{}run-one\n", container_header("main")),
            "container headers stay: the caller still needs to tell containers apart"
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn pod_selector_errors() {
        let base = tmp("pod-selector-errors");
        let layout = PodLayout::new(&base);
        plant_under(
            &layout,
            "nightly",
            "nightly-1-aaaaa",
            "20261019T000003Z",
            "x",
        );

        assert_eq!(
            collect(
                &layout,
                "nightly",
                PodLogsOptions {
                    pod: Some("nightly-9-zzzzz".into()),
                    ..Default::default()
                }
            )
            .await
            .unwrap_err(),
            PodLogsError::NoPodUnderId {
                id: "nightly".into(),
                pod: "nightly-9-zzzzz".into()
            }
        );
        assert!(matches!(
            collect(
                &layout,
                "nightly",
                PodLogsOptions {
                    pod: Some("../x".into()),
                    ..Default::default()
                }
            )
            .await,
            Err(PodLogsError::InvalidName {
                kind: "pod name",
                ..
            })
        ));
        let _ = std::fs::remove_dir_all(&base);
    }

    #[tokio::test]
    async fn names_that_could_leave_their_dir_are_refused() {
        let layout = PodLayout::new(&tmp("names"));
        for bad in ["", ".", "..", "../etc", "a/b", "a\\b", "App", "-a", "a_b"] {
            assert!(
                matches!(
                    collect(&layout, bad, PodLogsOptions::default()).await,
                    Err(PodLogsError::InvalidName {
                        kind: "log pod id",
                        ..
                    })
                ),
                "{bad:?}"
            );
            assert!(
                matches!(
                    collect(
                        &layout,
                        "app",
                        PodLogsOptions {
                            container: Some(bad.into()),
                            ..Default::default()
                        }
                    )
                    .await,
                    Err(PodLogsError::InvalidName {
                        kind: "container name",
                        ..
                    })
                ),
                "{bad:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_container_missing_from_the_spec_still_streams() {
        let base = tmp("unlisted");
        let layout = PodLayout::new(&base);
        plant(
            &layout,
            "app",
            "20261019T140000Z",
            &[],
            &["main"],
            &[
                ("main", "0.log", &cri(TS, "stdout", "main")),
                ("extra", "0.log", &cri(TS, "stdout", "extra")),
            ],
        );
        let out = collect(&layout, "app", PodLogsOptions::default())
            .await
            .unwrap();
        let main_at = out.find("main\n").unwrap();
        let extra_at = out.find("extra\n").unwrap();
        assert!(main_at < extra_at, "spec containers first: {out}");
        let _ = std::fs::remove_dir_all(&base);
    }
}
