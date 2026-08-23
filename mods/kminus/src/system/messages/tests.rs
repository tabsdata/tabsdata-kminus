// Copyright 2026 Tabsdata Inc.

use super::*;
use std::env;

fn tempdir() -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = env::temp_dir().join(format!("km-msg-{}-{}", std::process::id(), n));
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// The path a component writes for one phase. km only ever reads the
/// directory, so the naming rule lives here beside the tests that pin it and
/// in `read`'s filter, which is the code that actually depends on it.
fn message_file(workdir: &Path, phase: Phase, binary: &str) -> PathBuf {
    messages_dir(workdir).join(format!("{}{}.json", phase.prefix(), binary))
}

fn write<S: AsRef<str>>(workdir: &Path, phase: Phase, binary: &str, lines: &[S]) {
    let path = message_file(workdir, phase, binary);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let body: Vec<&str> = lines.iter().map(|l| l.as_ref()).collect();
    fs::write(&path, format!("{}\n", body.join("\n"))).unwrap();
}

fn line(time: &str, binary: &str, level: &str, message: &str) -> String {
    format!(r#"{{"time":"{time}","binary":"{binary}","level":"{level}","message":"{message}"}}"#)
}

// -- paths -------------------------------------------------------------------

#[test]
fn phase_prefixes_keep_the_two_sets_apart() {
    assert_eq!(Phase::Start.prefix(), "start-");
    assert_eq!(Phase::Stop.prefix(), "stop-");
}

#[test]
fn message_file_is_named_for_phase_and_binary() {
    let p = message_file(Path::new("/wd"), Phase::Start, "control-plane");
    assert_eq!(p, Path::new("/wd/messages/start-control-plane.json"));
    let p = message_file(Path::new("/wd"), Phase::Stop, "pod-runner");
    assert_eq!(p, Path::new("/wd/messages/stop-pod-runner.json"));
}

// -- clear -------------------------------------------------------------------

// A previous run's messages must never be read as this run's.
#[test]
fn clear_removes_a_previous_runs_messages_and_leaves_the_dir() {
    let dir = tempdir();
    write(
        &dir,
        Phase::Start,
        "pod-runner",
        &[line("t1", "pod-runner", "INFO", "old")],
    );
    assert_eq!(read(&dir, Phase::Start).len(), 1);

    clear(&dir);

    assert!(read(&dir, Phase::Start).is_empty());
    assert!(
        messages_dir(&dir).is_dir(),
        "components need the dir to exist"
    );
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn clear_works_when_there_is_nothing_to_clear() {
    let dir = tempdir();
    clear(&dir);
    assert!(messages_dir(&dir).is_dir());
    fs::remove_dir_all(&dir).ok();
}

// -- read --------------------------------------------------------------------

#[test]
fn read_is_empty_without_a_messages_dir() {
    let dir = tempdir();
    assert!(read(&dir, Phase::Start).is_empty());
    fs::remove_dir_all(&dir).ok();
}

// The whole point of the timestamp: components write independently, and the
// operator should see one ordered narrative rather than per-file blocks.
#[test]
fn read_orders_across_components_by_time() {
    let dir = tempdir();
    write(
        &dir,
        Phase::Start,
        "control-plane",
        &[
            line(
                "2026-08-22T10:00:03.000Z",
                "control-plane",
                "INFO",
                "cp-late",
            ),
            line(
                "2026-08-22T10:00:01.000Z",
                "control-plane",
                "INFO",
                "cp-early",
            ),
        ],
    );
    write(
        &dir,
        Phase::Start,
        "pod-runner",
        &[line(
            "2026-08-22T10:00:02.000Z",
            "pod-runner",
            "INFO",
            "pr-middle",
        )],
    );

    let got: Vec<String> = read(&dir, Phase::Start)
        .into_iter()
        .map(|m| m.message)
        .collect();
    assert_eq!(got, vec!["cp-early", "pr-middle", "cp-late"]);
    fs::remove_dir_all(&dir).ok();
}

// Same instant, so the binary breaks the tie rather than directory order.
#[test]
fn read_breaks_time_ties_by_binary() {
    let dir = tempdir();
    let t = "2026-08-22T10:00:00.000Z";
    write(
        &dir,
        Phase::Start,
        "pod-runner",
        &[line(t, "pod-runner", "INFO", "b")],
    );
    write(
        &dir,
        Phase::Start,
        "control-plane",
        &[line(t, "control-plane", "INFO", "a")],
    );

    let got: Vec<String> = read(&dir, Phase::Start)
        .into_iter()
        .map(|m| m.binary)
        .collect();
    assert_eq!(got, vec!["control-plane", "pod-runner"]);
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn read_keeps_the_phases_separate() {
    let dir = tempdir();
    write(
        &dir,
        Phase::Start,
        "pod-runner",
        &[line("t1", "pod-runner", "INFO", "starting")],
    );
    write(
        &dir,
        Phase::Stop,
        "pod-runner",
        &[line("t2", "pod-runner", "INFO", "stopping")],
    );

    let start: Vec<String> = read(&dir, Phase::Start)
        .into_iter()
        .map(|m| m.message)
        .collect();
    let stop: Vec<String> = read(&dir, Phase::Stop)
        .into_iter()
        .map(|m| m.message)
        .collect();
    assert_eq!(start, vec!["starting"]);
    assert_eq!(stop, vec!["stopping"]);
    fs::remove_dir_all(&dir).ok();
}

// A component that writes rubbish loses its own line and nobody else's.
#[test]
fn read_skips_unparseable_lines_and_keeps_the_rest() {
    let dir = tempdir();
    write(
        &dir,
        Phase::Start,
        "pod-runner",
        &[
            line("t1", "pod-runner", "INFO", "good"),
            "this is not json".to_string(),
            String::new(),
            line("t2", "pod-runner", "WARN", "also good"),
        ],
    );

    let got: Vec<String> = read(&dir, Phase::Start)
        .into_iter()
        .map(|m| m.message)
        .collect();
    assert_eq!(got, vec!["good", "also good"]);
    fs::remove_dir_all(&dir).ok();
}

#[test]
fn read_ignores_files_that_are_not_message_files() {
    let dir = tempdir();
    let msgs = messages_dir(&dir);
    fs::create_dir_all(&msgs).unwrap();
    fs::write(msgs.join("notes.txt"), line("t1", "x", "INFO", "ignored")).unwrap();
    fs::write(
        msgs.join("start-pod-runner.json.tmp"),
        line("t1", "x", "INFO", "partial"),
    )
    .unwrap();

    assert!(read(&dir, Phase::Start).is_empty());
    fs::remove_dir_all(&dir).ok();
}

// Missing fields must not discard the line: a message with text is still
// worth showing even if the writer left the level out.
#[test]
fn read_defaults_missing_fields() {
    let dir = tempdir();
    write(
        &dir,
        Phase::Start,
        "pod-runner",
        &[r#"{"message":"bare"}"#.to_string()],
    );

    let got = read(&dir, Phase::Start);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].message, "bare");
    assert_eq!(got[0].level, "");
    assert_eq!(got[0].binary, "");
    fs::remove_dir_all(&dir).ok();
}

// -- rendering ---------------------------------------------------------------

#[test]
fn level_defaults_to_info_when_the_writer_omits_it() {
    let m = Message {
        message: "hello".into(),
        ..Default::default()
    };
    assert_eq!(level_of(&m), LEVEL_INFO);
}

#[test]
fn level_is_passed_through_when_set() {
    for level in [LEVEL_INFO, LEVEL_WARN, LEVEL_ERROR, "SOMETHING"] {
        let m = Message {
            level: level.into(),
            ..Default::default()
        };
        assert_eq!(level_of(&m), level);
    }
}

#[test]
fn format_line_shows_level_binary_and_message() {
    let m = Message {
        time: "2026-08-22T10:00:00.000Z".into(),
        binary: "pod-runner".into(),
        level: LEVEL_WARN.into(),
        message: "capacity.pods is 8".into(),
    };
    let line = format_line(&m);
    assert!(line.contains(LEVEL_WARN), "{line}");
    assert!(line.contains("pod-runner"), "{line}");
    assert!(line.contains("capacity.pods is 8"), "{line}");
    // The timestamp only orders the lines; it is not shown.
    assert!(!line.contains("2026-08-22"), "{line}");
}
