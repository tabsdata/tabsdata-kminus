// Copyright 2026 Tabsdata Inc.

//! Sending signals to a process by PID, and asking whether one is there.
//!
//! Thin platform wrappers, kept in one place because every supervisor
//! needs the same ones and because each carries a caveat that is easy to
//! get wrong twice. On Unix a negative PID addresses a process *group*.
//! On Windows a process group exists for exactly one purpose — being the
//! addressee of a console control event — and that event can only be
//! raised from inside the target's console, which no long-lived process
//! here shares (every spawn gets its own hidden console). The graceful
//! calls therefore re-exec the current binary as a short-lived helper
//! that attaches to the target's console and raises `CTRL_BREAK` there;
//! see [`ctrl_break_helper_hook`]. The force calls need none of this:
//! `TerminateProcess` per PID, `taskkill /F /T` for a tree.
//!
//! None of these can tell whether the PID is still the one you meant.
//! That is the caller's job, and the reason kill tokens exist: a number is
//! safe to signal only while something still guarantees it is yours.

/// Whether a process with this PID exists — saying nothing about *which*
/// process, so use it only where something else establishes identity.
///
/// On Unix an unreaped zombie answers yes; that reservation is exactly
/// what makes the PID safe to signal until the parent reaps. On Windows
/// this asks the process object (`OpenProcess` + `GetExitCodeProcess`)
/// rather than spawning `tasklist`; an unopenable (elevated) process
/// reads as absent, and an exit code of 259 (`STILL_ACTIVE`) reads as
/// running — the documented wart of this API.
pub fn is_pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None).is_ok()
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
        use windows_sys::Win32::System::Threading::{
            GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
        };
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if handle.is_null() {
                return false;
            }
            let mut code: u32 = 0;
            let ok = GetExitCodeProcess(handle, &mut code) != 0;
            CloseHandle(handle);
            ok && code == STILL_ACTIVE as u32
        }
    }
}

/// Send SIGKILL to a single process. On Windows the handle from
/// `OpenProcess` pins the number until `TerminateProcess`, so this cannot
/// land on a recycled PID.
pub fn force_kill(pid: u32) {
    #[cfg(unix)]
    {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_TERMINATE, TerminateProcess,
        };
        unsafe {
            let handle = OpenProcess(PROCESS_TERMINATE, 0, pid);
            if !handle.is_null() {
                TerminateProcess(handle, 1);
                CloseHandle(handle);
            }
        }
    }
}

/// Ask a single process to shut down: SIGTERM, or on Windows `CTRL_BREAK`
/// to the group `pid` leads — which also asks any descendants still in
/// that group, and asks nobody when `pid` never led one (a stray that is
/// no leader cannot be addressed by a console event; the force stage is
/// what stops it).
pub fn send_sigterm_pid(pid: u32) {
    #[cfg(unix)]
    {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGTERM,
        );
    }
    #[cfg(windows)]
    {
        spawn_ctrl_break_helper(pid);
    }
}

/// Ask a process to shut down: SIGTERM, or on Windows `CTRL_BREAK` to the
/// group it leads.
///
/// Delivery on Windows takes two hops (see [`ctrl_break_helper_hook`]),
/// and lands only on a group leader — which everything spawned by this
/// stack is (`CREATE_NEW_PROCESS_GROUP`). A process launched by hand from
/// a terminal leads no group, receives nothing, and burns its grace
/// before the force stage; its own terminal's Ctrl+C is how it stops
/// gracefully.
pub fn request_graceful(pid: u32) {
    #[cfg(unix)]
    {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGTERM,
        );
    }
    #[cfg(windows)]
    {
        spawn_ctrl_break_helper(pid);
    }
}

/// Send SIGTERM to the entire process group led by `pid` (see
/// [`leads_own_group`]); on Windows, `CTRL_BREAK` to that group — a
/// console event is group-addressed by nature, so this and
/// [`request_graceful`] converge there.
pub fn send_sigterm_group(pid: u32) {
    #[cfg(unix)]
    {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(-(pid as i32)),
            nix::sys::signal::Signal::SIGTERM,
        );
    }
    #[cfg(windows)]
    {
        spawn_ctrl_break_helper(pid);
    }
}

/// argv[1] that turns a re-exec of the current binary into the console
/// event helper (see [`ctrl_break_helper_hook`]).
pub const CTRL_BREAK_HELPER_ARG: &str = "__kcommon-ctrl-break";

/// Turn this invocation into the Windows console-event helper when argv
/// asks for it; return immediately otherwise.
///
/// `GenerateConsoleCtrlEvent` only reaches process groups attached to the
/// *caller's* console, and every process this stack spawns owns a hidden
/// console of its own (`CREATE_NO_WINDOW`), so no supervisor can raise
/// the event directly. The graceful calls instead re-exec the current
/// binary as a short-lived, console-less helper that attaches to the
/// target's console and raises `CTRL_BREAK` there. Every binary that
/// stops processes on Windows must call this first thing in `main`,
/// before argument parsing — without it the re-exec parses the sentinel
/// as ordinary CLI arguments and the graceful request silently asks
/// nobody.
///
/// A no-op on Unix. Cargo-test binaries cannot install it (libtest owns
/// their `main`), so stop paths exercised under `cargo test` on Windows
/// skip straight to the force stage.
pub fn ctrl_break_helper_hook() {
    #[cfg(windows)]
    {
        let mut args = std::env::args();
        if args.nth(1).as_deref() == Some(CTRL_BREAK_HELPER_ARG) {
            let code = match args.next().and_then(|a| a.parse::<u32>().ok()) {
                Some(pid) => ctrl_break_group(pid),
                None => 2,
            };
            std::process::exit(code);
        }
    }
}

/// Attach to `pid`'s console and raise `CTRL_BREAK` for the group `pid`
/// leads. Only runs inside the re-exec'd helper: attaching means first
/// detaching from your own console, which only a process with nothing
/// else to do can afford.
#[cfg(windows)]
fn ctrl_break_group(pid: u32) -> i32 {
    use windows_sys::Win32::System::Console::{
        AttachConsole, CTRL_BREAK_EVENT, FreeConsole, GenerateConsoleCtrlEvent,
        SetConsoleCtrlHandler,
    };
    unsafe {
        // Spawned console-less, but harmless to make sure of it.
        FreeConsole();
        if AttachConsole(pid) == 0 {
            return 3; // target gone, or never had a console
        }
        // The event must not stop the helper itself.
        SetConsoleCtrlHandler(None, 1);
        if GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid) == 0 {
            return 4;
        }
        0
    }
}

/// Run the console-event helper against `pid`'s group. Best-effort like
/// every graceful ask: a failure here means the target burns its grace,
/// not that it survives, so it is logged and swallowed.
#[cfg(windows)]
fn spawn_ctrl_break_helper(pid: u32) {
    use std::os::windows::process::CommandExt;
    use windows_sys::Win32::System::Threading::DETACHED_PROCESS;

    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(e) => {
            tracing::debug!(pid, error = %e, "Cannot locate own binary for Ctrl+Break helper");
            return;
        }
    };
    // DETACHED_PROCESS: born with no console at all, free to attach to
    // the target's.
    match std::process::Command::new(exe)
        .arg(CTRL_BREAK_HELPER_ARG)
        .arg(pid.to_string())
        .creation_flags(DETACHED_PROCESS)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
    {
        Ok(status) if status.success() => {}
        Ok(status) => {
            tracing::debug!(pid, code = ?status.code(), "Ctrl+Break helper could not deliver");
        }
        Err(e) => {
            tracing::debug!(pid, error = %e, "Cannot spawn Ctrl+Break helper");
        }
    }
}

/// Send SIGKILL to the entire process group led by `pid`.
pub fn force_kill_group(pid: u32) {
    #[cfg(unix)]
    {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(-(pid as i32)),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
    #[cfg(windows)]
    {
        let _ = std::process::Command::new("taskkill")
            .args(["/F", "/PID", &pid.to_string(), "/T"])
            .output();
    }
}

/// Whether `pid` leads its own process group — i.e. whether a group
/// signal reaches its descendants, or would spill into whatever else
/// shares the group.
pub fn leads_own_group(pid: u32) -> bool {
    #[cfg(unix)]
    {
        matches!(
            nix::unistd::getpgid(Some(nix::unistd::Pid::from_raw(pid as i32))),
            Ok(pgid) if pgid.as_raw() as u32 == pid,
        )
    }
    #[cfg(windows)]
    {
        // Windows groups exist only as console-event addressees and offer
        // no leadership query; this predicate answers the Unix
        // `kill(-pgid)` question, and the Windows force path walks the
        // tree via `taskkill /F /T` instead.
        let _ = pid;
        false
    }
}

#[cfg(test)]
mod tests;
