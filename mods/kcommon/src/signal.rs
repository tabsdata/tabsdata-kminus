// Copyright 2026 Tabsdata Inc.

//! Sending signals to a process by PID, and asking whether one is there.
//!
//! Thin platform wrappers, kept in one place because every supervisor
//! needs the same ones and because each carries a caveat that is easy to
//! get wrong twice. On Unix a negative PID addresses a process *group*.
//! On Windows a process group exists for exactly one purpose — being the
//! addressee of a console control event — and that event can only be
//! raised from inside the target's console. km starts each component in a
//! hidden console of its own (see [`crate::console`]), and whatever a
//! component starts shares it in a group of its own, so no stopper shares
//! its target's console. The graceful calls therefore re-exec the current
//! binary as a short-lived helper that attaches to the target's console
//! and raises `CTRL_BREAK` there; see [`ctrl_break_helper_hook`]. A target
//! with no console at all (`CREATE_NO_WINDOW`) cannot be reached and stops
//! by the force stage. The force calls need none of this:
//! `TerminateProcess` per PID, `taskkill /F /T` for a tree.
//!
//! None of these can tell whether the PID is still the one you meant.
//! That is the caller's job, and the reason kill tokens exist: a number is
//! safe to signal only while something still guarantees it is yours.

/// Whether a process with this PID exists — saying nothing about *which*
/// process, so use it only where something else establishes identity.
///
/// On Unix an unreaped zombie answers yes; that reservation is exactly
/// what makes the PID safe to signal until the parent reaps. A process
/// this one may not signal (`EPERM`) exists all the same, and answers yes.
/// On Windows this asks the process object (`OpenProcess` +
/// `GetExitCodeProcess`) rather than spawning `tasklist`; a process that
/// refuses to be opened (elevated, `ERROR_ACCESS_DENIED`) exists and
/// answers yes, and an exit code of 259 (`STILL_ACTIVE`) reads as running
/// — the documented wart of this API.
pub fn is_pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        matches!(
            nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), None),
            Ok(()) | Err(nix::errno::Errno::EPERM)
        )
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{
            CloseHandle, ERROR_ACCESS_DENIED, GetLastError, STILL_ACTIVE,
        };
        use windows_sys::Win32::System::Threading::{
            GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
        };
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if handle.is_null() {
                return GetLastError() == ERROR_ACCESS_DENIED;
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
/// that group.
///
/// On Windows `pid` must lead a process group. Windows has no way to ask
/// whether it does, and a console event aimed at a process that leads
/// none goes to every process attached to that console, the caller's
/// own tree included.
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
/// and `pid` must lead a process group, as everything this stack spawns
/// does (`CREATE_NEW_PROCESS_GROUP`): aimed at a process that leads none,
/// such as one launched by hand from a terminal, the event goes to every
/// process attached to its console (see [`send_sigterm_pid`]).
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
/// event helper (see [`ctrl_break_helper_hook`]). It is also a valid test
/// name, which is how a test binary hosts the helper.
pub const CTRL_BREAK_HELPER_ARG: &str = "kcommon_ctrl_break_helper";

/// Variable carrying the PID whose group the helper raises `CTRL_BREAK` for.
/// Not an argument: a test binary would read it as a test-name filter.
pub const CTRL_BREAK_PID_VAR: &str = "KCOMMON_CTRL_BREAK_PID";

/// Turn this invocation into the Windows console-event helper when argv
/// asks for it; return immediately otherwise.
///
/// `GenerateConsoleCtrlEvent` only reaches process groups attached to the
/// *caller's* console, and every component of this stack runs in a hidden
/// console of its own (see [`crate::console`]), so no supervisor can raise
/// the event directly. The graceful calls instead re-exec the current
/// binary as a short-lived, console-less helper that attaches to the
/// target's console and raises `CTRL_BREAK` there. Every binary that
/// stops processes on Windows must call this first thing in `main`,
/// before argument parsing — without it the re-exec parses the sentinel
/// as ordinary CLI arguments and the graceful request silently asks
/// nobody.
///
/// A test binary, whose `main` libtest owns, hosts the helper with a test
/// of its own named [`CTRL_BREAK_HELPER_ARG`] that calls this: the re-exec
/// selects that test by name. Without one, its stops skip straight to the
/// force stage. A no-op on Unix.
pub fn ctrl_break_helper_hook() {
    #[cfg(windows)]
    {
        if std::env::args().nth(1).as_deref() == Some(CTRL_BREAK_HELPER_ARG) {
            let code = match std::env::var(CTRL_BREAK_PID_VAR)
                .ok()
                .and_then(|pid| pid.parse::<u32>().ok())
            {
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
        .env(CTRL_BREAK_PID_VAR, pid.to_string())
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
