// Copyright 2026 Tabsdata Inc.

//! Sending signals to a process by PID, and asking whether one is there.
//!
//! Thin platform wrappers, kept in one place because every supervisor
//! needs the same ones and because each carries a caveat that is easy to
//! get wrong twice. On Unix a negative PID addresses a process *group*; on
//! Windows there are no process groups at all, so the group calls fall
//! back to `taskkill /T`, which walks parent links instead.
//!
//! None of these can tell whether the PID is still the one you meant.
//! That is the caller's job, and the reason kill tokens exist: a number is
//! safe to signal only while something still guarantees it is yours.

/// Whether a process with this PID exists.
///
/// Says nothing about *which* process: a recycled number answers yes just
/// as readily as the one the caller had in mind. Use it only where
/// something else already establishes identity — a `Child` handle held
/// open, a matching kill token, a verified image name.
///
/// On Unix an exited child that its parent has not yet reaped is still a
/// zombie, and a zombie answers yes. That is not a flaw to work around:
/// the zombie is what reserves the number, and reserving it is precisely
/// what makes the PID safe to signal until the parent calls `wait`.
///
/// On Windows this asks the process object directly — `OpenProcess` plus
/// `GetExitCodeProcess` — rather than spawning `tasklist`, which cost a
/// full process launch per check and dominated the cost of every kill.
/// Two consequences of asking that way: a process this user lacks rights
/// to open (an elevated one, typically) reads as absent, which is the
/// right answer for a supervisor that could not signal it anyway; and a
/// process that exited *with* code 259 (`STILL_ACTIVE`) reads as running,
/// a documented Windows wart shared by everything that uses this API.
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

/// Send SIGKILL to a single process.
///
/// On Windows this opens the process object and calls `TerminateProcess`
/// on it — no `taskkill` spawn, and between `OpenProcess` succeeding and
/// the terminate call the handle pins the number, so this cannot land on
/// a recycled PID.
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

/// Ask a single process to shut down.
///
/// Unix: SIGTERM. Windows has no SIGTERM; `taskkill` without `/F` is the
/// closest equivalent, and `/T` covers descendants that sanitised any
/// token out of their own environment.
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
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T"])
            .output();
    }
}

/// Ask a console-attached process group to shut down (Windows), or SIGTERM
/// a single process (Unix).
///
/// The Windows form sends `CTRL_BREAK_EVENT` to the process group led by
/// `pid`, which is what a console application's shutdown handler listens
/// for. Known caveat, unresolved: `GenerateConsoleCtrlEvent` only reaches
/// groups attached to the *caller's* console, so a child created with
/// `CREATE_NO_WINDOW` — its own console — never receives it and burns its
/// whole grace period instead. Fixing delivery needs work on a Windows
/// machine; until then this is best-effort and the force stage is what
/// actually stops things there.
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
        use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};
        unsafe {
            GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid);
        }
    }
}

/// Send SIGTERM to the entire process group led by `pid`.
///
/// Only meaningful when `pid` leads its own group — see
/// [`leads_own_group`]. Signaling the group reaches descendants that a
/// single-process signal would miss.
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
        // Windows has no process groups; fall back to a tree walk.
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T"])
            .output();
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

/// Whether `pid` is the leader of its own process group, i.e. the group
/// contains it and its descendants and nothing else.
///
/// Answers whether signalling the group is a proportionate way to reach a
/// process's children, or whether it would spill into whatever else shares
/// that group.
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
        // Windows has no process groups; `force_kill_group` already
        // terminates the tree via `taskkill /T`.
        let _ = pid;
        false
    }
}

#[cfg(test)]
mod tests;
