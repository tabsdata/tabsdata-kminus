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

/// Ask a single process to shut down: SIGTERM, or on Windows `taskkill`
/// without `/F` (with `/T` for descendants that dropped their token).
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

/// Ask a process to shut down: SIGTERM, or `CTRL_BREAK_EVENT` to its
/// group on Windows.
///
/// Known caveat, unresolved: `GenerateConsoleCtrlEvent` only reaches
/// groups on the *caller's* console, so a `CREATE_NO_WINDOW` child never
/// receives it and burns its grace. Fixing delivery needs a Windows
/// machine; until then the force stage is what actually stops things
/// there.
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

/// Send SIGTERM to the entire process group led by `pid` (see
/// [`leads_own_group`]).
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
        // Windows has no process groups; `force_kill_group` already
        // terminates the tree via `taskkill /T`.
        let _ = pid;
        false
    }
}

#[cfg(test)]
mod tests;
