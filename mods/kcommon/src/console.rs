// Copyright 2026 Tabsdata Inc.

//! Starting a Windows child in a hidden console of its own.
//!
//! A graceful stop on Windows is a `CTRL_BREAK` raised inside the target's
//! console (see [`crate::signal`]). A child spawned with `CREATE_NO_WINDOW`
//! has no console, so nothing can raise one for it, and every stop ran out
//! its grace before `TerminateProcess`. A child sharing its launcher's
//! console has the opposite problem: closing the terminal km was started
//! from ends it. A console of its own, created hidden, is neither: no
//! terminal reaches it, and the helper can attach to it.
//!
//! `std::process::Command` cannot ask for a hidden window on stable Rust
//! (`CommandExt::show_window` is unstable), so this calls `CreateProcessW`
//! itself. It reads the program, arguments, environment changes and
//! working directory back from the `Command`; standard input and output go
//! to `NUL` and standard error to the given file, which are the only
//! handles the child inherits. Whatever the child starts without console
//! flags of its own shares this console, so each of its process groups is
//! reachable by the same helper.

#[cfg(any(windows, test))]
const QUOTE: u16 = b'"' as u16;
#[cfg(any(windows, test))]
const BACKSLASH: u16 = b'\\' as u16;

/// Append `arg` to a command line so `CommandLineToArgvW` (and the MSVC
/// runtime) reads it back unchanged.
#[cfg(any(windows, test))]
fn push_arg(line: &mut Vec<u16>, arg: &[u16]) {
    if !line.is_empty() {
        line.push(b' ' as u16);
    }
    let plain = !arg.is_empty()
        && !arg
            .iter()
            .any(|&unit| matches!(unit, 0x20 | 0x09 | 0x0a | 0x0b | QUOTE));
    if plain {
        line.extend_from_slice(arg);
        return;
    }
    line.push(QUOTE);
    let mut backslashes = 0usize;
    for &unit in arg {
        if unit == BACKSLASH {
            backslashes += 1;
        } else {
            if unit == QUOTE {
                line.extend(std::iter::repeat_n(BACKSLASH, backslashes + 1));
            }
            backslashes = 0;
        }
        line.push(unit);
    }
    line.extend(std::iter::repeat_n(BACKSLASH, backslashes));
    line.push(QUOTE);
}

/// Windows compares environment names without regard to case.
#[cfg(any(windows, test))]
fn env_key(name: &[u16]) -> Vec<u16> {
    name.iter()
        .map(|&unit| match u8::try_from(unit) {
            Ok(byte) => u16::from(byte.to_ascii_uppercase()),
            Err(_) => unit,
        })
        .collect()
}

/// A `CREATE_UNICODE_ENVIRONMENT` block: `inherited` with `changes`
/// applied (`None` removes a variable), sorted by name as Windows expects.
#[cfg(any(windows, test))]
fn env_block(
    inherited: impl IntoIterator<Item = (Vec<u16>, Vec<u16>)>,
    changes: impl IntoIterator<Item = (Vec<u16>, Option<Vec<u16>>)>,
) -> Vec<u16> {
    let mut vars: std::collections::BTreeMap<Vec<u16>, (Vec<u16>, Vec<u16>)> = inherited
        .into_iter()
        .map(|(name, value)| (env_key(&name), (name, value)))
        .collect();
    for (name, value) in changes {
        match value {
            Some(value) => {
                vars.insert(env_key(&name), (name, value));
            }
            None => {
                vars.remove(&env_key(&name));
            }
        }
    }
    let mut block = Vec::new();
    for (name, value) in vars.into_values() {
        block.extend(name);
        block.push(b'=' as u16);
        block.extend(value);
        block.push(0);
    }
    if block.is_empty() {
        block.push(0);
    }
    block.push(0);
    block
}

#[cfg(windows)]
pub use windows::OwnConsoleChild;

#[cfg(windows)]
mod windows {
    use std::ffi::OsStr;
    use std::fs::File;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::AsRawHandle;
    use std::os::windows::process::ExitStatusExt;
    use std::path::Path;
    use std::process::{Command, ExitStatus};
    use std::{io, mem, ptr};

    use windows_sys::Win32::Foundation::{
        CloseHandle, DUPLICATE_SAME_ACCESS, DuplicateHandle, HANDLE, WAIT_OBJECT_0,
    };
    use windows_sys::Win32::System::Threading::{
        CREATE_NEW_CONSOLE, CREATE_NEW_PROCESS_GROUP, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
        DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT, GetCurrentProcess,
        GetExitCodeProcess, InitializeProcThreadAttributeList, LPPROC_THREAD_ATTRIBUTE_LIST,
        PROC_THREAD_ATTRIBUTE_HANDLE_LIST, PROCESS_INFORMATION, STARTF_USESHOWWINDOW,
        STARTF_USESTDHANDLES, STARTUPINFOEXW, TerminateProcess, UpdateProcThreadAttribute,
        WaitForSingleObject,
    };
    use windows_sys::Win32::UI::WindowsAndMessaging::SW_HIDE;

    use super::{env_block, push_arg};

    fn wide(text: &OsStr) -> Vec<u16> {
        text.encode_wide().collect()
    }

    fn wide_z(text: &OsStr) -> Vec<u16> {
        text.encode_wide().chain(std::iter::once(0)).collect()
    }

    /// A handle this module owns and closes.
    struct Owned(HANDLE);

    // SAFETY: a kernel handle is valid from any thread of the process that
    // holds it, and `Owned` gives no shared access to it.
    unsafe impl Send for Owned {}

    impl Drop for Owned {
        fn drop(&mut self) {
            // SAFETY: the handle came from a successful Win32 call and is
            // closed exactly once.
            unsafe {
                CloseHandle(self.0);
            }
        }
    }

    /// An inheritable copy of `file`'s handle, for the child's standard
    /// streams.
    fn inheritable(file: &File) -> io::Result<Owned> {
        let mut copy: HANDLE = ptr::null_mut();
        // SAFETY: both process handles are the current-process pseudo
        // handle, the source is a live handle borrowed from `file`, and
        // `copy` receives a new handle that `Owned` then closes.
        let ok = unsafe {
            DuplicateHandle(
                GetCurrentProcess(),
                file.as_raw_handle() as HANDLE,
                GetCurrentProcess(),
                &mut copy,
                0,
                1,
                DUPLICATE_SAME_ACCESS,
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Owned(copy))
    }

    /// The attribute list that limits inheritance to exactly `handles`.
    struct HandleList {
        buffer: Vec<u8>,
    }

    impl HandleList {
        fn new(handles: &[HANDLE]) -> io::Result<Self> {
            let mut size = 0usize;
            // SAFETY: a null list with a size pointer only reports the size
            // required; failing with ERROR_INSUFFICIENT_BUFFER is expected.
            unsafe {
                InitializeProcThreadAttributeList(ptr::null_mut(), 1, 0, &mut size);
            }
            let mut list = Self {
                buffer: vec![0u8; size],
            };
            // SAFETY: `buffer` is `size` bytes, as the call above asked for.
            if unsafe { InitializeProcThreadAttributeList(list.raw(), 1, 0, &mut size) } == 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: `handles` outlives the list's use, which ends when
            // `CreateProcessW` returns in `spawn`.
            let updated = unsafe {
                UpdateProcThreadAttribute(
                    list.raw(),
                    0,
                    PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                    handles.as_ptr().cast(),
                    mem::size_of_val(handles),
                    ptr::null_mut(),
                    ptr::null(),
                )
            };
            if updated == 0 {
                let error = io::Error::last_os_error();
                // SAFETY: the list was initialised above.
                unsafe { DeleteProcThreadAttributeList(list.raw()) };
                list.buffer.clear();
                return Err(error);
            }
            Ok(list)
        }

        fn raw(&mut self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
            self.buffer.as_mut_ptr().cast()
        }
    }

    impl Drop for HandleList {
        fn drop(&mut self) {
            if !self.buffer.is_empty() {
                // SAFETY: a non-empty buffer holds an initialised list.
                unsafe { DeleteProcThreadAttributeList(self.raw()) };
            }
        }
    }

    /// A child running in a hidden console of its own, leading a new
    /// process group.
    pub struct OwnConsoleChild {
        process: Owned,
        pid: u32,
        exited: Option<ExitStatus>,
    }

    impl OwnConsoleChild {
        /// Start `cmd` with standard input and output on `NUL` and standard
        /// error on `stderr` (or `NUL`). The child inherits the caller's
        /// environment with `cmd`'s changes applied; an `env_clear` on
        /// `cmd` is not honoured, as `Command` does not expose it.
        pub fn spawn(cmd: &Command, stderr: Option<&File>) -> io::Result<Self> {
            let nul_in = File::open("NUL")?;
            let nul_out = File::options().write(true).open("NUL")?;
            let stdin = inheritable(&nul_in)?;
            let stdout = inheritable(&nul_out)?;
            let stderr = inheritable(stderr.unwrap_or(&nul_out))?;
            let handles = [stdin.0, stdout.0, stderr.0];
            let mut attributes = HandleList::new(&handles)?;

            let program = cmd.get_program();
            let application = Path::new(program)
                .parent()
                .is_some_and(|parent| !parent.as_os_str().is_empty())
                .then(|| wide_z(program));
            let mut line = Vec::new();
            push_arg(&mut line, &wide(program));
            for arg in cmd.get_args() {
                push_arg(&mut line, &wide(arg));
            }
            line.push(0);

            let environment = env_block(
                std::env::vars_os().map(|(name, value)| (wide(&name), wide(&value))),
                cmd.get_envs()
                    .map(|(name, value)| (wide(name), value.map(wide))),
            );
            let directory = cmd.get_current_dir().map(|dir| wide_z(dir.as_os_str()));

            // SAFETY: zeroed is the documented initial state of both
            // structures; every field this call relies on is set below.
            let mut startup: STARTUPINFOEXW = unsafe { mem::zeroed() };
            startup.StartupInfo.cb = mem::size_of::<STARTUPINFOEXW>() as u32;
            startup.StartupInfo.dwFlags = STARTF_USESHOWWINDOW | STARTF_USESTDHANDLES;
            startup.StartupInfo.wShowWindow = SW_HIDE as u16;
            startup.StartupInfo.hStdInput = stdin.0;
            startup.StartupInfo.hStdOutput = stdout.0;
            startup.StartupInfo.hStdError = stderr.0;
            startup.lpAttributeList = attributes.raw();
            let mut info: PROCESS_INFORMATION = unsafe { mem::zeroed() };

            // SAFETY: every pointer refers to a NUL-terminated buffer or a
            // structure that lives until the call returns; the command line
            // is a mutable buffer, as CreateProcessW requires.
            let created = unsafe {
                CreateProcessW(
                    application
                        .as_ref()
                        .map_or(ptr::null(), |name| name.as_ptr()),
                    line.as_mut_ptr(),
                    ptr::null(),
                    ptr::null(),
                    1,
                    CREATE_NEW_CONSOLE
                        | CREATE_NEW_PROCESS_GROUP
                        | CREATE_UNICODE_ENVIRONMENT
                        | EXTENDED_STARTUPINFO_PRESENT,
                    environment.as_ptr().cast(),
                    directory.as_ref().map_or(ptr::null(), |dir| dir.as_ptr()),
                    &startup.StartupInfo,
                    &mut info,
                )
            };
            if created == 0 {
                return Err(io::Error::last_os_error());
            }
            drop(Owned(info.hThread));
            Ok(Self {
                process: Owned(info.hProcess),
                pid: info.dwProcessId,
                exited: None,
            })
        }

        pub fn id(&self) -> u32 {
            self.pid
        }

        /// The exit status once the child has exited, without waiting.
        pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
            if self.exited.is_some() {
                return Ok(self.exited);
            }
            // SAFETY: the process handle is live while `self` is.
            if unsafe { WaitForSingleObject(self.process.0, 0) } != WAIT_OBJECT_0 {
                return Ok(None);
            }
            let mut code = 0u32;
            // SAFETY: as above; `code` receives the exit code.
            if unsafe { GetExitCodeProcess(self.process.0, &mut code) } == 0 {
                return Err(io::Error::last_os_error());
            }
            self.exited = Some(ExitStatus::from_raw(code));
            Ok(self.exited)
        }

        /// End the child at once, as `Child::kill` does.
        pub fn kill(&mut self) -> io::Result<()> {
            if self.try_wait()?.is_some() {
                return Ok(());
            }
            // SAFETY: the handle is live; the exit code matches std's kill.
            if unsafe { TerminateProcess(self.process.0, 1) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests;
