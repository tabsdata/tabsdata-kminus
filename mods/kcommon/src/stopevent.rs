// Copyright 2026 Tabsdata Inc.

const STOP_EVENT_PREFIX: &str = "Local\\tabsdata-km-stop-";

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fingerprint(folder: &str) -> u64 {
    let mut hash = FNV_OFFSET;
    for byte in folder.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

pub fn stop_event_name(folder: &str) -> String {
    let normalised: String = folder
        .trim_end_matches(['\\', '/'])
        .chars()
        .map(|character| match character {
            '/' => '\\',
            other => other.to_ascii_lowercase(),
        })
        .collect();
    format!("{STOP_EVENT_PREFIX}{:016x}", fingerprint(&normalised))
}

#[cfg(windows)]
pub struct StopEvent(windows_sys::Win32::Foundation::HANDLE);

#[cfg(windows)]
unsafe impl Send for StopEvent {}

#[cfg(windows)]
impl StopEvent {
    pub fn create(folder: &str) -> Option<Self> {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::System::Threading::CreateEventW;

        let name: Vec<u16> = std::ffi::OsStr::new(&stop_event_name(folder))
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        let handle = unsafe { CreateEventW(std::ptr::null(), 1, 0, name.as_ptr()) };
        if handle.is_null() {
            tracing::warn!(
                error = %std::io::Error::last_os_error(),
                "Cannot create the stop event; only console signals will stop this instance"
            );
            return None;
        }
        Some(Self(handle))
    }

    pub fn wait(&self) {
        use windows_sys::Win32::System::Threading::{INFINITE, WaitForSingleObject};
        unsafe {
            WaitForSingleObject(self.0, INFINITE);
        }
    }
}

#[cfg(windows)]
impl Drop for StopEvent {
    fn drop(&mut self) {
        use windows_sys::Win32::Foundation::CloseHandle;
        unsafe {
            CloseHandle(self.0);
        }
    }
}

#[cfg(windows)]
pub fn signal_stop(folder: &str) -> bool {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{EVENT_MODIFY_STATE, OpenEventW, SetEvent};

    let name: Vec<u16> = std::ffi::OsStr::new(&stop_event_name(folder))
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    unsafe {
        let handle = OpenEventW(EVENT_MODIFY_STATE, 0, name.as_ptr());
        if handle.is_null() {
            return false;
        }
        let set = SetEvent(handle) != 0;
        CloseHandle(handle);
        set
    }
}

#[cfg(not(windows))]
pub fn signal_stop(_folder: &str) -> bool {
    false
}

#[cfg(test)]
mod tests;
