// Copyright 2026 Tabsdata Inc.

//! Cross-platform memory usage.

/// Process memory usage.
pub struct MemoryUsage {
    /// Resident set size (physical memory) in bytes.
    pub physical_bytes: u64,
    /// Virtual memory size in bytes.
    pub virtual_bytes: u64,
}

/// Get the current process memory usage.
pub fn get_memory_usage() -> Option<MemoryUsage> {
    let stats = memory_stats::memory_stats()?;
    Some(MemoryUsage {
        physical_bytes: stats.physical_mem as u64,
        virtual_bytes: stats.virtual_mem as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_usage_returns_nonzero() {
        let mem = get_memory_usage().expect("should return memory stats");
        assert!(mem.physical_bytes > 0, "RSS should be > 0");
        assert!(mem.virtual_bytes > 0, "VSZ should be > 0");
    }
}
