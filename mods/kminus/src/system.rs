// Copyright 2026 Tabsdata Inc.

//! Cross-platform system primitives used by `km`: ports, files, PIDs,
//! component heartbeats and the messages components report through.

pub mod file;
pub mod heartbeat;
pub mod messages;
pub mod pid;
pub mod port;
pub mod proc;
