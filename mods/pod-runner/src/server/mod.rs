// Copyright 2026 Tabsdata Inc.

//! HTTP server — Axum-based REST API for pod management.
//!
//! Exposes the [`PodLogic`](crate::logic::PodLogic) operations as HTTP
//! endpoints. Use [`create_router`] to build the Axum router, then
//! serve it with `axum::serve`.
//!
//! # Endpoints
//!
//! | Method | Path            | Description                     |
//! |--------|-----------------|---------------------------------|
//! | POST   | `/createPod`    | Submit and start a pod          |
//! | DELETE | `/deletePod`    | Delete a pod (by name)          |
//! | GET    | `/getPod`       | Get pod spec + status           |
//! | GET    | `/getPodStatus` | Get pod status only             |
//! | GET    | `/getPods`      | List all pods                   |
//! | GET    | `/getContainerLogs` | Get container log contents  |
//! | GET    | `/capacity`     | Node capacity (cpu/mem/pods)    |
//! | GET    | `/nodeConditions` | Node health conditions        |
//! | GET    | `/nodeAddresses` | Node network addresses         |

pub mod app;
pub mod config;
pub mod heartbeat;
pub mod messages;
pub mod tls;
