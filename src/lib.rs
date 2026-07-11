//! kalshi-mm library surface — exposes the trading modules to the binary
//! (src/main.rs) and to the integration tests (tests/parity.rs, which
//! replays golden fixtures generated from the Python implementation).

pub mod api;
pub mod book;
pub mod config;
pub mod engine;
pub mod executor;
pub mod features;
pub mod model;
pub mod paper;
pub mod state;
pub mod transport;
