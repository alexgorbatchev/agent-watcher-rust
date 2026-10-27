#![doc = include_str!("../README.md")]
pub mod checkpoint;
pub mod delivery;
pub mod discovery;
pub mod opencode;
mod persistence;
pub mod scanner;
pub mod tailer;
pub mod transcript;
pub mod watcher;

pub use watcher::{Config, Event, Handler, HandlerFuture, Harness, run};

use std::{io, sync::Arc};
/// Confirms durable delivery. Clones refer to the same idempotent checkpoint.
#[derive(Clone)]
pub struct Acknowledgement(Arc<dyn Fn() -> io::Result<()> + Send + Sync>);
impl Acknowledgement {
    pub fn new(callback: impl Fn() -> io::Result<()> + Send + Sync + 'static) -> Self {
        Self(Arc::new(callback))
    }
    pub fn acknowledge(&self) -> io::Result<()> {
        (self.0)()
    }
}
