//! Ordered transcript acknowledgements, isolated by file generation.
use crate::Acknowledgement;
use std::{
    collections::VecDeque,
    io,
    sync::{Arc, Mutex},
};

struct Pending {
    id: u64,
    acknowledged: bool,
    commit: Acknowledgement,
}
#[derive(Default)]
struct State {
    generation: Option<u64>,
    sequence: u64,
    pending: VecDeque<Pending>,
}
#[derive(Default)]
pub struct DeliveryQueue {
    state: Mutex<State>,
}
impl DeliveryQueue {
    pub fn add(
        self: &Arc<Self>,
        generation: u64,
        commit: Acknowledgement,
    ) -> io::Result<Acknowledgement> {
        let mut state = self
            .state
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        if state.generation.is_some_and(|current| generation < current) {
            return Ok(Acknowledgement::new(|| Ok(())));
        }
        if state.generation != Some(generation) {
            state.pending.clear();
            state.generation = Some(generation);
        }
        state.sequence = state
            .sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("delivery sequence overflow"))?;
        let id = state.sequence;
        state.pending.push_back(Pending {
            id,
            acknowledged: false,
            commit,
        });
        let queue = self.clone();
        Ok(Acknowledgement::new(move || {
            queue.acknowledge(generation, id)
        }))
    }
    fn acknowledge(&self, generation: u64, id: u64) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|error| io::Error::other(error.to_string()))?;
        if state.generation != Some(generation) {
            return Ok(());
        }
        if let Some(pending) = state.pending.iter_mut().find(|entry| entry.id == id) {
            pending.acknowledged = true;
        }
        while let Some(pending) = state.pending.front().filter(|entry| entry.acknowledged) {
            pending.commit.acknowledge()?;
            state.pending.pop_front();
        }
        Ok(())
    }
}
