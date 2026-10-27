//! Stateful parsing with per-line backpressure and contiguous durable delivery.
use crate::{
    Acknowledgement,
    delivery::DeliveryQueue,
    tailer::Line,
    watcher::{Event, Handler, Harness},
};
use agent_parser::{ClaudeCodeParser, CodexParser};
use std::{io, sync::Arc};
use tokio_util::sync::CancellationToken;

struct Batch {
    offset: u64,
    generation: u64,
    events: Vec<Event>,
    next: usize,
    acknowledgement: Acknowledgement,
}
pub struct TranscriptReader {
    session: Event,
    queue: Arc<DeliveryQueue>,
    batch: Option<Batch>,
    generation: Option<u64>,
    claude: ClaudeCodeParser,
    codex: CodexParser,
}
impl TranscriptReader {
    pub fn new(session: Event) -> Self {
        Self {
            session,
            queue: Arc::new(DeliveryQueue::default()),
            batch: None,
            generation: None,
            claude: ClaudeCodeParser::new(),
            codex: CodexParser::new(),
        }
    }
    pub async fn process(
        &mut self,
        cancel: &CancellationToken,
        line: Line,
        handler: &Handler,
    ) -> io::Result<()> {
        if cancel.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "transcript delivery cancelled",
            ));
        }
        if self
            .generation
            .is_some_and(|generation| line.generation < generation)
        {
            return Ok(());
        }
        if self
            .batch
            .as_ref()
            .is_none_or(|batch| batch.offset != line.offset || batch.generation != line.generation)
        {
            if self.generation != Some(line.generation) {
                self.claude.reset();
                self.codex.reset();
                self.generation = Some(line.generation);
            }
            let acknowledgement = self.queue.add(line.generation, line.acknowledgement)?;
            let parsed = match self.session.harness {
                Harness::ClaudeCode => self.claude.parse_line(&line.bytes),
                Harness::Codex => self.codex.parse_line(&line.bytes),
                Harness::Pi => agent_parser::parse_pi_line(&line.bytes, &self.session.parsed.cwd),
                Harness::Opencode => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "OpenCode events require a database snapshot",
                    ));
                }
            };
            let mut events = Vec::new();
            match parsed {
                Ok(parsed) => {
                    for mut parsed in parsed
                        .into_iter()
                        .filter(|event| event.event_type != "run_started")
                    {
                        if parsed.cwd.is_empty() {
                            parsed.cwd.clone_from(&self.session.parsed.cwd);
                        }
                        let mut event = self.session.clone();
                        event.parsed = parsed;
                        events.push(event);
                    }
                }
                Err(error) => {
                    tracing::warn!(action="transcript_line_parse_failed",harness=%self.session.harness,%error)
                }
            }
            self.batch = Some(Batch {
                offset: line.offset,
                generation: line.generation,
                events,
                next: 0,
                acknowledgement,
            });
        }
        let batch = self.batch.as_mut().expect("batch initialized above");
        if batch.events.is_empty() {
            batch.acknowledgement.acknowledge()?;
        }
        while batch.next < batch.events.len() {
            let acknowledgement =
                (batch.next + 1 == batch.events.len()).then(|| batch.acknowledgement.clone());
            handler(cancel, batch.events[batch.next].clone(), acknowledgement).await?;
            batch.next += 1;
        }
        self.batch = None;
        Ok(())
    }
}
