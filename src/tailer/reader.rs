use super::{Cursor, CursorStore, FileIdentity, WatcherRegistry, now_millis, should_reset};
use crate::Acknowledgement;
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryMode {
    Explicit,
    Automatic,
}
pub struct Line {
    pub bytes: Vec<u8>,
    pub offset: u64,
    pub generation: u64,
    pub acknowledgement: Acknowledgement,
}
#[derive(Default)]
struct Position {
    offset: u64,
    committed: u64,
    generation: u64,
    key: String,
    prefix: String,
    size: u64,
    identity: FileIdentity,
    initialized: bool,
}
pub struct Tailer {
    path: PathBuf,
    store: Arc<CursorStore>,
    registry: Arc<WatcherRegistry>,
    mode: DeliveryMode,
    position: Arc<Mutex<Position>>,
    read_lock: tokio::sync::Mutex<()>,
    stop: CancellationToken,
}
impl Tailer {
    pub fn new(
        path: impl AsRef<Path>,
        store: Arc<CursorStore>,
        registry: Arc<WatcherRegistry>,
        mode: DeliveryMode,
    ) -> io::Result<Self> {
        Ok(Self {
            path: std::path::absolute(path)?,
            store,
            registry,
            mode,
            position: Arc::new(Mutex::new(Position::default())),
            read_lock: tokio::sync::Mutex::new(()),
            stop: CancellationToken::new(),
        })
    }
    pub fn stop(&self) {
        self.stop.cancel();
    }
    pub fn current_offset(&self) -> u64 {
        self.position.lock().unwrap().offset
    }
    pub fn committed_offset(&self) -> u64 {
        self.position.lock().unwrap().committed
    }
    fn checkpoint(&self, offset: u64) -> (u64, Acknowledgement) {
        let generation = self.position.lock().unwrap().generation;
        let position = self.position.clone();
        let store = self.store.clone();
        let path = self.path.clone();
        (
            generation,
            Acknowledgement::new(move || {
                let mut p = position.lock().unwrap();
                if p.generation != generation || offset <= p.committed {
                    return Ok(());
                }
                store.set(Cursor {
                    key: p.key.clone(),
                    path: path.clone(),
                    offset,
                    size: p.size,
                    inode: p.identity.inode,
                    device: p.identity.device,
                    prefix_hash: p.prefix.clone(),
                    updated_at: now_millis(),
                });
                p.committed = offset;
                Ok(())
            }),
        )
    }
    pub async fn read_available<F, Fut>(
        &self,
        cancel: &CancellationToken,
        mut handler: F,
    ) -> io::Result<()>
    where
        F: FnMut(Line) -> Fut,
        Fut: Future<Output = io::Result<()>>,
    {
        let _read = tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),lock=self.read_lock.lock()=>lock};
        let mut file = File::open(&self.path).await?;
        let metadata = file.metadata().await?;
        let identity = FileIdentity::read(&metadata);
        let key = identity.key(&self.path);
        let prefix = if metadata.len() >= 512 {
            let mut buf = [0; 512];
            match file.read_exact(&mut buf).await {
                Ok(_) => format!("{:x}", Sha256::digest(buf)),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => String::new(),
                Err(e) => return Err(e),
            }
        } else {
            String::new()
        };
        let offset = {
            let mut p = self.position.lock().unwrap();
            let first = !p.initialized;
            if first
                || p.key != key
                || p.offset > metadata.len()
                || (!p.prefix.is_empty() && !prefix.is_empty() && p.prefix != prefix)
            {
                p.generation += 1;
                p.offset = 0;
                p.committed = 0;
                // Load persisted progress only once. Reset generations never resurrect old offsets.
                if first
                    && let Some(c) = self.store.get(&key)
                    && !should_reset(&c, identity.inode, metadata.len(), &prefix)
                {
                    p.offset = c.offset;
                    p.committed = c.offset;
                }
            }
            p.initialized = true;
            p.key = key;
            p.prefix = prefix;
            p.size = metadata.len();
            p.identity = identity;
            p.offset
        };
        file.seek(io::SeekFrom::Start(offset)).await?;
        let mut reader = BufReader::with_capacity(64 * 1024, file);
        let mut offset = offset;
        loop {
            let mut bytes = Vec::new();
            let length = tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),_=self.stop.cancelled()=>return Ok(()),result=reader.read_until(b'\n',&mut bytes)=>result?};
            if length == 0 || bytes.last() != Some(&b'\n') {
                break;
            }
            offset = offset
                .checked_add(length as u64)
                .ok_or_else(|| io::Error::other("transcript offset overflow"))?;
            while matches!(bytes.last(), Some(b'\n' | b'\r')) {
                bytes.pop();
            }
            let (generation, acknowledgement) = self.checkpoint(offset);
            if !bytes.is_empty() {
                handler(Line {
                    bytes,
                    offset,
                    generation,
                    acknowledgement: acknowledgement.clone(),
                })
                .await?;
            }
            self.position.lock().unwrap().offset = offset;
            if self.mode == DeliveryMode::Automatic {
                acknowledgement.acknowledge()?;
            }
        }
        Ok(())
    }
    pub async fn run<F, Fut>(
        self: Arc<Self>,
        cancel: CancellationToken,
        mut handler: F,
    ) -> io::Result<()>
    where
        F: FnMut(Line) -> Fut,
        Fut: Future<Output = io::Result<()>>,
    {
        if cancel.is_cancelled() || self.stop.is_cancelled() {
            return Ok(());
        }
        let mut subscription = self.registry.subscribe(&self.path)?;
        let mut poll = tokio::time::interval(Duration::from_secs(10));
        loop {
            if let Err(error) = self.read_available(&cancel, &mut handler).await {
                tracing::warn!(action="transcript_read_error",path=%self.path.display(),%error);
            }
            tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),_=self.stop.cancelled()=>return Ok(()),changed=subscription.changed()=>{if !changed{return Ok(())}},_=poll.tick()=>{}}
        }
    }
}
