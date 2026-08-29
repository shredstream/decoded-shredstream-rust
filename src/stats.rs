use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub(crate) struct Stats {
    pub messages: AtomicU64,
    pub bytes: AtomicU64,
    pub datagrams: AtomicU64,
    pub decode_errors: AtomicU64,
    pub skipped_msg_type: AtomicU64,
    pub queue_dropped: AtomicU64,
    pub reconnects: AtomicU64,
    pub last_slot: AtomicU64,
    pub recv_buffer_bytes: AtomicU64,
}

macro_rules! bump {
    ($counter:expr) => {
        $counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    };
    ($counter:expr, $n:expr) => {
        $counter.fetch_add($n, std::sync::atomic::Ordering::Relaxed)
    };
}
pub(crate) use bump;

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        let ld = Ordering::Relaxed;
        StatsSnapshot {
            messages: self.messages.load(ld),
            bytes: self.bytes.load(ld),
            datagrams: self.datagrams.load(ld),
            decode_errors: self.decode_errors.load(ld),
            skipped_msg_type: self.skipped_msg_type.load(ld),
            queue_dropped: self.queue_dropped.load(ld),
            reconnects: self.reconnects.load(ld),
            last_slot: self.last_slot.load(ld),
            recv_buffer_bytes: self.recv_buffer_bytes.load(ld),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StatsSnapshot {
    pub messages: u64,
    pub bytes: u64,
    pub datagrams: u64,
    pub decode_errors: u64,
    pub skipped_msg_type: u64,
    pub queue_dropped: u64,
    pub reconnects: u64,
    pub last_slot: u64,
    pub recv_buffer_bytes: u64,
}
