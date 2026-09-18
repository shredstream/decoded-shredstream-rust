use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;

use crate::notice::{Notice, NoticeHook};
use crate::stats::{bump, Stats};
use crate::update::TransactionUpdate;

pub const FRAME_MAGIC: u16 = 0x5AE7;
pub const FRAME_VERSION: u8 = 2;
pub const FRAME_HEADER_LEN: usize = 16;
pub const MAX_DATAGRAM: usize = 1408;
pub const MSG_EVENT: u8 = 1;
pub const MSG_TRANSACTION: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    pub version: u8,
    pub msg_type: u8,
    pub flags: u8,
    pub frag_index: u8,
    pub frag_count: u8,
    pub seq: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FrameError {
    TooShort,
    BadMagic,
    UnsupportedVersion,
    Truncated,
    BadFragment,
}

impl FrameError {
    pub fn code(&self) -> &'static str {
        match self {
            FrameError::TooShort => "too_short",
            FrameError::BadMagic => "bad_magic",
            FrameError::UnsupportedVersion => "unsupported_version",
            FrameError::Truncated => "truncated",
            FrameError::BadFragment => "bad_fragment",
        }
    }
}

pub fn parse_header(buf: &[u8]) -> Result<FrameHeader, FrameError> {
    if buf.len() < FRAME_HEADER_LEN {
        return Err(FrameError::TooShort);
    }
    if u16::from_le_bytes([buf[0], buf[1]]) != FRAME_MAGIC {
        return Err(FrameError::BadMagic);
    }
    if buf[2] != FRAME_VERSION {
        return Err(FrameError::UnsupportedVersion);
    }
    Ok(FrameHeader {
        version: buf[2],
        msg_type: buf[3],
        flags: buf[4],
        frag_index: buf[5],
        frag_count: buf[6],
        seq: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
    })
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Push {
    Update(TransactionUpdate),
    Skipped,
    Error(FrameError),
}

#[derive(Default)]
struct Reassembler {
    buf: Vec<u8>,
    frag_count: u8,
    expect_index: u8,
    last_seq: u64,
    active: bool,
}

impl Reassembler {
    fn reset(&mut self) {
        self.buf.clear();
        self.frag_count = 0;
        self.expect_index = 0;
        self.last_seq = 0;
        self.active = false;
    }
}

pub struct StreamDecoder {
    stats: Arc<Stats>,
    hook: NoticeHook,
    reasm: Reassembler,
}

impl Default for StreamDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamDecoder {
    pub fn new() -> Self {
        StreamDecoder {
            stats: Arc::new(Stats::default()),
            hook: NoticeHook::default(),
            reasm: Reassembler::default(),
        }
    }

    pub(crate) fn with(stats: Arc<Stats>, hook: NoticeHook) -> Self {
        StreamDecoder {
            stats,
            hook,
            reasm: Reassembler::default(),
        }
    }

    pub fn stats(&self) -> crate::StatsSnapshot {
        self.stats.snapshot()
    }

    pub fn push(&mut self, datagram: &[u8]) -> Push {
        bump!(self.stats.datagrams);
        bump!(self.stats.bytes, datagram.len() as u64);

        let header = match parse_header(datagram) {
            Ok(h) => h,
            Err(e) => {
                bump!(self.stats.decode_errors);
                self.hook.fire(Notice::DecodeError);
                return Push::Error(e);
            }
        };

        if header.msg_type != MSG_TRANSACTION {
            bump!(self.stats.skipped_msg_type);
            return Push::Skipped;
        }

        let payload = &datagram[FRAME_HEADER_LEN..];

        if header.frag_count == 1 {
            self.reasm.reset();
            return self.complete(payload);
        }

        if header.frag_count == 0 || header.frag_index >= header.frag_count {
            bump!(self.stats.decode_errors);
            self.hook.fire(Notice::DecodeError);
            return Push::Error(FrameError::BadFragment);
        }

        if header.frag_index == 0 {
            self.reasm.reset();
            self.reasm.active = true;
            self.reasm.frag_count = header.frag_count;
            self.reasm.expect_index = 1;
            self.reasm.last_seq = header.seq;
            self.reasm.buf.extend_from_slice(payload);
            return Push::Skipped;
        }

        if !self.reasm.active
            || self.reasm.frag_count != header.frag_count
            || header.frag_index != self.reasm.expect_index
            || header.seq != self.reasm.last_seq.wrapping_add(1)
        {
            self.reasm.reset();
            bump!(self.stats.decode_errors);
            self.hook.fire(Notice::DecodeError);
            return Push::Error(FrameError::BadFragment);
        }
        self.reasm.buf.extend_from_slice(payload);
        self.reasm.last_seq = header.seq;
        self.reasm.expect_index += 1;

        if header.frag_index + 1 == header.frag_count {
            let buf = std::mem::take(&mut self.reasm.buf);
            self.reasm.reset();
            return self.complete(&buf);
        }
        Push::Skipped
    }

    fn complete(&mut self, payload: &[u8]) -> Push {
        if payload.len() < 8 {
            bump!(self.stats.decode_errors);
            self.hook.fire(Notice::DecodeError);
            return Push::Error(FrameError::Truncated);
        }
        let slot = u64::from_le_bytes(payload[0..8].try_into().unwrap());
        let tx = Bytes::copy_from_slice(&payload[8..]);

        bump!(self.stats.messages);
        self.stats
            .last_slot
            .store(slot, std::sync::atomic::Ordering::Relaxed);

        Push::Update(TransactionUpdate::new(
            slot,
            tx,
            Vec::new(),
            Arc::from(Vec::new()),
            None,
            SystemTime::now(),
        ))
    }
}
