use std::fmt;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

use bytes::Bytes;

use crate::message::{decode_message, message_bytes, CompiledMessage, MessageDecodeError};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Signature(pub [u8; 64]);

impl Signature {
    pub fn as_bytes(&self) -> &[u8; 64] {
        &self.0
    }
    pub fn to_base58(&self) -> String {
        bs58::encode(self.0).into_string()
    }
}

impl fmt::Display for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_base58())
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Signature({})", self.to_base58())
    }
}

fn decode_shortvec(buf: &[u8]) -> Option<(usize, usize)> {
    let mut value: usize = 0;
    for (i, &byte) in buf.iter().take(3).enumerate() {
        value |= ((byte & 0x7F) as usize) << (7 * i);
        if byte & 0x80 == 0 {
            if i == 2 && byte > 0x03 {
                return None;
            }
            return Some((value, i + 1));
        }
    }
    None
}

fn derive_signatures(bytes: &[u8]) -> Vec<Signature> {
    let Some((count, prefix)) = decode_shortvec(bytes) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(count.min(64));
    for i in 0..count {
        let start = prefix + i * 64;
        let Some(chunk) = bytes.get(start..start + 64) else {
            break;
        };
        out.push(Signature(chunk.try_into().expect("64-byte slice")));
    }
    out
}

pub struct TransactionUpdate {
    pub(crate) slot: u64,
    pub(crate) bytes: Bytes,
    pub(crate) provided_signatures: Vec<Bytes>,
    pub(crate) filters: Arc<[String]>,
    pub(crate) created_at: Option<SystemTime>,
    pub(crate) received_at: SystemTime,
    pub(crate) signatures: OnceLock<Vec<Signature>>,
}

impl TransactionUpdate {
    pub(crate) fn new(
        slot: u64,
        bytes: Bytes,
        provided_signatures: Vec<Bytes>,
        filters: Arc<[String]>,
        created_at: Option<SystemTime>,
        received_at: SystemTime,
    ) -> Self {
        TransactionUpdate {
            slot,
            bytes,
            provided_signatures,
            filters,
            created_at,
            received_at,
            signatures: OnceLock::new(),
        }
    }

    pub fn slot(&self) -> u64 {
        self.slot
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn signature(&self) -> Signature {
        let mut s = [0u8; 64];
        if let Some(view) = self.bytes.get(1..65) {
            s.copy_from_slice(view);
        }
        Signature(s)
    }

    pub fn signatures(&self) -> &[Signature] {
        self.signatures.get_or_init(|| {
            if !self.provided_signatures.is_empty() {
                let provided: Option<Vec<Signature>> = self
                    .provided_signatures
                    .iter()
                    .map(|b| <&[u8; 64]>::try_from(&b[..]).ok().map(|s| Signature(*s)))
                    .collect();
                if let Some(v) = provided {
                    return v;
                }
            }
            derive_signatures(&self.bytes)
        })
    }

    pub fn filters(&self) -> &[String] {
        &self.filters
    }

    pub fn created_at(&self) -> Option<SystemTime> {
        self.created_at
    }

    pub fn received_at(&self) -> SystemTime {
        self.received_at
    }

    pub fn parse(&self) -> Result<CompiledMessage<'_>, MessageDecodeError> {
        decode_message(message_bytes(&self.bytes)?)
    }
}

impl fmt::Debug for TransactionUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransactionUpdate")
            .field("slot", &self.slot)
            .field("signature", &self.signature())
            .field("bytes", &self.bytes.len())
            .field("filters", &self.filters)
            .finish_non_exhaustive()
    }
}
