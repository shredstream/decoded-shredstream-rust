use core::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompiledInstruction<'a> {
    pub program_index: u8,
    pub account_indices: &'a [u8],
    pub data: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddressTableLookup<'a> {
    pub table_address: &'a [u8],
    pub writable_indexes: &'a [u8],
    pub readonly_indexes: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed: u8,
    pub num_readonly_unsigned: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledMessage<'a> {
    pub version: Option<u8>,
    pub header: MessageHeader,
    pub static_accounts: Vec<&'a [u8]>,
    pub lifetime_token: &'a [u8],
    pub instructions: Vec<CompiledInstruction<'a>>,
    pub address_table_lookups: Vec<AddressTableLookup<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageDecodeError(pub &'static str);

impl fmt::Display for MessageDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "malformed transaction message: {}", self.0)
    }
}

impl std::error::Error for MessageDecodeError {}

type Result<T> = core::result::Result<T, MessageDecodeError>;

fn shortvec(buf: &[u8], offset: usize, what: &'static str) -> Result<(usize, usize)> {
    let (mut value, mut shift, start) = (0usize, 0u32, offset);
    let mut o = offset;
    loop {
        let b = *buf.get(o).ok_or(MessageDecodeError(what))?;
        o += 1;
        value |= ((b & 0x7f) as usize) << shift;
        if b & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 21 {
            return Err(MessageDecodeError(what));
        }
    }
    Ok((value, o - start))
}

fn take<'a>(buf: &'a [u8], offset: usize, len: usize, what: &'static str) -> Result<&'a [u8]> {
    buf.get(offset..offset + len).ok_or(MessageDecodeError(what))
}

fn decode_v1(buf: &[u8]) -> Result<CompiledMessage<'_>> {
    let h = take(buf, 1, 3, "header")?;
    let header = MessageHeader {
        num_required_signatures: h[0],
        num_readonly_signed: h[1],
        num_readonly_unsigned: h[2],
    };
    let mask = u32::from_le_bytes(take(buf, 4, 4, "config mask")?.try_into().unwrap());
    let lifetime_token = take(buf, 8, 32, "lifetime token")?;
    let n_instr = *buf.get(40).ok_or(MessageDecodeError("instruction count"))? as usize;
    let n_keys = *buf.get(41).ok_or(MessageDecodeError("account count"))? as usize;
    let mut o = 42usize;
    let mut static_accounts = Vec::with_capacity(n_keys);
    for _ in 0..n_keys {
        static_accounts.push(take(buf, o, 32, "static accounts")?);
        o += 32;
    }
    let cfg = if mask & 0b11 == 0b11 { 8 } else { 0 }
        + 4 * ((mask >> 2) & 1) as usize
        + 4 * ((mask >> 3) & 1) as usize
        + 4 * ((mask >> 4) & 1) as usize;
    take(buf, o, cfg, "config values")?;
    o += cfg;
    let headers = take(buf, o, 4 * n_instr, "instruction headers")?;
    o += 4 * n_instr;
    let mut instructions = Vec::with_capacity(n_instr);
    for h in headers.chunks_exact(4) {
        let na = h[1] as usize;
        let nd = u16::from_le_bytes([h[2], h[3]]) as usize;
        let account_indices = take(buf, o, na, "instruction accounts")?;
        o += na;
        let data = take(buf, o, nd, "instruction data")?;
        o += nd;
        instructions.push(CompiledInstruction {
            program_index: h[0],
            account_indices,
            data,
        });
    }
    Ok(CompiledMessage {
        version: Some(1),
        header,
        static_accounts,
        lifetime_token,
        instructions,
        address_table_lookups: Vec::new(),
    })
}

pub fn decode_message(buf: &[u8]) -> Result<CompiledMessage<'_>> {
    if buf.first() == Some(&0x81) {
        return decode_v1(buf);
    }
    let mut o = 0usize;

    let first = *buf.first().ok_or(MessageDecodeError("empty message"))?;
    let version = if first & 0x80 != 0 {
        o = 1;
        Some(first & 0x7f)
    } else {
        None
    };

    let h = take(buf, o, 3, "header")?;
    let header = MessageHeader {
        num_required_signatures: h[0],
        num_readonly_signed: h[1],
        num_readonly_unsigned: h[2],
    };
    o += 3;

    let (n, sz) = shortvec(buf, o, "account count")?;
    o += sz;
    let mut static_accounts = Vec::with_capacity(n);
    for _ in 0..n {
        static_accounts.push(take(buf, o, 32, "static accounts")?);
        o += 32;
    }

    let lifetime_token = take(buf, o, 32, "lifetime token")?;
    o += 32;

    let (n, sz) = shortvec(buf, o, "instruction count")?;
    o += sz;
    let mut instructions = Vec::with_capacity(n);
    for _ in 0..n {
        let program_index = *buf.get(o).ok_or(MessageDecodeError("program index"))?;
        o += 1;
        let (na, sz) = shortvec(buf, o, "instruction account count")?;
        o += sz;
        let account_indices = take(buf, o, na, "instruction accounts")?;
        o += na;
        let (nd, sz) = shortvec(buf, o, "instruction data length")?;
        o += sz;
        let data = take(buf, o, nd, "instruction data")?;
        o += nd;
        instructions.push(CompiledInstruction {
            program_index,
            account_indices,
            data,
        });
    }

    let mut address_table_lookups = Vec::new();
    if version.is_some() {
        let (n, sz) = shortvec(buf, o, "lookup count")?;
        o += sz;
        address_table_lookups.reserve(n);
        for _ in 0..n {
            let table_address = take(buf, o, 32, "lookup table address")?;
            o += 32;
            let (nw, sz) = shortvec(buf, o, "writable index count")?;
            o += sz;
            let writable_indexes = take(buf, o, nw, "writable indexes")?;
            o += nw;
            let (nr, sz) = shortvec(buf, o, "readonly index count")?;
            o += sz;
            let readonly_indexes = take(buf, o, nr, "readonly indexes")?;
            o += nr;
            address_table_lookups.push(AddressTableLookup {
                table_address,
                writable_indexes,
                readonly_indexes,
            });
        }
    }

    Ok(CompiledMessage {
        version,
        header,
        static_accounts,
        lifetime_token,
        instructions,
        address_table_lookups,
    })
}

pub fn message_bytes(tx: &[u8]) -> Result<&[u8]> {
    if tx.first() == Some(&0x81) {
        let n = *tx.get(1).ok_or(MessageDecodeError("header"))? as usize;
        let end = tx
            .len()
            .checked_sub(64 * n)
            .filter(|&e| e >= 42)
            .ok_or(MessageDecodeError("signatures do not fit"))?;
        return Ok(&tx[..end]);
    }
    let (n, sz) = shortvec(tx, 0, "signature count")?;
    tx.get(sz + n * 64..)
        .ok_or(MessageDecodeError("signatures do not fit"))
}
