use wasm_bindgen::prelude::*;

const MAGIC: u32 = 0x5052_5031;
const MAJOR: u8 = 1;
const MINOR: u8 = 0;
const HEADER_BYTES: usize = 36;
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const MAX_ATTRIBUTE_BYTES: usize = 1024 * 1024;
const MAX_ATTRIBUTES: usize = 256;
const MAX_ATTRIBUTE_ID_BYTES: usize = 1024;
const MAX_ATTRIBUTE_VALUE_BYTES: usize = 1024 * 1024;
const ATTRIBUTE_REQUIRED: u8 = 0x01;
const DATA_FRAGMENTED_FLAG: u8 = 0x01;

const HELLO: u8 = 0x01;
const WELCOME: u8 = 0x02;
const CLOSE: u8 = 0x03;
const PING: u8 = 0x04;
const PONG: u8 = 0x05;
const OPEN: u8 = 0x10;
const DATA: u8 = 0x11;
const DEMAND: u8 = 0x12;
const COMPLETE: u8 = 0x13;
const CANCEL: u8 = 0x14;
const ERROR: u8 = 0x15;
const SIGNAL: u8 = 0x16;
const FRAGMENT: u8 = 0x17;

fn known_kind(kind: u8) -> bool {
    matches!(kind, HELLO | WELCOME | CLOSE | PING | PONG | OPEN | DATA | DEMAND | COMPLETE | CANCEL | ERROR | SIGNAL | FRAGMENT)
}

#[wasm_bindgen]
pub fn protocol_major() -> u8 {
    MAJOR
}

#[wasm_bindgen]
pub fn protocol_minor() -> u8 {
    MINOR
}

#[wasm_bindgen]
pub fn validate_frame(frame: &[u8]) -> Result<(), JsValue> {
    if frame.len() > MAX_FRAME_BYTES {
        return Err(JsValue::from_str("frame exceeds the default PRP/1 frame limit"));
    }
    if frame.len() < HEADER_BYTES {
        return Err(JsValue::from_str("frame is shorter than the PRP/1 core header"));
    }
    if u32::from_be_bytes(frame[0..4].try_into().unwrap()) != MAGIC {
        return Err(JsValue::from_str("invalid PRP/1 magic"));
    }
    if frame[4] != MAJOR {
        return Err(JsValue::from_str("unsupported PRP major version"));
    }
    if frame[5] != MINOR {
        return Err(JsValue::from_str("unsupported PRP minor version"));
    }
    let kind = frame[6];
    if !known_kind(kind) {
        return Err(JsValue::from_str("unknown PRP/1 core frame kind"));
    }
    let frame_flags = frame[7];
    if frame[34] != 0 || frame[35] != 0 {
        return Err(JsValue::from_str("PRP/1 reserved header field must be zero"));
    }
    let header_bytes = u16::from_be_bytes(frame[8..10].try_into().unwrap()) as usize;
    if header_bytes != HEADER_BYTES {
        return Err(JsValue::from_str("PRP/1 core header length must be exactly 36 bytes"));
    }
    let stream_id = u64::from_be_bytes(frame[10..18].try_into().unwrap());
    let sequence = u64::from_be_bytes(frame[18..26].try_into().unwrap());
    if sequence == 0 {
        return Err(JsValue::from_str("PRP/1 peer sequence starts at 1"));
    }
    let header_value = u32::from_be_bytes(frame[26..30].try_into().unwrap());
    let attribute_bytes = u32::from_be_bytes(frame[30..34].try_into().unwrap()) as usize;
    if attribute_bytes > MAX_ATTRIBUTE_BYTES {
        return Err(JsValue::from_str("attribute area exceeds the default PRP/1 limit"));
    }
    let end = HEADER_BYTES
        .checked_add(attribute_bytes)
        .ok_or_else(|| JsValue::from_str("attribute length overflow"))?;
    if end > frame.len() {
        return Err(JsValue::from_str("attribute area exceeds frame length"));
    }

    let mut offset = HEADER_BYTES;
    let mut attribute_count = 0usize;
    while offset < end {
        attribute_count += 1;
        if attribute_count > MAX_ATTRIBUTES {
            return Err(JsValue::from_str("frame has too many attributes"));
        }
        if end - offset < 7 {
            return Err(JsValue::from_str("truncated PRP/1 attribute header"));
        }
        let flags = frame[offset];
        if flags & !ATTRIBUTE_REQUIRED != 0 {
            return Err(JsValue::from_str("undefined PRP/1 attribute flag bits"));
        }
        let id_len = u16::from_be_bytes(frame[offset + 1..offset + 3].try_into().unwrap()) as usize;
        let value_len = u32::from_be_bytes(frame[offset + 3..offset + 7].try_into().unwrap()) as usize;
        if id_len == 0 || id_len > MAX_ATTRIBUTE_ID_BYTES {
            return Err(JsValue::from_str("invalid PRP/1 attribute id length"));
        }
        if value_len > MAX_ATTRIBUTE_VALUE_BYTES {
            return Err(JsValue::from_str("PRP/1 attribute value exceeds the default limit"));
        }
        offset += 7;
        let id_end = offset.checked_add(id_len).ok_or_else(|| JsValue::from_str("attribute id length overflow"))?;
        let value_end = id_end.checked_add(value_len).ok_or_else(|| JsValue::from_str("attribute value length overflow"))?;
        if value_end > end {
            return Err(JsValue::from_str("truncated PRP/1 attribute value"));
        }
        std::str::from_utf8(&frame[offset..id_end])
            .map_err(|_| JsValue::from_str("PRP/1 attribute id is not valid UTF-8"))?;
        offset = value_end;
    }

    let has_attributes = attribute_bytes != 0;
    let payload_bytes = frame.len() - end;
    let has_payload = payload_bytes != 0;
    match kind {
        HELLO | WELCOME => {
            if stream_id != 0 || frame_flags != 0 || header_value != 0 || has_payload {
                return Err(JsValue::from_str("invalid HELLO/WELCOME frame shape"));
            }
        }
        CLOSE => {
            if stream_id != 0 || frame_flags != 0 || header_value != 0 || has_attributes {
                return Err(JsValue::from_str("invalid CLOSE frame shape"));
            }
        }
        PING | PONG => {
            if stream_id != 0 || frame_flags != 0 || header_value != 0 || has_attributes || payload_bytes != 8 {
                return Err(JsValue::from_str("invalid PING/PONG frame shape"));
            }
            if u64::from_be_bytes(frame[end..end + 8].try_into().unwrap()) == 0 {
                return Err(JsValue::from_str("PING/PONG probe id must be nonzero"));
            }
        }
        OPEN => {
            if stream_id == 0 || frame_flags != 0 || header_value != 0 || has_payload {
                return Err(JsValue::from_str("invalid OPEN frame shape"));
            }
        }
        DATA => {
            if stream_id == 0 || frame_flags & !DATA_FRAGMENTED_FLAG != 0 {
                return Err(JsValue::from_str("invalid DATA frame shape"));
            }
            let fragmented = frame_flags & DATA_FRAGMENTED_FLAG != 0;
            if fragmented {
                if header_value == 0 || payload_bytes >= header_value as usize {
                    return Err(JsValue::from_str("fragmented DATA must declare a larger total logical payload length"));
                }
            } else if header_value != 0 {
                return Err(JsValue::from_str("non-fragmented DATA must not use the header value field"));
            }
        }
        FRAGMENT => {
            if stream_id == 0 || frame_flags != 0 || header_value != 0 || has_attributes || !has_payload {
                return Err(JsValue::from_str("invalid FRAGMENT frame shape"));
            }
        }
        DEMAND => {
            if stream_id == 0 || frame_flags != 0 || header_value == 0 || has_attributes || has_payload {
                return Err(JsValue::from_str("invalid DEMAND frame shape"));
            }
        }
        COMPLETE => {
            if stream_id == 0 || frame_flags != 0 || header_value != 0 || has_attributes || has_payload {
                return Err(JsValue::from_str("invalid COMPLETE frame shape"));
            }
        }
        CANCEL => {
            if stream_id == 0 || frame_flags != 0 || header_value != 0 || has_attributes {
                return Err(JsValue::from_str("invalid CANCEL frame shape"));
            }
        }
        ERROR => {
            if frame_flags != 0 || header_value != 0 {
                return Err(JsValue::from_str("invalid ERROR frame shape"));
            }
        }
        SIGNAL => {
            if stream_id != 0 || frame_flags != 0 || header_value != 0 {
                return Err(JsValue::from_str("invalid SIGNAL frame shape"));
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}

#[wasm_bindgen]
pub fn encode_header(
    kind: u8,
    stream_id: u64,
    sequence: u64,
    flags: u8,
    header_value: u32,
    attribute_bytes: u32,
) -> Result<Vec<u8>, JsValue> {
    if !known_kind(kind) {
        return Err(JsValue::from_str("unknown PRP/1 core frame kind"));
    }
    if sequence == 0 {
        return Err(JsValue::from_str("PRP/1 peer sequence starts at 1"));
    }
    if attribute_bytes as usize > MAX_ATTRIBUTE_BYTES {
        return Err(JsValue::from_str("attribute area exceeds the default PRP/1 limit"));
    }
    let mut header = vec![0u8; HEADER_BYTES];
    header[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    header[4] = MAJOR;
    header[5] = MINOR;
    header[6] = kind;
    header[7] = flags;
    header[8..10].copy_from_slice(&(HEADER_BYTES as u16).to_be_bytes());
    header[10..18].copy_from_slice(&stream_id.to_be_bytes());
    header[18..26].copy_from_slice(&sequence.to_be_bytes());
    header[26..30].copy_from_slice(&header_value.to_be_bytes());
    header[30..34].copy_from_slice(&attribute_bytes.to_be_bytes());
    Ok(header)
}
