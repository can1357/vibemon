use std::io::{self, Read, Write};

pub const MAX_PAYLOAD_LEN: usize = 1 << 20;
pub const HEADER_LEN: usize = 9;

pub const FRAME_REQ: u8 = 1;
pub const FRAME_STDIN: u8 = 2;
pub const FRAME_STDOUT: u8 = 3;
pub const FRAME_STDERR: u8 = 4;
pub const FRAME_EXIT: u8 = 5;
pub const FRAME_RESP: u8 = 6;
pub const FRAME_KILL: u8 = 7;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub ty: u8,
    pub id: u32,
    pub payload: Vec<u8>,
}

#[cfg(test)]
impl Frame {
    pub fn new(ty: u8, id: u32, payload: Vec<u8>) -> Self {
        Self { ty, id, payload }
    }
}

pub fn read_frame<R: Read>(reader: &mut R) -> io::Result<Option<Frame>> {
    let mut header = [0u8; HEADER_LEN];
    header[0] = match read_first_byte(reader)? {
        Some(byte) => byte,
        None => return Ok(None),
    };
    reader.read_exact(&mut header[1..])?;

    let payload_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
    if payload_len > MAX_PAYLOAD_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame payload length {payload_len} exceeds {MAX_PAYLOAD_LEN}"),
        ));
    }

    let ty = header[4];
    let id = u32::from_le_bytes([header[5], header[6], header[7], header[8]]);
    let mut payload = vec![0u8; payload_len];
    if payload_len != 0 {
        reader.read_exact(&mut payload)?;
    }

    Ok(Some(Frame { ty, id, payload }))
}

pub fn write_frame<W: Write>(writer: &mut W, ty: u8, id: u32, payload: &[u8]) -> io::Result<()> {
    if payload.len() > MAX_PAYLOAD_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "frame payload length {} exceeds {}",
                payload.len(),
                MAX_PAYLOAD_LEN
            ),
        ));
    }

    let mut header = [0u8; HEADER_LEN];
    header[..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    header[4] = ty;
    header[5..].copy_from_slice(&id.to_le_bytes());
    writer.write_all(&header)?;
    writer.write_all(payload)
}

fn read_first_byte<R: Read>(reader: &mut R) -> io::Result<Option<u8>> {
    let mut byte = [0u8; 1];
    loop {
        match reader.read(&mut byte) {
            Ok(0) => return Ok(None),
            Ok(1) => return Ok(Some(byte[0])),
            Ok(_) => unreachable!("one-byte buffer cannot read more than one byte"),
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn frame_type_constants_match_contract() {
        assert_eq!(FRAME_REQ, 1);
        assert_eq!(FRAME_STDIN, 2);
        assert_eq!(FRAME_STDOUT, 3);
        assert_eq!(FRAME_STDERR, 4);
        assert_eq!(FRAME_EXIT, 5);
        assert_eq!(FRAME_RESP, 6);
        assert_eq!(FRAME_KILL, 7);
    }

    #[test]
    fn write_frame_uses_little_endian_layout() {
        let mut out = Vec::new();
        write_frame(&mut out, FRAME_REQ, 0x0a0b0c0d, b"hi").unwrap();

        assert_eq!(
            out,
            vec![
                2, 0, 0, 0, // payload length
                FRAME_REQ, 0x0d, 0x0c, 0x0b, 0x0a, // id
                b'h', b'i'
            ]
        );
    }

    #[test]
    fn read_frame_round_trips() {
        let mut bytes = Vec::new();
        write_frame(&mut bytes, FRAME_STDOUT, 42, b"payload").unwrap();

        let frame = read_frame(&mut Cursor::new(bytes)).unwrap().unwrap();
        assert_eq!(frame, Frame::new(FRAME_STDOUT, 42, b"payload".to_vec()));
    }

    #[test]
    fn read_frame_returns_none_on_clean_eof() {
        assert!(
            read_frame(&mut Cursor::new(Vec::<u8>::new()))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn write_frame_rejects_oversized_payload() {
        let payload = vec![0u8; MAX_PAYLOAD_LEN + 1];
        let err = write_frame(&mut Vec::new(), FRAME_REQ, 1, &payload).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn read_frame_rejects_oversized_payload_before_allocating() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&((MAX_PAYLOAD_LEN as u32) + 1).to_le_bytes());
        bytes.push(FRAME_REQ);
        bytes.extend_from_slice(&7u32.to_le_bytes());

        let err = read_frame(&mut Cursor::new(bytes)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
