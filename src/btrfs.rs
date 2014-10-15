#![feature(slicing_syntax)]

use uuid::Uuid;
use std::io::{BufReader, BufWriter, IoResult, IoError, EndOfFile};
use crc32::crc32c;


static BTRFS_HEADER_MAGIC: &'static [u8] = b"btrfs-stream\x00";

#[cfg(test)]
static BTRFS_SAMPLE_SUBVOL: &'static [u8] = b"btrfs-stream\x00\x01\x00\x00\x00:\x00\x00\x00\x01\x00\x9bd}\xab\x0f\x00\x16\x00root_jessie_2014-07-21\x01\x00\x10\x00\xa37K@\xc0\x8e\xb5E\x93\xf7\x83a\xe8\xb45\xb8\x02\x00\x08\x00\xc6\x95\x00\x00\x00\x00\x00\x00\x1c\x00\x00\x00\x13\x00\x027-\x8c\x0f\x00\x00\x00\x06\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x12";

#[cfg(test)]
static BTRFS_SAMPLE_SNAPSHOT: &'static [u8] = b"btrfs-stream\x00\x01\x00\x00\x00Z\x00\x00\x00\x02\x00\xd78\x04+\x0f\x00\x16\x00root_jessie_2014-08-25\x01\x00\x10\x00\x19\xf1vb=y\x94O\xb4\x0fm\xcc\x1dy@\xd1\x02\x00\x08\x00?)\x00\x00\x00\x00\x00\x00\x14\x00\x10\x00\x8a\xcf\\z3\x0ciD\xa7\x13\xa8\xfb\xa5v\x15x\x15\x00\x08\x00\xd2\x18\x00\x00\x00\x00\x00\x004\x00\x00\x00\x14\x00\r\xe5\xc0%\x0f";


#[deriving(Show)]
pub enum BtrfsParseError {
    InvalidVersion,
    ProtocolError(String),
    ReadError(IoError)
}

pub type BtrfsParseResult<T> = Result<T, BtrfsParseError>;


impl BtrfsParseError {
    pub fn is_eof(err: &BtrfsParseError) -> bool {
        match err {
            &ReadError(ref ioerr) => ioerr.kind == EndOfFile,
            _ => false
        }
    }
}

#[deriving(Show)]
pub enum BtrfsConcatError {
    InvalidOrder,
    FileOpenError(IoError),
    ParseError(BtrfsParseError)
}

type BtrfsConcatResult<T> = Result<T, BtrfsConcatError>;


#[allow(non_camel_case_types)]
#[deriving(FromPrimitive, PartialEq, Clone, Show)]
pub enum BtrfsCommandType {
    BTRFS_SEND_C_UNSPEC,
    BTRFS_SEND_C_SUBVOL,
    BTRFS_SEND_C_SNAPSHOT,
    BTRFS_SEND_C_MKFILE,
    BTRFS_SEND_C_MKDIR,
    BTRFS_SEND_C_MKNOD,
    BTRFS_SEND_C_MKFIFO,
    BTRFS_SEND_C_MKSOCK,
    BTRFS_SEND_C_SYMLINK,
    BTRFS_SEND_C_RENAME,
    BTRFS_SEND_C_LINK,
    BTRFS_SEND_C_UNLINK,
    BTRFS_SEND_C_RMDIR,
    BTRFS_SEND_C_SET_XATTR,
    BTRFS_SEND_C_REMOVE_XATTR,
    BTRFS_SEND_C_WRITE,
    BTRFS_SEND_C_CLONE,
    BTRFS_SEND_C_TRUNCATE,
    BTRFS_SEND_C_CHMOD,
    BTRFS_SEND_C_CHOWN,
    BTRFS_SEND_C_UTIMES,
    BTRFS_SEND_C_END,
    BTRFS_SEND_C_UPDATE_EXTENT
}

pub struct BtrfsCommandBuf(pub Vec<u8>);


impl BtrfsCommandBuf {
    pub fn get_kind(&self) -> Option<BtrfsCommandType> {
        let BtrfsCommandBuf(ref buf) = *self;
        FromPrimitive::from_u16(BufReader::new(buf[4..6]).read_le_u16().unwrap())
    }

    pub fn get_crc32(&self) -> u32 {
        let BtrfsCommandBuf(ref buf) = *self;
        let mut reader = BufReader::new(buf[6..10]);
        reader.read_le_u32().unwrap()
    }

    pub fn validate_crc32(&self) -> bool {
        self.calculate_crc32() == self.get_crc32()
    }

    pub fn calculate_crc32(&self) -> u32 {
        let BtrfsCommandBuf(ref buf) = *self;
        let crc32_state = crc32c(0, buf[0..6]);
        let crc32_state = crc32c(crc32_state, b"\x00\x00\x00\x00");
        crc32c(crc32_state, buf[10..])
    }

    pub fn read(reader: &mut Reader) -> IoResult<BtrfsCommandBuf> {
        let len = try!(reader.read_le_u32());
        let want_bytes = (2 + 4 + len) as uint;
        let mut buf = Vec::from_fn(4 + want_bytes, |_| 0);
        {
            let mut writer = BufWriter::new(buf[mut]);
            assert!(writer.write_le_u32(len).is_ok());
        }
        // Reading exactly into a buffer
        assert_eq!(want_bytes, try!(reader.read_at_least(want_bytes, buf[mut 4..])));
        Ok(BtrfsCommandBuf(buf))
    }

    pub fn parse(&self) -> Result<BtrfsCommand, BtrfsParseError> {
        let BtrfsCommandBuf(ref buf) = *self;
        BtrfsCommand::parse(&mut BufReader::new(buf[]))
    }
}

#[test]
fn test_btrfs_cmd_buf() {
    let mut reader = BufReader::new(BTRFS_SAMPLE_SUBVOL);
    let header = match BtrfsHeader::parse(&mut reader) {
        Ok(header) => header,
        Err(err) => fail!("err: {}", err)
    };
    assert_eq!(header.version, 1);
    let command_buf = match BtrfsCommandBuf::read(&mut reader) {
        Ok(command_buf) => command_buf,
        Err(err) => fail!("err: {}", err)
    };
    assert_eq!(command_buf.get_crc32(), command_buf.calculate_crc32());
    assert_eq!(command_buf.get_kind(), Some(BTRFS_SEND_C_SUBVOL));
}


#[deriving(Clone)]
pub struct BtrfsCommand {
    pub len: u32,
    pub kind: BtrfsCommandType,
    pub crc32: u32,
    pub data: Vec<u8>
}

impl BtrfsCommand {
    pub fn from_kind(kind: BtrfsCommandType, data: Vec<u8>) -> BtrfsCommand {
        let mut out = BtrfsCommand {
            len: data.len() as u32,
            kind: kind,
            crc32: 0,
            data: data
        };
        out.crc32 = out.calculate_crc32();
        out
    }

    fn parse(reader: &mut Reader) -> Result<BtrfsCommand, BtrfsParseError> {
        let len = match reader.read_le_u32() {
            Ok(length) => length,
            Err(err) => return Err(ReadError(err))
        };
        let command = match reader.read_le_u16() {
            Ok(command_num) => command_num,
            Err(err) => return Err(ReadError(err))
        };
        let crc32 = match reader.read_le_u32() {
            Ok(crc) => crc,
            Err(err) => return Err(ReadError(err))
        };
        let buf = match reader.read_exact(len as uint) {
            Ok(buf) => buf,
            Err(err) => return Err(ReadError(err))
        };
        Ok(BtrfsCommand {
            len: len,
            kind: FromPrimitive::from_u16(command).unwrap(),
            crc32: crc32,
            data: buf
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut self_w_crc = self.clone();
        let cap = (10 + self.len) as uint;
        let mut buf: Vec<u8> = Vec::from_fn(cap, |_| 0);
        self_w_crc.crc32 = self_w_crc.calculate_crc32();
        {
            let mut writer = BufWriter::new(buf[mut]);
            assert!(writer.write_le_u32(self_w_crc.len).is_ok());
            assert!(writer.write_le_u16(self_w_crc.kind as u16).is_ok());
            assert!(writer.write_le_u32(self_w_crc.crc32).is_ok());
            assert!(writer.write(self_w_crc.data.as_slice()).is_ok());
        }
        buf
    }

    pub fn validate_crc32(&self) -> bool {
        let calc_crc32 = self.calculate_crc32();
        let out = calc_crc32 == self.crc32;
        if !out {
            println!("{} != {}", calc_crc32, self.crc32);
        }
        return out;
    }

    pub fn calculate_crc32(&self) -> u32 {
        assert_eq!(self.data.len(), self.len as uint);
        let mut buf = Vec::with_capacity(self.data.len() + 10);

        let mut header_buf = [0_u8, ..10];
        {
            let mut writer = BufWriter::new(header_buf);
            assert!(writer.write_le_u32(self.len).is_ok());
            assert!(writer.write_le_u16(self.kind as u16).is_ok());
            assert!(writer.write_le_u32(0_u32).is_ok());
        }
        buf.extend(header_buf.iter().map(|x| x.clone()));
        buf.extend(self.data.iter().map(|x| x.clone()));

        crc32c(0, buf.as_slice())
    }
}


#[deriving(Show)]
pub struct BtrfsHeader {
    pub version: u32,
}


impl BtrfsHeader {
    pub fn load(data: &[u8]) -> Result<BtrfsHeader, BtrfsParseError> {
        BtrfsHeader::parse(&mut BufReader::new(data))
    }

    pub fn parse(reader: &mut Reader) -> Result<BtrfsHeader, BtrfsParseError> {
        let magic = match reader.read_exact(BTRFS_HEADER_MAGIC.len()) {
            Ok(val) => val,
            Err(err) => return Err(ReadError(err))
        };
        if magic.as_slice() != BTRFS_HEADER_MAGIC {
            return Err(ProtocolError(format!("Invalid magic")));
        }
        let version = match reader.read_le_u32() {
            Ok(val) => val,
            Err(err) => return Err(ReadError(err))
        };
        Ok(BtrfsHeader { version: version })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = [0u8, ..4];
        assert!(BufWriter::new(buf).write_le_u32(self.version).is_ok());
        let mut out = Vec::new();
        out.extend(BTRFS_HEADER_MAGIC.iter().map(|x| x.clone()));
        out.extend(buf.iter().map(|x| x.clone()));
        out
    }
}


#[deriving(Clone)]
pub struct BtrfsSubvol {
    pub name: Vec<u8>,
    pub uuid: Uuid,
    pub ctransid: u64,
}


impl BtrfsSubvol {
    pub fn load(data: &[u8]) -> Result<BtrfsSubvol, BtrfsParseError> {
        BtrfsSubvol::parse(&mut BufReader::new(data))
    }

    pub fn parse(reader: &mut Reader) -> Result<BtrfsSubvol, BtrfsParseError> {
        let name = match tlv_read(reader) {
            Ok(BtrfsTlvType { type_num: 15, data: data }) => {
                data
            },
            Ok(BtrfsTlvType { type_num: type_num, .. }) => {
                return Err(ProtocolError(format!("Unknown type: {}", type_num)));
            },
            Err(err) => return Err(ReadError(err))
        };
        let uuid = match tlv_read(reader) {
            Ok(BtrfsTlvType { type_num: 1, data: data }) => {
                match Uuid::from_bytes(data.as_slice()) {
                    Some(uuid) => uuid,
                    None => return Err(ProtocolError(format!("Bad UUID")))
                }
            }
            Ok(BtrfsTlvType { type_num: type_num, .. }) => {
                return Err(ProtocolError(format!("Unknown type: {}", type_num)));
            },
            Err(err) => return Err(ReadError(err))
        };
        let ctransid = match tlv_read(reader) {
            Ok(BtrfsTlvType { type_num: 2, data: data }) => {
                let mut reader = BufReader::new(data.as_slice());
                match reader.read_le_u64() {
                    Ok(val) => val,
                    Err(err) => {
                        return Err(ProtocolError(format!("Err: {}", err)));
                    }
                }
            },
            Ok(BtrfsTlvType { type_num: type_num, .. }) => {
                return Err(ProtocolError(format!("Unknown type: {}", type_num)));
            },
            Err(err) => return Err(ReadError(err))
        };
        Ok(BtrfsSubvol {
            name: name,
            uuid: uuid,
            ctransid: ctransid
        })
    }

    pub fn encap(&self) -> BtrfsCommand {
        let cap = 4 * 3 + self.name.len() + 16 + 8;
        let mut data: Vec<u8> = Vec::from_fn(cap as uint, |_| 0);
        {
            let mut writer = BufWriter::new(data[mut]);
            assert!(tlv_push(&mut writer, 15, self.name.as_slice()).is_ok());
            assert!(tlv_push(&mut writer, 1, self.uuid.as_bytes()).is_ok());
            assert!(writer.write_le_u16(2).is_ok());
            assert!(writer.write_le_u16(8).is_ok());
            assert!(writer.write_le_u64(self.ctransid).is_ok());
        }
        BtrfsCommand::from_kind(BTRFS_SEND_C_SUBVOL, data)
    }
}


#[deriving(Clone)]
pub struct BtrfsSnapshot {
    pub name: Vec<u8>,
    pub uuid: Uuid,
    pub ctransid: u64,
    pub clone_uuid: Uuid,
    pub clone_ctransid: u64,
}


impl BtrfsSnapshot {
    pub fn load(data: &[u8]) -> Result<BtrfsSnapshot, BtrfsParseError> {
        BtrfsSnapshot::parse(&mut BufReader::new(data))
    }

    pub fn parse(reader: &mut Reader) -> Result<BtrfsSnapshot, BtrfsParseError> {
        let name = match tlv_read(reader) {
            Ok(BtrfsTlvType { type_num: 15, data: data }) => {
                data
            },
            Ok(BtrfsTlvType { type_num: type_num, .. }) => {
                return Err(ProtocolError(format!("Unknown type for name: {}", type_num)));
            },
            Err(err) => return Err(ReadError(err))
        };
        let uuid = match tlv_read(reader) {
            Ok(BtrfsTlvType { type_num: 1, data: data }) => {
                match Uuid::from_bytes(data.as_slice()) {
                    Some(uuid) => uuid,
                    None => return Err(ProtocolError(format!("Bad UUID")))
                }
            }
            Ok(BtrfsTlvType { type_num: type_num, .. }) => {
                return Err(ProtocolError(format!("Unknown type for uuid: {}", type_num)));
            },
            Err(err) => return Err(ReadError(err))
        };
        let ctransid = match tlv_read(reader) {
            Ok(BtrfsTlvType { type_num: 2, data: data }) => {
                let mut reader = BufReader::new(data.as_slice());
                match reader.read_le_u64() {
                    Ok(val) => val,
                    Err(err) => {
                        return Err(ProtocolError(format!("Err: {}", err)));
                    }
                }
            },
            Ok(BtrfsTlvType { type_num: type_num, .. }) => {
                return Err(ProtocolError(format!("Unknown type for ctransid: {}", type_num)));
            },
            Err(err) => return Err(ReadError(err))
        };
        let clone_uuid = match tlv_read(reader) {
            Ok(BtrfsTlvType { type_num: 20, data: data }) => {
                match Uuid::from_bytes(data.as_slice()) {
                    Some(uuid) => uuid,
                    None => return Err(ProtocolError(format!("Bad UUID")))
                }
            }
            Ok(BtrfsTlvType { type_num: type_num, .. }) => {
                return Err(ProtocolError(format!("Unknown type for clone_uuid: {}", type_num)));
            },
            Err(err) => return Err(ReadError(err))
        };
        let clone_ctransid = match tlv_read(reader) {
            Ok(BtrfsTlvType { type_num: 21, data: data }) => {
                let mut reader = BufReader::new(data.as_slice());
                match reader.read_le_u64() {
                    Ok(val) => val,
                    Err(err) => {
                        return Err(ProtocolError(format!("Err: {}", err)));
                    }
                }
            },
            Ok(BtrfsTlvType { type_num: type_num, .. }) => {
                return Err(ProtocolError(format!("Unknown type for clone_ctransid: {}", type_num)));
            },
            Err(err) => return Err(ReadError(err))
        };
        Ok(BtrfsSnapshot {
            name: name,
            uuid: uuid,
            ctransid: ctransid,
            clone_uuid: clone_uuid,
            clone_ctransid: clone_ctransid
        })
    }

    pub fn encap(&self) -> BtrfsCommand {
        let cap = 4 * 5 + self.name.len() + 2 * 16 + 8 + 8;
        let mut data: Vec<u8> = Vec::from_fn(cap as uint, |_| 0);
        {
            let mut writer = BufWriter::new(data[mut]);
            assert!(tlv_push(&mut writer, 15, self.name.as_slice()).is_ok());
            assert!(tlv_push(&mut writer, 1, self.uuid.as_bytes()).is_ok());
            assert!(writer.write_le_u16(2).is_ok());
            assert!(writer.write_le_u16(8).is_ok());
            assert!(writer.write_le_u64(self.ctransid).is_ok());
            assert!(tlv_push(&mut writer, 20, self.clone_uuid.as_bytes()).is_ok());
            assert!(writer.write_le_u16(21).is_ok());
            assert!(writer.write_le_u16(8).is_ok());
            assert!(writer.write_le_u64(self.clone_ctransid).is_ok());
        }
        BtrfsCommand::from_kind(BTRFS_SEND_C_SNAPSHOT, data)
    }
}


struct BtrfsTlvType {
    type_num: u16,
    data: Vec<u8>
}


fn tlv_read(reader: &mut Reader) -> IoResult<BtrfsTlvType> {
    let tlv_type = try!(reader.read_le_u16());
    let tlv_len = try!(reader.read_le_u16());
    let data = try!(reader.read_exact(tlv_len as uint));
    Ok(BtrfsTlvType {
        type_num: tlv_type,
        data: data
    })
}

fn tlv_push(writer: &mut Writer, tlv_type: u16, buf: &[u8]) -> IoResult<()> {
    try!(writer.write_le_u16(tlv_type));
    try!(writer.write_le_u16(buf.len() as u16));
    try!(writer.write(buf));
    Ok(())
}

pub struct BtrfsCommandIter<'a> {
    reader: &'a mut Reader+'a,
    is_finished: bool
}


impl<'a> BtrfsCommandIter<'a> {
    pub fn new<'a>(reader: &'a mut Reader) -> Result<BtrfsCommandIter<'a>, BtrfsParseError> {
        let header = try!(BtrfsHeader::parse(reader));
        if header.version != 1 {
            return Err(InvalidVersion);
        }
        Ok(BtrfsCommandIter {
            reader: reader,
            is_finished: false
        })
    }
}

impl<'a> Iterator<BtrfsCommand> for BtrfsCommandIter<'a> {
    fn next(&mut self) -> Option<BtrfsCommand> {
        if self.is_finished {
            return None
        }
        match BtrfsCommand::parse(self.reader) {
            Ok(command) => {
                if command.kind == BTRFS_SEND_C_END {
                    self.is_finished = true;
                }
                Some(command)
            }
            Err(err) => fail!("err: {}", err)
        }
    }
}


#[test]
fn test_subvol_metadata_extract() {
    let mut reader = BufReader::new(BTRFS_SAMPLE_SUBVOL);
    let header = match BtrfsHeader::parse(&mut reader) {
        Ok(header) => header,
        Err(err) => fail!("err: {}", err)
    };
    assert_eq!(header.version, 1);
                                     
    let uuid = Uuid::parse_str("a3374b40-c08e-b545-93f7-8361e8b435b8").ok().unwrap();

    let command = match BtrfsCommand::parse(&mut reader) {
        Ok(command) => command,
        Err(_) => unreachable!()
    };
    assert_eq!(command.kind, BTRFS_SEND_C_SUBVOL);
    let subvol = match BtrfsSubvol::load(command.data.as_slice()) {
        Ok(subvol) => subvol,
        Err(err) => fail!("err: {}", err)
    };
}

#[test]
fn test_snapshot_metadata_extract() {
    let mut reader = BufReader::new(BTRFS_SAMPLE_SNAPSHOT);
    let header = match BtrfsHeader::parse(&mut reader) {
        Ok(header) => header,
        Err(err) => fail!("err: {}", err)
    };
    assert_eq!(header.version, 1);

    let uuid = Uuid::parse_str("19f17662-3d79-944f-b40f-6dcc1d7940d1").ok().unwrap();
    let clone_uuid = Uuid::parse_str("8acf5c7a-330c-6944-a713-a8fba5761578").ok().unwrap();

    match BtrfsCommand::parse(&mut reader) {
        Ok(command) => {
            assert_eq!(command.kind, BTRFS_SEND_C_SNAPSHOT);
        },
        Err(_) => unreachable!()
    };
}


pub fn get_first_command(reader: &mut Reader) -> Result<BtrfsCommand, BtrfsParseError> {
    let mut cmd_iter = try!(BtrfsCommandIter::new(reader));
    match cmd_iter.next() {
        Some(cmd) => Ok(cmd),
        None => Err(ProtocolError(format!("No commands")))
    }
}
