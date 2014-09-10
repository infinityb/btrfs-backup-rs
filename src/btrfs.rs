use uuid::Uuid;

use std::io::{BufReader, IoResult, IoError};

static BTRFS_HEADER_MAGIC: &'static [u8] = b"btrfs-stream\x00";

#[cfg(test)]
static BTRFS_SAMPLE_SUBVOL: &'static [u8] = b"btrfs-stream\x00\x01\x00\x00\x00:\x00\x00\x00\x01\x00\x9bd}\xab\x0f\x00\x16\x00root_jessie_2014-07-21\x01\x00\x10\x00\xa37K@\xc0\x8e\xb5E\x93\xf7\x83a\xe8\xb45\xb8\x02\x00\x08\x00\xc6\x95\x00\x00\x00\x00\x00\x00\x1c\x00\x00\x00\x13\x00\x027-\x8c\x0f\x00\x00\x00\x06\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x12";

#[cfg(test)]
static BTRFS_SAMPLE_SNAPSHOT: &'static [u8] = b"btrfs-stream\x00\x01\x00\x00\x00Z\x00\x00\x00\x02\x00\xd78\x04+\x0f\x00\x16\x00root_jessie_2014-08-25\x01\x00\x10\x00\x19\xf1vb=y\x94O\xb4\x0fm\xcc\x1dy@\xd1\x02\x00\x08\x00?)\x00\x00\x00\x00\x00\x00\x14\x00\x10\x00\x8a\xcf\\z3\x0ciD\xa7\x13\xa8\xfb\xa5v\x15x\x15\x00\x08\x00\xd2\x18\x00\x00\x00\x00\x00\x004\x00\x00\x00\x14\x00\r\xe5\xc0%\x0f";


pub enum BtrfsCommand {
    BtrfsSubvolCommand(BtrfsSubvol),
    BtrfsSnapshotCommand(BtrfsSnapshot),
}


impl BtrfsCommand {
    pub fn parse(reader: &mut Reader) -> Result<BtrfsCommand, BtrfsParseError> {
        let _len = match reader.read_le_u32() {
            Ok(length) => length,
            Err(err) => return Err(ReadError(err))
        };
        let command = match reader.read_le_u16() {
            Ok(command_num) => command_num,
            Err(err) => return Err(ReadError(err))
        };
        let _crc32 = match reader.read_le_u32() {
            Ok(length) => length,
            Err(err) => return Err(ReadError(err))
        };

        match command {
            1 => match BtrfsSubvol::parse(reader) {
                Ok(subv) => Ok(BtrfsSubvolCommand(subv)),
                Err(err) => Err(err)
            },
            2 => match BtrfsSnapshot::parse(reader) {
                Ok(subv) => Ok(BtrfsSnapshotCommand(subv)),
                Err(err) => Err(err)
            },
            cmd => Err(ProtocolError(format!("Unknown command: {}", cmd)))
        }
    }
}


#[deriving(Show)]
pub enum BtrfsParseError {
    InvalidVersion,
    ProtocolError(String),
    ReadError(IoError)
}


#[deriving(Show)]
pub struct BtrfsHeader {
    version: u32,
}


impl BtrfsHeader {
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
}


pub struct BtrfsSubvol {
    pub name: Vec<u8>,
    pub uuid: Uuid,
    pub ctransid: u64,
}


impl BtrfsSubvol {
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
}


pub struct BtrfsSnapshot {
    pub name: Vec<u8>,
    pub uuid: Uuid,
    pub ctransid: u64,
    pub clone_uuid: Uuid,
    pub clone_ctransid: u64,
}


impl BtrfsSnapshot {
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


struct BtrfsCommandIter<'a> {
    reader: &'a mut Reader+'a
    }


impl<'a> BtrfsCommandIter<'a> {
    pub fn new<'a>(reader: &'a mut Reader) -> Result<BtrfsCommandIter<'a>, BtrfsParseError> {
        let header = try!(BtrfsHeader::parse(reader));
        if header.version != 1 {
            return Err(InvalidVersion);
        }
        Ok(BtrfsCommandIter {
            reader: reader
        })
    }
}

impl<'a> Iterator<BtrfsCommand> for BtrfsCommandIter<'a> {
    fn next(&mut self) -> Option<BtrfsCommand> {
        match BtrfsCommand::parse(self.reader) {
            Ok(command) => Some(command),
            Err(_) => None
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
    match BtrfsCommand::parse(&mut reader) {
        Ok(BtrfsSubvolCommand(subvol)) => {
            assert_eq!(subvol.name.as_slice(), b"root_jessie_2014-07-21");
            assert_eq!(subvol.uuid, uuid);  // TODO: fails
            assert_eq!(subvol.ctransid, 38342);
        },
        Ok(_) => unreachable!(),
        Err(err) => unreachable!()
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
        Ok(BtrfsSnapshotCommand(snapshot)) => {
            assert_eq!(snapshot.name.as_slice(), b"root_jessie_2014-08-25");
            assert_eq!(snapshot.uuid, uuid);
            assert_eq!(snapshot.ctransid, 10559);
            assert_eq!(snapshot.clone_uuid, clone_uuid);  // TODO: fails
            assert_eq!(snapshot.clone_ctransid, 6354);
        },
        Ok(_) => unreachable!(),
        Err(err) => unreachable!()
    };
}


pub fn get_first_command(reader: &mut Reader) -> Result<BtrfsCommand, BtrfsParseError> {
    let mut cmd_iter = try!(BtrfsCommandIter::new(reader));
    match cmd_iter.next() {
        Some(cmd) => Ok(cmd),
        None => Err(ProtocolError(format!("No commands")))
    }
}