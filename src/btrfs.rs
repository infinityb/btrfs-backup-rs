use uuid::Uuid;

use std::io::{BufReader, IoResult, IoError};

static BTRFS_HEADER_MAGIC: &'static [u8] = b"btrfs-stream\x00";

#[cfg(test)]
static BTRFS_SAMPLE_FILE: &'static [u8] = 
    b"btrfs-stream\x00\x01\x00\x00\x003\x00\x00\x00\x01\x00\x1e\xa7\xeb|\x0f\x00\x0f\x00home_2014-07-21\x01\x00\x10\x00p\xb9\x88\x90\x1e\xfc\x89J\x87|\xb32o\x80\xbd\xdc\x02\x00\x08\x00G\x00\x00\x00\x00\x00\x00\x00\x1c\x00\x00\x00\x13\x00\x027-\x8c\x0f\x00\x00\x00\x06\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x12\x00YUu5\x0f\x00";


pub enum BtrfsCommand {
    BtrfsSubvolCommand(BtrfsSubvol),
    BtrfsSnapshotCommand(BtrfsSnapshot),
}


#[deriving(Show)]
pub enum BtrfsParseError {
    ProtocolError,
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
            return Err(ProtocolError);
        }
        let version = match reader.read_le_u32() {
            Ok(val) => val,
            Err(err) => return Err(ReadError(err))
        };
        Ok(BtrfsHeader { version: version })
    }
}


pub struct BtrfsSubvol {
    name: String,
    uuid: Uuid,
    ctransid: u64,
}


pub struct BtrfsSnapshot {
    name: String,
    uuid: Uuid,
    ctransid: u64,
    clone_uuid: Uuid,
    clone_ctransid: u64,
}


impl BtrfsSnapshot {
    pub fn parse(reader: &Reader) -> Result<BtrfsSnapshot, BtrfsParseError> {
        println!("BtrfsSnapshot::parse not implemented");
        Err(ProtocolError)
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


fn parse_header(reader: &Reader) {
    //
}



#[test]
fn test_subvol_metadata_extract() {
    let mut reader = BufReader::new(BTRFS_SAMPLE_FILE);
    let header = match BtrfsHeader::parse(&mut reader) {
        Ok(header) => header,
        Err(err) => fail!("err: {}", err)
    };
    assert_eq!(header.version, 1);
}