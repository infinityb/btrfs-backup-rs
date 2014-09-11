use std::io::{File, BufReader, IoResult, IoError, OtherIoError};
use std::io::fs::rename;
use std::collections::HashSet;

use uuid::Uuid;
use phf::PhfMap;
use reliable_rw::{copy_out, ProtocolError, IntegrityError, ReadError, WriteError};

use repository::Repository;


static magic_number: &'static [u8] = b"\xa8\x5b\x4b\x2b\x1b\x75\x4c\x0a";


pub struct Protocol<'a> {
    reader: &'a mut Reader+'a,
    writer: &'a mut Writer+'a
}

pub enum ProtocolCommand {
    /// 
    FindNodes,
    ListNodes,
    UploadArchive,
}


static OP_CODES: PhfMap<u64, ProtocolCommand> = phf_map! {
    1_u64 => FindNodes,
    2_u64 => ListNodes,
    3_u64 => UploadArchive,
};


impl<'a> Protocol<'a> {
    pub fn new<'a>(reader: &'a mut Reader, writer: &'a mut Writer) -> Protocol<'a> {
        Protocol {
            reader: reader,
            writer: writer
        }
    }

    pub fn read_magic(&mut self) -> IoResult<bool> {
        let magic = try!(self.reader.read_exact(magic_number.len()));
        Ok(magic.as_slice() == magic_number)
    }

    pub fn read_parent_list(&mut self) -> IoResult<Vec<Uuid>> {
        let list_size = try!(self.reader.read_be_u32());
        let list_data = try!(self.reader.read_exact((16 * list_size) as uint));

        let mut list_reader = BufReader::new(list_data.as_slice());

        let mut out: Vec<Uuid> = Vec::new();
        for _ in range(0, list_size) {
            let uuid_part = try!(list_reader.read_exact(16));
            match Uuid::from_bytes(uuid_part.as_slice()) {
                Some(uuid) => out.push(uuid),
                None => unreachable!()
            }
        }

        Ok(out)
    }

    pub fn write_repository(&mut self, _repo: &Repository) -> IoResult<()> {
        self.writer.write(Vec::new().as_slice())
    }

    fn dispatch_find_nodes(&mut self, repo: &Repository) -> IoResult<()> {
        let want_parents: HashSet<Uuid> = try!(self.read_parent_list())
                .move_iter().collect();

        let have_parents: HashSet<Uuid> = repo.iter_nodes()
                .map(|node| node.get_uuid().clone()).collect();

        for cand in want_parents.intersection(&have_parents) {
            println!("candidate: {}", cand);
            // write out
        }
        Ok(())
    }

    fn dispatch_list_nodes(&mut self, repo: &Repository) -> IoResult<()> {
        for cand in try!(self.read_parent_list()).iter() {
            println!("candidate: {}", cand);
            // write out
        }
        Ok(())
    }

    fn dispatch_upload_archive(&mut self, repo: &Repository) -> IoResult<()> {
        let object_id = Uuid::new_v4().to_hyphenated_string();

        let mut tmp_path = repo.get_root().clone();
        tmp_path.push(format!("{}.tmp", object_id.as_slice()).as_slice());

        let mut final_path = repo.get_root().clone();
        final_path.push(object_id.as_slice());

        let mut file = try!(File::create(&tmp_path));

        try!(match copy_out(self.reader, &mut file) {
            Ok(()) => Ok(()),
            // TODO: fix hacks.
            Err(IntegrityError) => Err(IoError {
                kind: OtherIoError,
                desc: "IntegrityError during read",
                detail: None
            }),
            Err(ProtocolError) => Err(IoError {
                kind: OtherIoError,
                desc: "ProtocolError during read",
                detail: None
            }),
            Err(ReadError(io_error)) => Err(io_error),
            Err(WriteError(io_error)) => Err(io_error),
        });
        try!(rename(&tmp_path, &final_path));
        Ok(())
    }

    fn dispatch(&mut self, repo: &Repository, command: &ProtocolCommand) -> IoResult<()> {
        Ok(match command {
            &FindNodes => try!(self.dispatch_find_nodes(repo)),
            &ListNodes => try!(self.dispatch_list_nodes(repo)),
            &UploadArchive => try!(self.dispatch_upload_archive(repo)),
        })
    }

    pub fn run(&mut self, repo: &Repository) -> IoResult<()> {
        let is_valid = try!(self.read_magic());
        if !is_valid {
            // ProtocolError("Invalid magic")
            fail!("Invalid magic");
        }

        loop {
            let op_code = try!(self.reader.read_be_u64());
            match OP_CODES.find(&op_code) {
                Some(val) => try!(self.dispatch(repo, val)),
                None => {
                    // ProtocolError("Invalid op-code: {}", op_code);
                    fail!("Invalid op-code: {}", op_code);
                }
            }
        }
    }
}