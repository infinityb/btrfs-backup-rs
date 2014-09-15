use std::io::{File, BufReader, IoResult, IoError, OtherIoError, stderr};
use std::io::fs::{rename, unlink};
use std::collections::HashSet;

use uuid::Uuid;
use reliable_rw::{copy_out, ProtocolError, IntegrityError, ReadError, WriteError};

use repository::Repository;


static MAGIC_REQUEST: &'static [u8] = b"\xa8\x5b\x4b\x2b\x1b\x75\x4c\x0a";
static MAGIC_RESPONSE: &'static [u8] = b"\xfb\x70\x4c\x63\x41\x1d\x9c\x0a";

pub struct ProtocolServer<'a> {
    reader: &'a mut Reader+'a,
    writer: &'a mut Writer+'a
}


#[deriving(FromPrimitive, PartialEq)]
pub enum ProtocolCommand {
    Quit = 0,
    FindNodes = 1,
    ListNodes = 2,
    UploadArchive = 3,
}


impl<'a> ProtocolServer<'a> {
    pub fn new<'a>(reader: &'a mut Reader, writer: &'a mut Writer) -> ProtocolServer<'a> {
        ProtocolServer {
            reader: reader,
            writer: writer
        }
    }

    pub fn read_magic(&mut self) -> IoResult<bool> {
        let magic = try!(self.reader.read_exact(MAGIC_REQUEST.len()));
        Ok(magic.as_slice() == MAGIC_REQUEST)
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

    fn dispatch_find_nodes(&mut self, repo: &Repository) -> IoResult<()> {
        let want_parents: HashSet<Uuid> = try!(self.read_parent_list())
                .move_iter().collect();

        let have_parents: HashSet<Uuid> = repo.iter_nodes()
                .map(|node| node.get_uuid().clone()).collect();

        for cand in want_parents.intersection(&have_parents) {
            try!(self.writer.write_u8(1));
            try!(self.writer.write(cand.as_bytes()));
        }
        try!(self.writer.write_u8(0));
        try!(self.writer.flush());
        Ok(())
    }

    fn dispatch_list_nodes(&mut self, repo: &Repository) -> IoResult<()> {
        let mut err = stderr();

        let mut node_count: uint = 0;
        try!(err.write(format!("listing nodes...\n").as_bytes()));

        for node in repo.iter_nodes() {
            try!(self.writer.write_u8(1));
            try!(self.writer.write(node.get_uuid().as_bytes()));
            node_count += 1;
        }
        try!(err.write(format!("    sent {} nodes\n", node_count).as_bytes()));
        try!(self.writer.write_u8(0));
        try!(self.writer.flush());
        Ok(())
    }

    fn dispatch_upload_archive(&mut self, repo: &Repository) -> IoResult<()> {
        let object_id = Uuid::new_v4();
        let object_id_str = object_id.to_hyphenated_string();
        let mut stderr_writer = stderr();
        stderr_writer.write(format!("SERVER: obj:{} create\n", object_id_str).as_bytes());

        let mut tmp_path = repo.get_root().clone();
        tmp_path.push(format!("{}.tmp", object_id_str).as_slice());

        let mut final_path = repo.get_root().clone();
        final_path.push(object_id_str.as_slice());

        let mut file = try!(File::create(&tmp_path));

        let result = match copy_out(self.reader, &mut file) {
            Ok(()) => {
                Ok(())
            },
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
        };
        match result {
            Ok(_) => {
                stderr_writer.write(format!("SERVER: obj:{} commit\n", object_id_str).as_bytes());
                try!(rename(&tmp_path, &final_path));
                try!(self.writer.write(b"\x01"));
                try!(self.writer.write(object_id.as_bytes()));
                try!(self.writer.flush());
                Ok(())
            }
            Err(err) => {
                stderr_writer.write(format!(
                    "SERVER: obj:{} rollback: {}\n",
                    object_id_str, err
                ).as_bytes());
                try!(self.writer.write(b"\x00"));
                try!(unlink(&tmp_path));
                try!(self.writer.flush());
                Err(err)
            }
        }
    }

    fn dispatch(&mut self, repo: &Repository, command: ProtocolCommand) -> IoResult<()> {
        Ok(match command {
            Quit => (),
            FindNodes => try!(self.dispatch_find_nodes(repo)),
            ListNodes => try!(self.dispatch_list_nodes(repo)),
            UploadArchive => try!(self.dispatch_upload_archive(repo)),
        })
    }

    pub fn run(&mut self, repo: &Repository) -> IoResult<()> {
        let mut stderr_writer = stderr();
        let is_valid = try!(self.read_magic());
        if !is_valid {
            try!(stderr_writer.write("Invalid magic".as_bytes()));
            try!(stderr_writer.flush());
            return Ok(()); // FIXME?
        }
        try!(self.writer.write(MAGIC_RESPONSE));

        loop {
            let op_code = try!(self.reader.read_be_u64());
            try!(stderr_writer.write(format!("handling OP{}\n", op_code).as_bytes()));
            try!(stderr_writer.flush());
            let op_code: Option<ProtocolCommand> = FromPrimitive::from_u64(op_code);
            match op_code {
                Some(Quit) => break,
                Some(val) => try!(self.dispatch(repo, val)),
                None => {
                    try!(stderr_writer.write("Invalid magic".as_bytes()));
                    try!(stderr_writer.flush());
                    return Ok(()); // FIXME?
                }
            }
        }
        Ok(())
    }
}