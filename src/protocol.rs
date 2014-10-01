use std::io::{File, BufReader, IoResult, IoError, OtherIoError, stderr};
use std::io::fs::{rename, unlink};
use std::collections::HashSet;

use serialize::json;
use serialize::json::DecoderError;

use uuid::Uuid;
// use msgpack;
use reliable_rw::{copy_out, IntegrityError};
use reliable_rw::ProtocolError as RelRwProtocolError;
use reliable_rw::ReadError as RelRwReadError;
use reliable_rw::WriteError as RelRwWriteError;

use repository::{Repository, FullBackup, IncrementalBackup};


static MAGIC_REQUEST: &'static [u8] = b"\xa8\x5b\x4b\x2b\x1b\x75\x4c\x0a";
static MAGIC_RESPONSE: &'static [u8] = b"\xfb\x70\x4c\x63\x41\x1d\x9c\x0a";


pub enum ProtocolError {
    ReadError(IoError),
    ObjectDecode(DecoderError),
    Other(String)
}

#[deriving(Encodable, Decodable)]
pub struct Edge {
    size: u64,
    from_node: Option<Uuid>,
    to_node: Uuid
}


impl Edge {
    // pub fn is_root(&self) -> bool {
    //     self.from_node.is_none()
    // }
}


#[deriving(Encodable, Decodable)]
pub struct Graph {
    edges: Vec<Edge>
}

impl Graph {
    pub fn new() -> Graph {
        Graph {
            edges: Vec::new()
        }
    }
}


#[deriving(FromPrimitive, PartialEq, Show)]
pub enum ProtocolCommand {
    Quit = 0,
    FindNodes = 1,
    ListNodes = 2,
    UploadArchive = 3,
    GetGraph = 4,
}


pub struct ProtocolServer<'a> {
    reader: &'a mut Reader+'a,
    writer: &'a mut Writer+'a
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

    #[deprecated]
    fn dispatch_find_nodes(&mut self, repo: &Repository) -> IoResult<()> {
        let want_parents: HashSet<Uuid> = try!(self.read_parent_list())
                .into_iter()
                .collect();

        let have_parents: HashSet<Uuid> = repo.iter_nodes()
                .map(|node| node.uuid.clone())
                .collect();

        for cand in want_parents.intersection(&have_parents) {
            try!(self.writer.write_u8(1));
            try!(self.writer.write(cand.as_bytes()));
        }
        try!(self.writer.write_u8(0));
        try!(self.writer.flush());
        Ok(())
    }

    #[deprecated]
    fn dispatch_list_nodes(&mut self, repo: &Repository) -> IoResult<()> {
        let mut err = stderr();

        let mut node_count: uint = 0;
        try!(err.write(format!("listing nodes...\n").as_bytes()));

        for node in repo.iter_nodes() {
            try!(self.writer.write_u8(1));
            try!(self.writer.write(node.uuid.as_bytes()));
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
        
        assert!(stderr_writer.write(format!(
            "SERVER: obj:{} create\n",
            object_id_str
        ).as_bytes()).is_ok());

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
            Err(RelRwProtocolError) => Err(IoError {
                kind: OtherIoError,
                desc: "ProtocolError during read",
                detail: None
            }),
            Err(RelRwReadError(io_error)) => Err(io_error),
            Err(RelRwWriteError(io_error)) => Err(io_error),
        };
        match result {
            Ok(_) => {
                assert!(stderr_writer.write(format!(
                    "SERVER: obj:{} commit\n",
                    object_id_str
                ).as_bytes()).is_ok());
                try!(rename(&tmp_path, &final_path));
                try!(self.writer.write(b"\x01"));
                try!(self.writer.write(object_id.as_bytes()));
                try!(self.writer.flush());
                Ok(())
            },
            Err(err) => {
                assert!(stderr_writer.write(format!(
                    "SERVER: obj:{} rollback: {}\n",
                    object_id_str, err
                ).as_bytes()).is_ok());
                try!(self.writer.write(b"\x00"));
                try!(unlink(&tmp_path));
                try!(self.writer.flush());
                Err(err)
            }
        }
    }

    fn dispatch_get_graph(&mut self, repo: &Repository) -> IoResult<()> {
        let mut graph = Graph::new();
        graph.edges.reserve(repo.nodes.len());
        for node in repo.nodes.iter() {
            graph.edges.push(match node.kind {
                FullBackup(ref subv) => {
                    Edge {
                        size: node.size,
                        from_node: None,
                        to_node: subv.uuid.clone()
                    }
                },
                IncrementalBackup(ref snap) => {
                    Edge {
                        size: node.size,
                        from_node: Some(snap.clone_uuid.clone()),
                        to_node: snap.uuid
                    }
                }
            });
        }
        let encoded = json::encode(&graph);
        let encoded_bytes = encoded.as_bytes();
        // let encoded = match msgpack::Encoder::to_msgpack(&graph) {
        //     Ok(encoded) => encoded,
        //     Err(err) => fail!("encoding graph failed: {}", err)
        // };
        let mut stderr_writer = stderr();
        stderr_writer.write_str(
            format!("SERVER: graph_response_len: {}\n",
            encoded.len()).as_slice());
        try!(self.writer.write_be_u32(encoded_bytes.len() as u32));
        try!(self.writer.write(encoded_bytes));
        try!(self.writer.flush())
        Ok(())
    }

    fn dispatch(&mut self, repo: &Repository, command: ProtocolCommand) -> IoResult<()> {
        Ok(match command {
            Quit => (),
            FindNodes => try!(self.dispatch_find_nodes(repo)),
            ListNodes => try!(self.dispatch_list_nodes(repo)),
            UploadArchive => try!(self.dispatch_upload_archive(repo)),
            GetGraph => try!(self.dispatch_get_graph(repo)),
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
            let op_code: Option<ProtocolCommand> = FromPrimitive::from_u64(
                try!(self.reader.read_be_u64()));
            try!(stderr_writer.write(format!("handling {}\n", op_code).as_bytes()));
            try!(stderr_writer.flush());

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


pub struct ProtocolClient<'a> {
    reader: &'a mut Reader+'a,
    writer: &'a mut Writer+'a
}


impl<'a> ProtocolClient<'a> {
    pub fn new<'a>(reader: &'a mut Reader, writer: &'a mut Writer) -> ProtocolClient<'a> {
        ProtocolClient {
            reader: reader,
            writer: writer
        }
    }

    pub fn get_graph(&mut self) -> Result<Graph, ProtocolError> {
        let len = match self.reader.read_be_u32() {
            Ok(len) => len as uint,
            Err(err) => return Err(ReadError(err))
        };
        let bytes = match self.reader.read_exact(len) {
            Ok(bytes) => bytes,
            Err(err) => return Err(ReadError(err))
        };
        let string = match String::from_utf8(bytes) {
            Ok(string) => string,
            Err(err) => return Err(Other(format!("bad encoding: {}", err)))
        };
        match json::decode(string.as_slice()) {
            Ok(graph) => Ok(graph),
            Err(err) => Err(ObjectDecode(err))
        }
    }
}
