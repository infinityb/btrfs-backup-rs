extern crate serialize;
extern crate uuid;
extern crate debug;

extern crate reliable_rw;

use std::os::{args_as_bytes, set_exit_status};
use std::io::fs::stat;
use std::io::{FileStat, TypeDirectory, stdin, stdout};
use repository::{Repository};
use protocol::Protocol;

mod repository;
mod protocol;
mod btrfs;


fn print_usage(program: &[u8]) {
    let mut stderr = std::io::stderr();
    let mut out: Vec<u8> = Vec::new();
    out = out.append(b"USAGE: ")
        .append(program)
        .append(b" repository-directory\n");

    assert!(stderr.write(out.as_slice()).is_ok());
}


fn main() {
    let args_bytes = args_as_bytes();
    let program_name = args_bytes[0].as_slice().clone();

    if args_bytes.len() < 2 {
        print_usage(program_name);
        set_exit_status(1);
        return;
    }

    let repository_directory = args_bytes[1].as_slice().clone();
    let path = Path::new(repository_directory);

    // Quick sanity check
    match stat(&path) {
        Ok(FileStat { kind: TypeDirectory, .. }) => (),  // Ok
        Ok(stat) => fail!("repository is not a directory: {}", stat.kind),
        Err(e) => fail!("stat error: {}", e)
    }

    let mut foo = match Repository::load_from(&path) {
        Ok(repo) => repo,
        Err(err) => fail!("Error while reading repository: {}", err)
    };

    let mut stdin = stdin();
    let mut stdout = stdout();
    

    let mut proto = Protocol::new(&mut stdin, &mut stdout);

    match proto.read_magic() {
        Ok(true) => (),
        Ok(false) => fail!("Invalid magic"),
        Err(err) => fail!("Error reading: {}", err)
    };
    proto.write_repository(&foo);

    // foo.add_edge(BackupNode::new("foo"), BackupNode::new("bar"));
}