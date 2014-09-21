#![allow(dead_code)]
extern crate serialize;
extern crate debug;

extern crate uuid;
extern crate msgpack;

extern crate reliable_rw;

use std::collections::HashMap;
use std::os::{args_as_bytes, set_exit_status};
use std::io::fs::stat;
use std::io::{FileStat, TypeDirectory};
use repository::{Repository, BackupNode};
use uuid::Uuid;


mod repository;
mod protocol;
mod btrfs;
mod crc32;


#[cfg(not(test))]
fn print_usage(program: &[u8]) {
    let mut stderr = std::io::stderr();
    let mut out: Vec<u8> = Vec::new();
    out = out.append(b"USAGE: ")
        .append(program)
        .append(b" repository-directory\n");

    assert!(stderr.write(out.as_slice()).is_ok());
}


#[cfg(not(test))]
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

    let foo = match Repository::load_from_nofsck(&path) {
        Ok(repo) => repo,
        Err(err) => fail!("Error while reading repository: {}", err)
    };

    let orphans = foo.find_orphans();

    let mut by_uuid: HashMap<Uuid, Vec<BackupNode>> = HashMap::new();
    for node in foo.nodes.into_iter() {
        let nodes = by_uuid.find_or_insert(node.uuid.clone(), Vec::new());
        nodes.push(node);
    }

    let mut orphan_nodes_lists = orphans.iter()
        .map(|uu| by_uuid.pop(uu))
        .filter(|opt_node_list| opt_node_list.is_some())
        .map(|opt_node_list| opt_node_list.unwrap());

    for orphan_nodes_list in orphan_nodes_lists {
        for orphan_node in orphan_nodes_list.iter() {
            println!("orphan: {}", orphan_node.path.display());
        }
    }
}
