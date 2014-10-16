#![allow(dead_code)]
#![feature(slicing_syntax)]

extern crate serialize;
extern crate debug;

extern crate uuid;
extern crate msgpack;

extern crate reliable_rw;
extern crate argparse;

use std::os;
use std::collections::HashMap;
use std::collections::hashmap::{Occupied, Vacant};
use std::os::set_exit_status;
use std::io::fs::stat;
use std::io::{FileStat, TypeDirectory};
use repository::{Repository, BackupNode};
use uuid::Uuid;
use argparse::{ArgumentParser, Store, StoreTrue};

mod repository;
mod protocol;
mod btrfs;
mod crc32;


#[deriving(Show)]
struct ProgramArgs {
    respository_path: String,
    deep: bool,
    verbose: bool
}

impl ProgramArgs {
    fn new() -> ProgramArgs {
        ProgramArgs {
            respository_path: "".to_string(),
            deep: false,
            verbose: false
        }
    }
}


#[cfg(not(test))]
fn main() {
    let mut prog_args = ProgramArgs::new();

    let mut ap = ArgumentParser::new();
    ap.set_description("Check a repository for consistency");

    ap.refer(&mut prog_args.respository_path)
        .add_argument(
            "repository", box Store::<String>, "Path to a Repository")
        .required();

    ap.refer(&mut prog_args.deep)
        .add_option(["-d", "--deep"], box StoreTrue,
        "Deep scan");

    ap.refer(&mut prog_args.verbose)
        .add_option(["-v", "--verbose"], box StoreTrue, "Verbose");

    match ap.parse_args() {
        Ok(()) => {}
        Err(x) => {
            os::set_exit_status(x);
            return;
        }
    }

    let path = Path::new(prog_args.respository_path);

    // Quick sanity check
    match stat(&path) {
        Ok(FileStat { kind: TypeDirectory, .. }) => (),  // Ok
        Ok(stat) => fail!("repository is not a directory: {}", stat.kind),
        Err(e) => fail!("stat error: {}", e)
    }

    let repo = match Repository::load_from_nofsck(&path) {
        Ok(repo) => repo,
        Err(err) => fail!("Error while reading repository: {}", err)
    };

    if prog_args.verbose {
        println!("Loaded repository with {} nodes", repo.nodes.len());
    }
    let orphans = repo.find_orphans();

    if prog_args.verbose && orphans.len() > 0 {
        println!("    including {} orphans", orphans.len());
    }

    let mut by_uuid: HashMap<Uuid, Vec<BackupNode>> = HashMap::new();
    for node in repo.nodes.into_iter() {
        match by_uuid.entry(node.uuid.clone()) {
            Vacant(entry) => entry.set(Vec::new()),
            Occupied(entry) => entry.into_mut()
        }.push(node);
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
