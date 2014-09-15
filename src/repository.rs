use std::io::{File, BufferedReader, IoResult, stderr};
use std::slice::Items;
use uuid::Uuid;


use btrfs::{
    get_first_command,
    BtrfsCommand,
    BtrfsSubvolCommand,
    BtrfsSubvol,
    BtrfsSnapshotCommand,
    BtrfsSnapshot,
    BtrfsUnknownCommand
};
use std::io::fs::readdir;


// #[deriving(Decodable, Encodable)]
pub enum BackupNodeKind {
    FullBackup(BtrfsSubvol),
    IncrementalBackup(BtrfsSnapshot)
}


// #[deriving(Decodable, Encodable)]
pub struct BackupNode {
    kind: BackupNodeKind,
    uuid: Uuid,
    path: Path,
    name: Vec<u8>
}


impl BackupNode {
    fn from_btrfs_command(path: &Path, command: &BtrfsCommand) -> BackupNode {
        match command.kind {
            BtrfsSubvolCommand(ref subvol) => {
                BackupNode {
                    kind: FullBackup(subvol.clone()),
                    uuid: subvol.uuid.clone(),
                    path: path.clone(),
                    name: subvol.name.clone(),
                }
            },
            BtrfsSnapshotCommand(ref snap) => {
                BackupNode {
                    kind: IncrementalBackup(snap.clone()),
                    uuid: snap.uuid.clone(),
                    path: path.clone(),
                    name: snap.name.clone()
                }
            },
            BtrfsUnknownCommand(command) => {
                fail!("invalid command {}", command);
            }
        }
    }

    pub fn get_uuid<'a>(&'a self) -> &'a Uuid {
        &self.uuid
    }
}


pub struct Repository {
    root: Path,
    nodes: Vec<BackupNode>
}


impl Repository {
    pub fn new(path: &Path) -> Repository {
        Repository {
            root: path.clone(),
            nodes: Vec::new()
        }
    }

    pub fn load_from(path: &Path) -> IoResult<Repository> {
        let mut repository = Repository::new(path);
        try!(repository.load());
        Ok(repository)
    }

    fn load(&mut self) -> IoResult<()> {
        let paths = try!(readdir(&self.root));
        for path in paths.iter() {
            match File::open(path) {
                Ok(file) => {
                    let mut file = BufferedReader::new(file);
                    let command = match get_first_command(&mut file) {
                        Ok(command) => command,
                        Err(_) => continue  // TODO: skip, I guess~  Maybe warn?
                    };
                    let node = BackupNode::from_btrfs_command(path, &command);
                    self.nodes.push(node);
                },
                Err(_) => {
                    // TODO: skip, I guess~  Maybe warn?
                }
            }
        }
        let mut err = stderr();
        err.write(format!("loaded repository with {} nodes\n", self.nodes.len()).as_bytes());
        Ok(())
    }

    pub fn iter_nodes<'a>(&'a self) -> Items<'a, BackupNode> {
        self.nodes.iter()
    }

    pub fn get_root(&self) -> &Path {
        &self.root
    }
}