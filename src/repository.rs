use std::gc::{GC, Gc};
use std::io::{File, BufReader, BufferedReader, IoResult, IoError};
use btrfs::{
    get_first_command,
    BtrfsCommand,
    BtrfsSubvolCommand,
    BtrfsSnapshotCommand
};
use std::io::fs::readdir;


pub enum BackupNodeKind {
    FullBackup,
    IncrementalBackup
}


pub struct BackupNode {
    kind: BackupNodeKind,
    path: Path,
    name: Vec<u8>
}


impl BackupNode {
    fn from_btrfs_command(path: &Path, command: &BtrfsCommand) -> BackupNode {
        match command {
            &BtrfsSubvolCommand(ref subvol) => {
                BackupNode {
                    kind: FullBackup,
                    path: path.clone(),
                    name: subvol.name.clone()
                }
            },
            &BtrfsSnapshotCommand(ref snap) => {
                BackupNode {
                    kind: IncrementalBackup,
                    path: path.clone(),
                    name: snap.name.clone()
                }
            }
        }
    }
}


pub struct Repository {
    root: Path,
    edges: Vec<(Gc<BackupNode>, Gc<BackupNode>)>,
    nodes: Vec<Gc<BackupNode>>
}


impl Repository {
    pub fn new(path: &Path) -> Repository {
        Repository {
            root: path.clone(),
            edges: Vec::new(),
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
                Ok(mut file) => {
                    let mut file = BufferedReader::new(file);
                    let command = match get_first_command(&mut file) {
                        Ok(command) => command,
                        Err(_) => continue  // TODO: skip, I guess~  Maybe warn?
                    };
                    let node = BackupNode::from_btrfs_command(path, &command);
                    self.nodes.push(box(GC) node);
                },
                Err(_) => {
                    // TODO: skip, I guess~  Maybe warn?
                }
            }
        }
        println!("loaded repository with {} nodes", self.nodes.len());
        Ok(())
    }

    pub fn add_edge(&mut self, from: BackupNode, to: BackupNode) {
        self.edges.push((box(GC) from, box(GC) to));
    }
}