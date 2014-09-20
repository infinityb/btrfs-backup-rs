use std::io::{File, BufReader, BufferedReader, IoResult, stderr};
use std::slice::Items;
use std::collections::{HashSet, RingBuf, Deque};

use uuid::Uuid;

use btrfs::{
    get_first_command,
    BtrfsCommand,
    BtrfsSubvol,
    BtrfsSnapshot,
    BTRFS_SEND_C_SUBVOL,
    BTRFS_SEND_C_SNAPSHOT,
};
use std::io::fs::readdir;


pub enum BackupNodeKind {
    FullBackup(BtrfsSubvol),
    IncrementalBackup(BtrfsSnapshot)
}


pub struct BackupNode {
    pub kind: BackupNodeKind,
    uuid: Uuid,
    parent_uuid: Option<Uuid>,
    path: Path,
    name: Vec<u8>
}


impl BackupNode {
    fn from_btrfs_command(path: &Path, command: &BtrfsCommand) -> BackupNode {
        let mut reader = BufReader::new(command.data.as_slice());
        match command.kind {
            BTRFS_SEND_C_SUBVOL => {
                let subvol = match BtrfsSubvol::parse(&mut reader) {
                    Ok(subvol) => subvol,
                    Err(err) => fail!("err: {}")
                };
                BackupNode {
                    kind: FullBackup(subvol.clone()),
                    uuid: subvol.uuid.clone(),
                    parent_uuid: None,
                    path: path.clone(),
                    name: subvol.name.clone(),
                }
            },
            BTRFS_SEND_C_SNAPSHOT => {
                let snap = match BtrfsSnapshot::parse(&mut reader) {
                    Ok(snap) => snap,
                    Err(err) => fail!("err: {}")
                };
                BackupNode {
                    kind: IncrementalBackup(snap.clone()),
                    uuid: snap.uuid.clone(),
                    parent_uuid: Some(snap.clone_uuid.clone()),
                    path: path.clone(),
                    name: snap.name.clone()
                }
            },
            _ => {
                fail!("invalid command {}", command.kind);
            }
        }
    }

    pub fn get_uuid<'a>(&'a self) -> &'a Uuid {
        &self.uuid
    }
}


pub struct Repository {
    root: Path,
    pub nodes: Vec<BackupNode>
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
        repository.load()
    }

    fn load(mut self) -> IoResult<Repository> {
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
        let orphans = self.find_orphans();

        let node_count_before = self.nodes.len();
        self.nodes = self.nodes.move_iter()
            .filter(|n| !orphans.contains(&n.uuid))
            .collect();
        let node_count_after = self.nodes.len();
        err.write_str(format!("loaded repository with {} nodes, but showing {}\n",
            node_count_before, node_count_after).as_slice());
        Ok(self)
    }

    pub fn iter_nodes<'a>(&'a self) -> Items<'a, BackupNode> {
        self.nodes.iter()
    }

    pub fn get_root(&self) -> &Path {
        &self.root
    }

    fn find_orphans(&self) -> HashSet<Uuid> {
        let mut root_reachable: HashSet<Uuid> = HashSet::new();
        let mut records: Vec<FsckReachabilityRecord> = Vec::new();

        for node in self.nodes.iter() {
            match FsckReachabilityRecord::from_node(node) {
                Some(record) => records.push(record),
                None => {
                    root_reachable.insert(node.uuid.clone());
                }
            }
        }

        let mut stderr_w = stderr();
        loop {
            let mut changed = false;
            let mut reachables_found: uint = 0;
            let mut total_scanned: uint = 0;

            for record in records.iter_mut() {
                total_scanned += 1;
                if record.is_reachable {
                    reachables_found += 1;
                } else if root_reachable.contains(&record.parent_uuid) {
                    root_reachable.insert(record.uuid.clone());
                    changed = true;
                    record.is_reachable = true;
                    reachables_found += 1;
                }
            }

            // Rewrite the list if 75% of it is reachable
            if (total_scanned * 3 <= 4 * reachables_found) {
                records.retain(|r| !r.is_reachable);
            }
            if !changed {
                break;
            }
        }

        let out: HashSet<Uuid> = records.into_iter()
            .filter(|r| !r.is_reachable)
            .map(|r| r.uuid)
            .collect();
        out
    }

    pub fn fsck(&self) {
        let orphans = self.find_orphans();
        let mut stderr_w = stderr();
        stderr_w.write_str(format!("Found {} orphans\n", orphans.len()).as_slice());
        for orphan in orphans.iter() {
            stderr_w.write_str(format!("Found {} orphans\n", orphans.len()).as_slice());
        }
        stderr_w.flush();
    }
}


struct FsckReachabilityRecord {
    is_reachable: bool,
    uuid: Uuid,
    parent_uuid: Uuid
}

impl FsckReachabilityRecord {
    pub fn from_node(node: &BackupNode) -> Option<FsckReachabilityRecord> {
        match node.parent_uuid {
            Some(ref parent_uuid) => Some(FsckReachabilityRecord {
                is_reachable: false,
                uuid: node.uuid.clone(),
                parent_uuid: parent_uuid.clone()
            }),
            None => None
        }
    }
}