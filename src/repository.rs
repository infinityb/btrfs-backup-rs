use std::io::{File, BufReader, BufferedReader, IoResult};
use std::io::fs::readdir;
use std::slice::Items;
use std::collections::HashSet;

use uuid::Uuid;

use btrfs::{
    get_first_command,
    BtrfsCommand,
    BtrfsSubvol,
    BtrfsSnapshot,
    BTRFS_SEND_C_SUBVOL,
    BTRFS_SEND_C_SNAPSHOT,
};


pub enum BackupNodeKind {
    FullBackup(BtrfsSubvol),
    IncrementalBackup(BtrfsSnapshot)
}


pub struct BackupNode {
    pub kind: BackupNodeKind,
    pub uuid: Uuid,
    pub parent_uuid: Option<Uuid>,
    pub path: Path,
    pub name: Vec<u8>
}


impl BackupNode {
    fn from_btrfs_command(path: &Path, command: &BtrfsCommand) -> BackupNode {
        let mut reader = BufReader::new(command.data.as_slice());
        match command.kind {
            BTRFS_SEND_C_SUBVOL => {
                let subvol = match BtrfsSubvol::parse(&mut reader) {
                    Ok(subvol) => subvol,
                    Err(err) => fail!("err: {}", err)
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
                    Err(err) => fail!("err: {}", err)
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
}


pub struct Repository {
    root: Path,
    pub nodes: Vec<BackupNode>
}


struct FsckReachabilityRecord {
    is_reachable: bool,
    uuid: Uuid,
    parent_uuid: Uuid
}


impl FsckReachabilityRecord {
    fn from_node(node: &BackupNode) -> Option<FsckReachabilityRecord> {
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


impl Repository {
    pub fn new(path: &Path) -> Repository {
        Repository {
            root: path.clone(),
            nodes: Vec::new()
        }
    }

    pub fn load_from(path: &Path) -> IoResult<Repository> {
        Repository::new(path).load(true)
    }

    pub fn load_from_nofsck(path: &Path) -> IoResult<Repository> {
        Repository::new(path).load(false)
    }

    fn load(mut self, fsck: bool) -> IoResult<Repository> {
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

        if fsck {
            let orphans = self.find_orphans();
            self.nodes = self.nodes.into_iter()
                .filter(|n| !orphans.contains(&n.uuid))
                .collect();
        }

        Ok(self)
    }

    pub fn iter_nodes<'a>(&'a self) -> Items<'a, BackupNode> {
        self.nodes.iter()
    }

    pub fn get_root(&self) -> &Path {
        &self.root
    }

    pub fn find_orphans(&self) -> HashSet<Uuid> {
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

            // Rewrite the list if >= 75% of it is reachable
            if total_scanned * 3 <= 4 * reachables_found {
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
}
