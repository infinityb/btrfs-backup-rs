use std::gc::{GC, Gc};


pub struct BackupNode {
    name: String
}


impl BackupNode {
    pub fn new(name: &str) -> BackupNode {
        BackupNode {
            name: String::from_str(name)
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

    pub fn load_from(path: &Path) -> Repository {
        let mut repository = Repository::new(path);
        repository.load();
        repository
    }

    fn load(&mut self) {
        //
    }

    pub fn add_edge(&mut self, from: BackupNode, to: BackupNode) {
        self.edges.push((box(GC) from, box(GC) to));
    }
}