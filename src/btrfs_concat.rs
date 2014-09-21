#![feature(macro_rules)]
#![allow(dead_code)]
extern crate uuid;
extern crate debug;

use std::path::Path;
use std::io::{BufferedReader, File, IoResult, stdout};
use std::os::args_as_bytes;
use std::collections::{RingBuf, Deque};

use uuid::Uuid;

use btrfs::{
    BtrfsHeader,
    BtrfsCommand,
    BtrfsSubvol,
    BtrfsSnapshot,
    BtrfsParseResult,
    ReadError,
    BtrfsCommandIter,
    BtrfsParseError,
    BTRFS_SEND_C_SUBVOL,
    BTRFS_SEND_C_SNAPSHOT,
    BTRFS_SEND_C_END,
};

mod btrfs;
mod crc32;

macro_rules! some_try(
    ($e:expr) => (match $e { Ok(e) => e, Err(err) => return Some(Err(err)) })
)


struct BtrfsCommandConcatIter {
    paths: RingBuf<Path>,
    reader: Option<BufferedReader<File>>,
    last_snap_cmd: Option<BtrfsSnapshot>,
    last_reader: Option<BufferedReader<File>>,
    curr_uuid: Option<Uuid>
}

// iters: Vec<BtrfsCommandIter>
impl BtrfsCommandConcatIter {
    pub fn new(paths: Vec<Path>) -> IoResult<BtrfsCommandConcatIter> {
        let mut paths: RingBuf<Path> = FromIterator::from_iter(paths.into_iter());
        if paths.len() < 2 {
            fail!("Insufficient number of paths");
        }

        let mut last_reader = BufferedReader::new(
            try!(File::open(&paths.pop().unwrap())));

        assert_eq!(BtrfsHeader::parse(&mut last_reader).unwrap().version, 1);
        let last_snap_cmd = match BtrfsCommand::parse(&mut last_reader) {
            Ok(command) => match BtrfsSnapshot::load(command.data.as_slice()) {
                Ok(snapshot) => Some(snapshot),
                Err(err) => fail!("error reading last snapshot: {}", err)
            },
            Err(err) => fail!("error reading last command: {}", err)
        };

        let first_reader = match paths.pop_front() {
            Some(path) => {
                let mut buf = BufferedReader::new(try!(File::open(&path)));
                assert_eq!(BtrfsHeader::parse(&mut buf).unwrap().version, 1);
                Some(buf)
            }
            None => None
        };

        Ok(BtrfsCommandConcatIter {
            paths: paths,
            reader: first_reader,
            last_snap_cmd: last_snap_cmd,
            last_reader: Some(last_reader),
            curr_uuid: None
        })
    }

    #[inline]
    fn validate_header(&self, header: &BtrfsHeader) {
        assert!(header.version == 1);
    }

    #[inline]
    fn validation_hook(&mut self, command: &BtrfsCommand) -> BtrfsParseResult<()> {
        if command.kind == BTRFS_SEND_C_SUBVOL {
            assert!(self.curr_uuid.is_none());
            match BtrfsSubvol::load(command.data.as_slice()) {
                Ok(subvol) => {
                    self.curr_uuid = Some(subvol.uuid);
                },
                Err(err) => fail!("err: {}")
            }
        }
        if command.kind == BTRFS_SEND_C_SNAPSHOT {
            match BtrfsSnapshot::load(command.data.as_slice()) {
                Ok(snap) => {
                    assert_eq!(self.curr_uuid, Some(snap.clone_uuid));
                    self.curr_uuid = Some(snap.uuid);
                },
                Err(err) => fail!("err: {}")
            }
        }
        Ok(())
    }

    #[inline]
    fn suppress_command(&self, command: &BtrfsCommand) -> bool {
        (
            (
                command.kind == BTRFS_SEND_C_END &&
                self.last_reader.is_some()
            ) || (
                command.kind == BTRFS_SEND_C_SNAPSHOT
            )
        )
    }

    #[inline]
    fn transform(&mut self, command: BtrfsCommand) -> BtrfsCommand {
        if self.last_snap_cmd.is_some() && command.kind == BTRFS_SEND_C_SUBVOL {
            let mut subv = BtrfsSubvol::load(command.data.as_slice()).unwrap();
            subv.name = self.last_snap_cmd.take().unwrap().name;
            subv.encap()
        } else {
            command
        }
    }

    fn current_command<'a>(&'a mut self) -> Option<BtrfsParseResult<BtrfsCommand>> {
        if self.reader.is_some() {
            match BtrfsCommand::parse(self.reader.as_mut().unwrap()) {
                Ok(command) => {
                    some_try!(self.validation_hook(&command));
                    return Some(Ok(self.transform(command)));
                }
                Err(ref err) if BtrfsParseError::is_eof(err) => {
                    self.reader = None;
                },
                Err(err) => return Some(Err(err))
            }
        }
        if self.paths.is_empty() && self.last_reader.is_some() {
            self.reader = self.last_reader.take();
            return self.current_command();
        }
        let path = match self.paths.pop_front() {
            Some(path) => path,
            None => return None
        };
        self.reader = Some(match File::open(&path) {
            Ok(file) => {
                let mut buf = BufferedReader::new(file);
                BtrfsHeader::parse(&mut buf);
                buf
            }
            Err(err) => return Some(Err(ReadError(err)))
        });
        self.current_command()
    }
}

impl Iterator<BtrfsParseResult<BtrfsCommand>> for BtrfsCommandConcatIter {
    fn next(&mut self) -> Option<BtrfsParseResult<BtrfsCommand>> {
        loop {
            match self.current_command() {
                Some(Ok(command)) => {
                    if !self.suppress_command(&command) {
                        return Some(Ok(command))
                    }
                },
                Some(Err(err)) => return Some(Err(err)),
                None => return None
            }
        }
    }
}

fn write_out(mut iter: BtrfsCommandConcatIter) -> BtrfsParseResult<()> {
    let mut stdout_w = stdout();
    stdout_w.write(BtrfsHeader { version: 1 }.serialize().as_slice());
    for command in iter {
        let command = try!(command);
        stdout_w.write(command.serialize().as_slice());
        // println!("{:?}", command);
    }
    Ok(())
}

#[cfg(not(test))]
fn main() {
    let filenames = match args_as_bytes().as_slice() {
        [] => fail!("impossible"),
        [_] => {
            println!("print_usage");
            return;
        },
        [_, ref filename] => vec![filename.clone()],
        [_, rest..] => Vec::from_slice(rest)
    };

    let paths: Vec<Path> = filenames.into_iter()
        .map(|x| Path::new(x)).collect();
    
    let mut iter = match BtrfsCommandConcatIter::new(paths) {
        Ok(iter) => iter,
        Err(err) => fail!("err: {}", err)
    };
    match write_out(iter) {
        Ok(()) => (),
        Err(err) => fail!("err: {}", err)
    }
}