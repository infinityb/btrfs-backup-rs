#![feature(macro_rules)]
#![allow(dead_code)]
#![feature(slicing_syntax)]

extern crate uuid;
extern crate debug;

use std::path::Path;
use std::io::{BufReader, BufferedReader, BufferedWriter, File, IoResult, stdout};
use std::os::args_as_bytes;
use std::collections::{RingBuf, Deque};

use uuid::Uuid;

use btrfs::{
    BtrfsHeader,
    BtrfsCommandBuf,
    BtrfsSubvol,
    BtrfsSnapshot,
    BtrfsParseResult,
    ReadError,
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
    current_path: Option<Path>,
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

        let last_snap_cmd = match BtrfsCommandBuf::read(&mut last_reader) {
            Ok(command) => match BtrfsSnapshot::load(command.get_data()) {
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
            current_path: None,
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
    fn validation_hook(&mut self, command: &BtrfsCommandBuf) -> BtrfsParseResult<()> {
        if command.get_kind() == Some(BTRFS_SEND_C_SUBVOL) {
            assert!(self.curr_uuid.is_none());
            match BtrfsSubvol::load(command.get_data()) {
                Ok(subvol) => {
                    self.curr_uuid = Some(subvol.uuid);
                },
                Err(err) => fail!("err: {}", err)
            }
        }
        if command.get_kind() == Some(BTRFS_SEND_C_SNAPSHOT) {
            match BtrfsSnapshot::load(command.get_data()) {
                Ok(snap) => {
                    assert_eq!(self.curr_uuid, Some(snap.clone_uuid));
                    self.curr_uuid = Some(snap.uuid);
                },
                Err(err) => fail!("err: {}", err)
            }
        }
        Ok(())
    }

    #[inline]
    fn suppress_command(&self, command: &BtrfsCommandBuf) -> bool {
        (
            (
                command.get_kind() == Some(BTRFS_SEND_C_END) &&
                self.last_reader.is_some()
            ) || (
                command.get_kind() == Some(BTRFS_SEND_C_SNAPSHOT)
            )
        )
    }

    #[inline]
    fn transform(&mut self, command: BtrfsCommandBuf) -> BtrfsCommandBuf {
        if self.last_snap_cmd.is_some() && command.get_kind() == Some(BTRFS_SEND_C_SUBVOL) {
            let mut subv = BtrfsSubvol::load(command.get_data()).unwrap();
            subv.name = self.last_snap_cmd.take().unwrap().name;
            let encapped = subv.encap().serialize();
            BtrfsCommandBuf::read(&mut BufReader::new(encapped[])).unwrap()
        } else {
            command
        }
    }

    fn current_command<'a>(&'a mut self) -> Option<BtrfsParseResult<BtrfsCommandBuf>> {
        if self.reader.is_some() {
            let buf = match BtrfsCommandBuf::read(self.reader.as_mut().unwrap()) {
                Ok(buf) => buf,
                Err(err) => return Some(Err(ReadError(err)))
            };
            some_try!(self.validation_hook(&buf));
            match buf.parse() {
                Ok(command) => {
                    
                    return Some(Ok(self.transform(buf)));
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
                match BtrfsHeader::parse(&mut buf) {
                    Ok(header) => assert_eq!(header.version, 1),
                    Err(err) => fail!("err: {}", err)
                };
                buf
            }
            Err(err) => return Some(Err(ReadError(err)))
        });
        self.current_path = Some(path);
        self.current_command()
    }
}

impl Iterator<BtrfsParseResult<BtrfsCommandBuf>> for BtrfsCommandConcatIter {
    fn next(&mut self) -> Option<BtrfsParseResult<BtrfsCommandBuf>> {
        loop {
            match self.current_command() {
                Some(Ok(command)) => {
                    if !self.suppress_command(&command) {
                        return Some(Ok(command))
                    }
                },
                Some(Err(err)) => {
                    match self.current_path {
                        Some(ref path) => fail!("err: {} during read of {}", err, path.display()),
                        None => ()
                    }
                    return Some(Err(err));
                }
                None => return None
            }
        }
    }
}

fn write_out(mut iter: BtrfsCommandConcatIter) -> BtrfsParseResult<()> {
    let mut stdout_w = BufferedWriter::new(stdout());
    assert!(stdout_w.write(BtrfsHeader { version: 1 }.serialize()[]).is_ok());
    for command in iter {
        let command = try!(command);
        assert!(stdout_w.write(command.as_slice()).is_ok());
    }
    Ok(())
}

#[cfg(not(test))]
fn main() {
    let filenames = match args_as_bytes()[] {
        [] => fail!("impossible"),
        [_] => {
            println!("print_usage");
            return;
        },
        [_, ref filename] => vec![filename.clone()],
        [_, rest..] => rest.to_vec()
    };

    let paths: Vec<Path> = filenames.into_iter()
        .map(|x| Path::new(x)).collect();

    let iter = match BtrfsCommandConcatIter::new(paths) {
        Ok(iter) => iter,
        Err(err) => fail!("err: {}", err)
    };
    match write_out(iter) {
        Ok(()) => (),
        Err(err) => fail!("err: {}", err)
    }
}
