#![allow(dead_code)]
extern crate uuid;
extern crate debug;

use std::path::Path;
use std::io::{BufferedReader, File};
use std::os::args_as_bytes;

use btrfs::BtrfsCommandIter;
mod btrfs;
mod crc32;


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
    let paths: Vec<Path> = filenames.into_iter().map(|x| Path::new(x)).collect();

    for filename in paths.iter() {
        let mut reader = match File::open(filename) {
            Ok(file) => BufferedReader::new(file),
            Err(err) => fail!("{}", err)
        };

        let mut command_iter = match BtrfsCommandIter::new(&mut reader) {
            Ok(iter) => iter,
            Err(err) => {
                println!("error opening file: {}", err);
                return;
            }
        };

        for command in command_iter {
            println!("{:?}", command);
        }
    }
}