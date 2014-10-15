#![allow(dead_code)]
#![feature(slicing_syntax)]

extern crate uuid;
extern crate debug;

use std::path::Path;
use std::io::{BufferedReader, File};
use std::os::args_as_bytes;

use btrfs::BtrfsCommandIter;
mod btrfs;
mod crc32;


fn main() {
    let filename = Path::new(match args_as_bytes().as_slice() {
        [] => fail!("impossible"),
        [_] => {
            println!("print_usage");
            return;
        },
        [_, ref filename] => filename.clone(),
        [_, ref filename, ..] => filename.clone()
    });

    let mut reader = match File::open(&filename) {
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
        if !command.validate_crc32() {
            println!("invalid CRC32");
            break;
        }
        println!("{:?}", command);
    }
}