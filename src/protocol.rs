use std::io::IoResult;
use repository::Repository;

static magic_number: &'static [u8] = b"\xa8\x5b\x4b\x2b\x1b\x75\x4c\x0a";


pub struct Protocol<'a> {
    reader: &'a mut Reader+'a,
    writer: &'a mut Writer+'a
}

impl<'a> Protocol<'a> {
    pub fn new<'a>(reader: &'a mut Reader, writer: &'a mut Writer) -> Protocol<'a> {
        Protocol {
            reader: reader,
            writer: writer
        }
    }

    pub fn read_magic(&mut self) -> IoResult<bool> {
        let magic = try!(self.reader.read_exact(magic_number.len()));
        Ok(magic.as_slice() == magic_number)
    }

    pub fn write_repository(&mut self, _repo: &Repository) -> IoResult<()> {
        self.writer.write(Vec::new().as_slice())
    }
}