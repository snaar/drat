use std::fs::File;
use std::io;
use std::io::{ErrorKind, Read};
use std::path::Path;

// same as sys_common::io::DEFAULT_BUF_SIZE at the time of writing
const DEFAULT_BUF_SIZE: usize = 8 * 1024;

pub fn are_contents_same<P1: AsRef<Path>, P2: AsRef<Path>>(
    path1: P1,
    path2: P2,
) -> io::Result<bool> {
    let mut buf1: Vec<u8> = vec![0; DEFAULT_BUF_SIZE];
    let mut buf2: Vec<u8> = vec![0; DEFAULT_BUF_SIZE];

    let mut file1 = File::open(path1)?;
    let mut file2 = File::open(path2)?;

    loop {
        let read1 = file1.read(&mut buf1)?;
        if read1 == 0 {
            let read2 = file2.read(&mut buf2)?;
            return Ok(read2 == 0);
        }

        match file2.read_exact(&mut buf2[0..read1]) {
            Ok(_) => {
                if buf1[0..read1] != buf2[0..read1] {
                    return Ok(false);
                }
            }
            Err(e) => {
                return match e.kind() {
                    ErrorKind::UnexpectedEof => Ok(false),
                    _ => Err(e),
                };
            }
        }
    }
}
