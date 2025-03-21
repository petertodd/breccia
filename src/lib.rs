#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

use std::collections::VecDeque;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::fmt;

mod buffer;
use self::buffer::Buffer;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Breccia<B> {
    fd: B,
}

fn offset_to_marker(offset: u64) -> u64 {
    !offset
}

impl<B: Read + Seek> Breccia<B> {
    pub fn open(mut fd: B) -> io::Result<Self> {
        fd.rewind()?;
        Ok(Self {
            fd
        })
    }

    pub fn blobs<'a>(&'a mut self) -> io::Result<Blobs<'a, B>> {
        self.fd.seek(SeekFrom::Start(0))?;
        Ok(Blobs::new(&mut self.fd))
    }
}

#[derive(Debug)]
pub struct Blobs<'a, B> {
    buffer: Buffer<&'a mut B>,
}

impl<'a, B> Blobs<'a, B> {
    fn new(fd: &'a mut B) -> Self {
        Self {
            buffer: Buffer::new(fd)
        }
    }
}

impl<'a, B: Read> Blobs<'a, B> {
    pub fn next(&mut self) -> io::Result<Option<(u64, &[u8])>> {
        let mut blob_len: usize = 0;

        loop {
            if self.buffer.buffer().len() < blob_len + 8 {
                // FIXME: limit maximum blob size
                self.buffer.fill(4096)?;
            };

            if let Some(potential_marker) = self.buffer.buffer().get(blob_len .. blob_len + 8) {
                let expected_marker = offset_to_marker(self.buffer.offset() + (blob_len as u64));
                if potential_marker == expected_marker.to_le_bytes() {
                    let offset = self.buffer.offset();
                    let blob_with_marker = self.buffer.consume(blob_len + 8);
                    let blob = &blob_with_marker[0 .. blob_len];
                    break Ok(Some((offset, blob)))
                } else {
                    blob_len += 1;
                }
            } else {
                // We reached the end of the file without finding a valid marker
                break Ok(None)
            }
        }
    }
}

impl<B: Seek + Write> Breccia<B> {
    fn write_marker(&mut self) -> io::Result<u64> {
        let offset = self.fd.seek(SeekFrom::End(0))?;
        let marker = offset_to_marker(offset);
        self.fd.write_all(&marker.to_le_bytes())?;
        Ok(offset + 8)
    }

    pub fn write_blob(&mut self, blob: &[u8]) -> io::Result<u64> {
        'outer: loop {
            // FIXME: don't always write a new marker
            let offset = self.write_marker()?;

            // Validate that the entire blob, at this index, is free of marker collisions
            let mut i = 0;
            while let Some(chunk) = blob.get(i .. i + 8) {
                if offset_to_marker(offset + i as u64).to_le_bytes() == chunk {
                    continue 'outer;
                }
                i += 1;
            }

            while let Some(partial_chunk) = blob.get(i .. ) {
                assert!(partial_chunk.len() < 8);
                if partial_chunk.len() == 0 {
                    break;
                };

                let mut buf = [0; 16];
                let end_marker = offset_to_marker(offset + blob.len() as u64);
                (&mut buf[0 .. partial_chunk.len()]).copy_from_slice(partial_chunk);
                (&mut buf[partial_chunk.len() .. partial_chunk.len() + 8]).copy_from_slice(&end_marker.to_le_bytes());
                let chunk = &buf[0 .. 8];

                if offset_to_marker(offset + i as u64).to_le_bytes() == chunk {
                    continue 'outer;
                }
                i += 1;
            }

            self.fd.write_all(blob)?;
            self.write_marker()?;
            break Ok(offset)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;

    #[test]
    fn write_blob() {
        let b = Cursor::new(vec![]);
        let mut b = Breccia::open(b).unwrap();

        dbg!(offset_to_marker(8).to_le_bytes());
        dbg!(&b);
        dbg!(b.write_blob(&[0xff - 8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 42]).unwrap());
        dbg!(b.write_blob(b"hello world!!!!!!!!").unwrap());
        println!("{:?}", b.fd);

        let mut blobs = b.blobs().unwrap();

        dbg!(&blobs);
        dbg!(blobs.next());
        dbg!(blobs.next());
        dbg!(blobs.next());
        dbg!(blobs.next());
        dbg!(blobs.next());
        dbg!(blobs.next());
        dbg!(blobs.next());
        dbg!(&blobs);
    }
}
