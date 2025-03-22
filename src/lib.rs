#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::ops::Range;
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
        Blobs::new(&mut self.fd)
    }
}

#[derive(Debug)]
pub struct Blobs<'a, B> {
    buffer: Buffer<&'a mut B>,
}

impl<'a, B: Seek> Blobs<'a, B> {
    fn new(fd: &'a mut B) -> io::Result<Self> {
        let offset = fd.seek(SeekFrom::Current(0))?;
        Ok(Self {
            buffer: Buffer::new_with_offset(fd, offset)
        })
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
                if dbg!(potential_marker) == dbg!(expected_marker.to_le_bytes()) {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NextTarget {
    Left,
    Right,
    Next,
}

impl<B: Seek + Read> Breccia<B> {
    pub fn binary_search_by<F, T>(&mut self, f: F) -> io::Result<Option<T>>
        where F: FnMut(u64, &[u8]) -> Result<Option<T>, NextTarget>
    {
        let end_offset = self.fd.seek(SeekFrom::End(0))?;
        self.binary_search_in_range_by(f, 0 .. end_offset)
    }

    pub fn binary_search_in_range_by<F, T>(&mut self, mut f: F, range: Range<u64>) -> io::Result<Option<T>>
        where F: FnMut(u64, &[u8]) -> Result<Option<T>, NextTarget>
    {
        dbg!(&range);
        assert!(range.start <= range.end);
        let midpoint = range.start.midpoint(range.end);

        self.fd.seek(SeekFrom::Start(midpoint))?;
        let mut blobs = Blobs::new(&mut self.fd)?;

        loop {
            match blobs.next()? {
                None => break Ok(None),
                Some((offset, blob)) => {
                    if offset < range.end {
                        match f(offset, blob) {
                            Ok(Some(r)) => break Ok(Some(r)),
                            Ok(None) => break Ok(None),
                            Err(NextTarget::Next) => {},
                            Err(NextTarget::Left) => break self.binary_search_in_range_by(f, range.start .. midpoint),
                            Err(NextTarget::Right) => break self.binary_search_in_range_by(f, midpoint .. range.end),
                        }
                    } else {
                        todo!("return Ok(None), right?")
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;

    #[test]
    fn write_blob() -> io::Result<()> {
        let b = Cursor::new(vec![]);
        let mut b = Breccia::open(b)?;

        dbg!(offset_to_marker(8).to_le_bytes());
        dbg!(&b);
        dbg!(b.write_blob(&[0xff - 8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 42])?);
        dbg!(b.write_blob(b"hello world!!!!!!!!")?);
        println!("{:?}", b.fd);

        let mut blobs = b.blobs()?;

        dbg!(&blobs);
        dbg!(blobs.next()?);
        dbg!(blobs.next()?);
        dbg!(blobs.next()?);
        dbg!(blobs.next()?);
        dbg!(blobs.next()?);
        dbg!(blobs.next()?);
        dbg!(blobs.next()?);
        dbg!(&blobs);

        Ok(())
    }

    #[test]
    fn binary_search() -> io::Result<()> {
        let b = Cursor::new(vec![]);
        let mut b = Breccia::open(b)?;

        for n in 0u32 .. 100 {
            b.write_blob(&n.to_le_bytes())?;
        }

        let target: u32 = 42;
        dbg!(b.binary_search_by(|offset, blob| {
            if let Ok(blob) = dbg!(blob).try_into() {
                let n = u32::from_le_bytes(blob);

                match target.cmp(&n) {
                    Ordering::Less => Err(NextTarget::Left),
                    Ordering::Greater => Err(NextTarget::Right),
                    Ordering::Equal => Ok(Some(target)),
                }
            } else {
                Err(NextTarget::Next)
            }
        })?);

        Ok(())
    }
}
