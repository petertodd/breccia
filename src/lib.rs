#![feature(slice_as_chunks)]

#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::ops::Range;
use std::fmt;
use std::marker::PhantomData;

mod offset;
pub use offset::Offset;

mod header;
pub use header::Header;

mod marker;
pub use marker::Marker;

mod buffer;
use self::buffer::Buffer;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Breccia<F, H = ()> {
    header: H,
    fd: F,
    clean: bool,
}

impl<F: Write + Seek, H: Header> Breccia<F, H> {
    pub fn create(mut fd: F, header: H) -> io::Result<Self> {
        fd.seek(SeekFrom::Start(0))?;
        fd.write_all(H::MAGIC)?;

        let mut header_bytes = vec![0u8; H::SIZE];
        header.serialize(&mut header_bytes);
        fd.write_all(&header_bytes)?;

        Ok(Self {
            header,
            fd,
            clean: true,
        })
    }

    pub fn write_blob(&mut self, blob: &[u8]) -> io::Result<Offset<H>> {
        // FIXME: we should actually just keep track of what the offset should be, and error out if
        // the file is changed underneath us

        let end_offset = self.fd.seek(SeekFrom::End(0))?;

        let blob_offset = Offset::<H>::try_from_file_offset(end_offset).expect("TODO");

        if self.clean != true {
            todo!()
        }

        // determine how much padding we need
        let mut padding = 0;
        'outer: loop {
            // Note that the last chunk can't actually collide except for truly enormous files.
            // FIXME: should we use 0 padding so we can actually test this?
            let (chunks, tail) = blob.as_chunks::<8>();
            let last_chunk = if tail.len() > 0 {
                let mut b = [0xfe; 8];
                (&mut b[0 .. tail.len()]).copy_from_slice(tail);
                Some(b)
            } else {
                None
            };

            let chunks = chunks.into_iter().chain(last_chunk.as_ref());
            for (i, chunk) in chunks.enumerate() {
                let possible_marker = Marker::from(chunk);
                if blob_offset.offset(padding).offset(i) == possible_marker.offset() {
                    padding += 1;
                    continue 'outer
                }
            }
            break
        }

        for _ in 0 .. padding {
            self.fd.write(&[0xff; 8])?;
        }
        let blob_offset = blob_offset.offset(padding);

        self.fd.write(blob)?;

        let end_padding_len = blob.len().next_multiple_of(8) - blob.len();
        let end_padding = &[0xfe; 7][0 .. end_padding_len];
        self.fd.write(end_padding)?;

        let end_marker_offset = blob_offset.offset((blob.len() + end_padding.len()) / 8);
        let marker = Marker::new(end_marker_offset, end_padding.len());
        self.fd.write(&marker.to_bytes())?;

        Ok(blob_offset)
    }
}

impl<F: Read + Seek, H: Header> Breccia<F, H> {
    pub fn blobs<'a>(&'a mut self) -> io::Result<Blobs<'a, F, H>> {
        self.fd.seek(SeekFrom::Start((H::MAGIC.len() + H::SIZE) as u64))?;
        Blobs::new(&mut self.fd)
    }
}

#[derive(Debug)]
pub struct Blobs<'a, B, H> {
    _marker: PhantomData<&'a H>,
    buffer: Buffer<&'a mut B>,
}

impl<'a, B: Read + Seek, H: Header> Blobs<'a, B, H> {
    fn new(fd: &'a mut B) -> io::Result<Self> {
        let offset = fd.seek(SeekFrom::Current(0))?;
        Ok(Self {
            _marker: PhantomData,
            buffer: Buffer::new_with_offset(fd, offset)
        })
    }

    pub fn next(&mut self) -> io::Result<Option<(Offset<H>, &[u8])>> {
        let mut blob_len: usize = 0;

        loop {
            if self.buffer.buffer().len() < blob_len + 8 {
                // FIXME: limit maximum blob size
                self.buffer.fill(512)?;
            };

            if let Some(potential_marker) = self.buffer.buffer().get(blob_len .. blob_len + 8) {
                let potential_marker: &[u8; 8] = potential_marker.try_into().unwrap();
                let potential_marker = Marker::from(potential_marker);

                let marker_file_offset = self.buffer.offset() + (blob_len as u64);
                let offset = Offset::<H>::try_from_file_offset(marker_file_offset).unwrap();
                if dbg!(potential_marker.offset()) == dbg!(offset) {
                    let offset = Offset::<H>::try_from_file_offset(self.buffer.offset()).unwrap();
                    let blob_with_marker = self.buffer.consume(blob_len + 8);
                    let blob = &blob_with_marker[0 .. blob_len - potential_marker.padding_len()];
                    break Ok(Some((offset, blob)))
                } else {
                    blob_len += 8;
                }
            } else {
                // We reached the end of the file without finding a valid marker
                break Ok(None)
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
        let mut buf = Cursor::new(vec![]);
        let mut b = Breccia::create(&mut buf, ())?;

        let offset = b.write_blob(&[])?;
        assert_eq!(offset.raw, 0);

        let offset = b.write_blob(&[])?;
        assert_eq!(offset.raw, 1);

        let offset = b.write_blob(&[42])?;
        assert_eq!(offset.raw, 2);

        assert_eq!(buf.get_ref(),
            &[0,0,0,0,0,0,0,0,
              1,0,0,0,0,0,0,0,
              42,0xfe,0xfe,0xfe,0xfe,0xfe,0xfe,0xfe,
              3,0,0,0,0,0,0,0b111_0_0000]);

        Ok(())
    }

    #[test]
    fn write_colliding_blob() -> io::Result<()> {
        let mut buf = Cursor::new(vec![]);
        let mut b = Breccia::create(&mut buf, ())?;

        let offset = b.write_blob(&[0,0,0,0,0,0,0,0b111_0_0000])?;
        assert_eq!(offset.raw, 1);

        assert_eq!(buf.get_ref(),
            &[255,255,255,255,255,255,255,255,
              0,0,0,0,0,0,0,0b111_0_0000,
              2,0,0,0,0,0,0,0]);

        Ok(())
    }

    #[test]
    fn blobs() -> io::Result<()> {
        let mut buf = Cursor::new(vec![]);
        let mut b = Breccia::create(&mut buf, ())?;

        let mut blobs = b.blobs()?;
        assert_eq!(blobs.next()?, None);
        assert_eq!(blobs.next()?, None);

        b.write_blob(&[])?;

        let mut blobs = b.blobs()?;
        assert_eq!(blobs.next()?, Some((Offset::new(0), &[][..])));
        assert_eq!(blobs.next()?, None);
        assert_eq!(blobs.next()?, None);

        b.write_blob(b"hello world!")?;
        let mut blobs = b.blobs()?;
        assert_eq!(blobs.next()?, Some((Offset::new(0), &[][..])));
        assert_eq!(blobs.next()?, Some((Offset::new(1), &b"hello world!"[..])));
        assert_eq!(blobs.next()?, None);
        assert_eq!(blobs.next()?, None);

        Ok(())
    }
}
