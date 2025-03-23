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

        fd.write_all(&Marker::new(Offset::<H>::new(0), 0).to_bytes())?;

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

        let blob_offset = Offset::<H>::try_from_file_offset(end_offset - Marker::SIZE as u64).expect("TODO");

        if self.clean != true {
            todo!()
        }

        // determine how much padding we need
        let mut padding = 0;
        'outer: loop {
            // Note that the last chunk can't actually collide except for truly enormous files.
            // FIXME: should we use 0 padding so we can actually test this?
            let (chunks, tail) = blob.as_chunks::<{Marker::SIZE}>();
            let last_chunk = if tail.len() > 0 {
                let mut b = [0xfe; Marker::SIZE];
                (&mut b[0 .. tail.len()]).copy_from_slice(tail);
                Some(b)
            } else {
                None
            };

            let chunks = chunks.into_iter().chain(last_chunk.as_ref());
            for (i, chunk) in chunks.enumerate() {
                let possible_marker = Marker::from(chunk);
                if blob_offset.offset(1).offset(padding).offset(i) == possible_marker.offset() {
                    padding += 1;
                    continue 'outer
                }
            }
            break
        }

        for i in 0 .. padding {
            let pad_offset = blob_offset.offset(1 + i);
            let marker = Marker::new(pad_offset, Marker::SIZE - 1);
            self.fd.write(&marker.to_bytes())?;
        }
        let blob_offset = blob_offset.offset(padding);

        self.fd.write(blob)?;

        let end_padding_len = blob.len().next_multiple_of(Marker::SIZE) - blob.len();
        let end_padding = &[0xfe; Marker::SIZE - 1][0 .. end_padding_len];
        self.fd.write(end_padding)?;

        let end_marker_offset = blob_offset.offset(1 + ((blob.len() + end_padding.len()) / Marker::SIZE));
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

pub struct Blobs<'a, B, H> {
    _marker: PhantomData<&'a H>,
    buffer: Buffer<&'a mut B>,
}

impl<B: fmt::Debug, H> fmt::Debug for Blobs<'_, B, H> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Blobs")
            .field("buffer", &self.buffer)
            .finish()
    }
}

impl<'a, B: Read + Seek, H: Header> Blobs<'a, B, H> {
    fn new(fd: &'a mut B) -> io::Result<Self> {
        let offset = fd.seek(SeekFrom::Current(0))?;

        // FIXME: what should we do if we're unaligned?
        let mut offset = Offset::<H>::try_from_file_offset(offset).expect("TODO");

        let mut buffer = Buffer::new_with_offset(fd, offset.to_file_offset());

        // FIXME: this could be more efficient
        loop {
            buffer.fill(Marker::SIZE)?;

            if let Some(potential_marker) = buffer.buffer().get(0 .. Marker::SIZE) {
                let potential_marker: &[u8; Marker::SIZE] = potential_marker.try_into().unwrap();
                let potential_marker = Marker::from(potential_marker);

                if potential_marker.offset() == offset {
                    buffer.consume(Marker::SIZE);
                    break
                }
                buffer.consume(Marker::SIZE);
                offset = offset.offset(1);
            } else {
                // We did *not* get a potential marker, which indicates we're at end-of-file
                break
            }
        }

        Ok(Self {
            buffer,
            _marker: PhantomData,
        })
    }

    pub fn next(&mut self) -> io::Result<Option<(Offset<H>, &[u8])>> {
        let mut blob_len: usize = 0;
        loop {
            if self.buffer.buffer().len() < blob_len + Marker::SIZE {
                // FIXME: limit maximum blob size
                self.buffer.fill(512)?;
            };

            if let Some(potential_marker) = self.buffer.buffer().get(blob_len .. blob_len + Marker::SIZE) {
                // FIXME: handle short reads
                let potential_marker: &[u8; Marker::SIZE] = potential_marker.try_into().unwrap();
                let potential_marker = Marker::from(potential_marker);

                let marker_file_offset = self.buffer.offset() + (blob_len as u64);
                let offset = Offset::<H>::try_from_file_offset(marker_file_offset).unwrap();
                if potential_marker.offset() == offset {
                    let offset = Offset::<H>::try_from_file_offset(self.buffer.offset() - (Marker::SIZE as u64)).unwrap();

                    if blob_len < potential_marker.padding_len() {
                        // Not a valid blob, so this marker was padding. Skip it.
                        //
                        // FIXME: what should we do if it's a differnt size? that'd indicate
                        // (very unlikely) corruption
                        assert_eq!(potential_marker.padding_len(), Marker::SIZE - 1);
                        self.buffer.consume(Marker::SIZE);
                    } else {
                        let blob_with_marker = self.buffer.consume(blob_len + Marker::SIZE);
                        let blob = &blob_with_marker[0 .. blob_len - potential_marker.padding_len()];
                        break Ok(Some((offset, blob)))
                    }
                } else {
                    blob_len += Marker::SIZE;
                }
            } else {
                // We reached the end of the file without finding a valid marker
                break Ok(None)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Search {
    /// The target should be to the left of this blob.
    Left,

    /// The target should be to the right of this blob.
    Right,

    /// This blob gave no useful information; try the next one.
    Next,
}

impl<B: Read + Seek, H: Header> Breccia<B, H> {
    fn last_offset(&mut self) -> io::Result<Offset<H>> {
        if !self.clean {
            todo!()
        }

        let end_file_offset = self.fd.seek(SeekFrom::End(-(Marker::SIZE as i64)))?;
        Ok(Offset::try_from_file_offset(end_file_offset).expect("TODO"))
    }

    pub fn binary_search<F, R>(&mut self, f: F) -> io::Result<Option<R>>
        where F: FnMut(Offset<H>, &[u8]) -> Result<Option<R>, Search>
    {
        let last_offset = self.last_offset()?;
        self.binary_search_in_range(f, Offset::new(0) .. last_offset)
    }

    pub fn binary_search_in_range<F, R>(&mut self, mut f: F, range: Range<Offset<H>>) -> io::Result<Option<R>>
        where F: FnMut(Offset<H>, &[u8]) -> Result<Option<R>, Search>
    {
        let midpoint = range.start.midpoint(range.end);

        self.fd.seek(SeekFrom::Start(midpoint.to_file_offset()))?;
        let mut blobs = Blobs::<B, H>::new(&mut self.fd)?;

        loop {
            // FIXME: handle a degenerate range
            if let Some((offset, blob)) = blobs.next()? {
                match f(offset, blob) {
                    Ok(Some(r)) => break Ok(Some(r)),
                    Ok(None) => break Ok(None),
                    Err(Search::Next) => {
                        continue
                    },
                    Err(Search::Right) => break self.binary_search_in_range(f, midpoint .. range.end),
                    Err(Search::Left) => break self.binary_search_in_range(f, range.start .. midpoint),
                }
            } else {
                // We've search the entire range, starting from the midpoint, without finding the
                // target.
                //
                // If the left side is non-empty, we still need to search it.
                if range.start != midpoint {
                    break self.binary_search_in_range(f, range.start .. midpoint)
                } else {
                    break Ok(None)
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
              2,0,0,0,0,0,0,0,
              42,0xfe,0xfe,0xfe,0xfe,0xfe,0xfe,0xfe,
              4,0,0,0,0,0,0,0b111_0_0000]);

        Ok(())
    }

    #[test]
    fn write_colliding_blob() -> io::Result<()> {
        let mut buf = Cursor::new(vec![]);
        let mut b = Breccia::create(&mut buf, ())?;

        let offset = b.write_blob(&[1,0,0,0,0,0,0,0b1110_0000])?;
        assert_eq!(offset.raw, 1);


        // validate that the padding is actually skipped over
        let mut blobs = b.blobs()?;
        assert_eq!(blobs.next()?,
                   Some((Offset::new(1), &[1,0,0,0,0,0,0,0b1110_0000][..])));
        assert_eq!(blobs.next()?, None);

        assert_eq!(buf.get_ref(),
            &[0,0,0,0,0,0,0,0,
              1,0,0,0,0,0,0,0b1110_0000,
              1,0,0,0,0,0,0,0b1110_0000,
              3,0,0,0,0,0,0,0]);

        Ok(())
    }

    #[test]
    fn blobs() -> io::Result<()> {
        let mut buf = Cursor::new(vec![]);
        let mut b = Breccia::create(&mut buf, ())?;

        let mut blobs = b.blobs()?;
        assert_eq!(blobs.next()?, None);
        assert_eq!(blobs.next()?, None);

        assert_eq!(b.write_blob(&[])?,
                   Offset::new(0));

        let mut blobs = b.blobs()?;
        assert_eq!(blobs.next()?, Some((Offset::new(0), &[][..])));
        assert_eq!(blobs.next()?, None);
        assert_eq!(blobs.next()?, None);

        assert_eq!(b.write_blob(b"hello world!")?,
                   Offset::new(1));

        let mut blobs = b.blobs()?;
        assert_eq!(blobs.next()?, Some((Offset::new(0), &[][..])));
        assert_eq!(blobs.next()?, Some((Offset::new(1), &b"hello world!"[..])));
        assert_eq!(blobs.next()?, None);
        assert_eq!(blobs.next()?, None);

        Ok(())
    }

    #[test]
    fn binary_search_on_empty_blobs() -> io::Result<()> {
        let mut buf = Cursor::new(vec![]);
        let mut b = Breccia::create(&mut buf, ())?;

        assert_eq!(b.binary_search(|_offset, _blob| panic!("should not be called"))?,
                   None::<()>);

        b.write_blob(&[])?;
        assert_eq!(b.binary_search(|offset, blob| {
            assert_eq!(blob, &[]);
            Ok(Some(offset))
        })?,
        Some(Offset::new(0)));

        b.write_blob(&[])?;
        assert_eq!(b.binary_search(|offset, blob| {
            assert_eq!(blob, &[]);
            Ok(Some(offset))
        })?,
        Some(Offset::new(1)));

        Ok(())
    }

    #[test]
    fn binary_search_for_ints() -> io::Result<()> {
        let mut buf = Cursor::new(vec![]);
        let mut b = Breccia::create(&mut buf, ())?;

        let mut expected_offsets = vec![];
        for i in 0 .. 100u32 {
            let offset = b.write_blob(&i.to_le_bytes())?;
            expected_offsets.push((i, offset));
        }

        for (i, expected_offset) in &expected_offsets {
            assert_eq!(b.binary_search(|offset, blob| {
                let blob = blob.try_into().unwrap();
                let found = u32::from_le_bytes(blob);
                if *i == found {
                    Ok(Some(offset))
                } else if *i < found {
                    Err(Search::Left)
                } else { // if i > found
                    Err(Search::Right)
                }
            })?,
            Some(*expected_offset));
        }

        Ok(())
    }
}
