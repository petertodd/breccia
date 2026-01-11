//! Single-file, append-only, blob storage with the option of efficient random access and search.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write, Seek, SeekFrom};
use std::ops::{self, Range};
use std::path::Path;
use std::ptr;

use memmap2::Mmap;

mod offset;
pub use offset::Offset;

mod header;
use header::HeaderExt;
pub use header::Header;

mod marker;
use marker::{Marker, State::Clean, State::Dirty};

const _: () = {
    if size_of::<usize>() != 8 {
        panic!("only 64-bit platforms are supported")
    }
};

/// The main interface to a breccia blob storage.
#[derive(Debug)]
pub struct Breccia<H = ()> {
    header: H,
    map: Mmap,
    markers: *const [Marker],
    fd: File,
}

// SAFETY: the markers pointer is only ever used to create a &[Marker] slice
unsafe impl<H> Sync for Breccia<H> {
}

// SAFETY: the markers pointer is only ever used to create a &[Marker] slice
unsafe impl<H> Send for Breccia<H> {
}

/// A mutable `Breccia`, that can have blobs written to it.
#[derive(Debug)]
pub struct BrecciaMut<H = ()> {
    inner: Breccia<H>,
}

impl<H> ops::Deref for BrecciaMut<H> {
    type Target = Breccia<H>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<H> ops::DerefMut for BrecciaMut<H> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<H> Breccia<H> {
    /// Returns a reference to the header.
    pub fn header(&self) -> &H {
        &self.header
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetBlobError {
    /// The offset is beyond the range of the breccia.
    OutOfRange,

    /// There is no blob starting at the provided offset; the offset points to either the middle of
    /// a different blob, or padding data.
    Unaligned,
}


impl<H: Header> Breccia<H> {
    fn try_map_to_markers_slice(map: &Mmap) -> io::Result<*const [Marker]> {
        let marker_slice = map.get(H::SIZE_WITH_PADDING ..)
                              .unwrap(); // TODO: actually handle this error

        Ok(ptr::slice_from_raw_parts(
            marker_slice.as_ptr() as *const Marker,
            marker_slice.len() / size_of::<Marker>()
        ))
    }

    /// Opens an existing breccia file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Self::open_file(File::open(path)?)
    }

    fn open_file(mut fd: File) -> io::Result<Self> {
        fd.seek(SeekFrom::Start(0))?;

        let mut actual_magic = vec![0u8; H::MAGIC.len()];
        fd.read_exact(&mut actual_magic)?;

        if actual_magic != H::MAGIC {
            return Err(io::Error::other("bad magic"));
        }

        let mut header_bytes = vec![0u8; H::SIZE_WITH_PADDING - H::MAGIC.len()];
        fd.read_exact(&mut header_bytes)?;
        let header = H::deserialize(&header_bytes).map_err(io::Error::other)?;

        let padding = &mut [0u8; size_of::<Marker>()][0 .. H::PADDING_SIZE];
        fd.read_exact(padding)?;

        // FIXME: validate that padding bytes are all zero

        // FIXME: check if last blob was written cleanly

        let map = unsafe {
            Mmap::map(&fd)?
        };

        Ok(Self {
            header,
            markers: Self::try_map_to_markers_slice(&map)?,
            map,
            fd,
        })
    }

    fn map(&self) -> &[Marker] {
        unsafe {
            &*self.markers
        }
    }

    /// Gets the blob at an offset.
    pub fn get_blob(&mut self, offset: Offset<H>) -> Result<&[u8], GetBlobError> {
        let first_mark = self.map().get(offset.raw)
                                   .ok_or(GetBlobError::OutOfRange)?;

        if first_mark.offset() == offset {
            let end_offset = offset + 1;

            while let Some(potential_mark) = self.map().get(end_offset.raw) {
                if potential_mark.is_padding() {
                    todo!("blob is invalid");
                } else if potential_mark.offset() == end_offset {
                    // We're at the end of the blob.
                    let blob = Marker::slice_to_bytes(&self.map()[offset.raw + 1 .. end_offset.raw]);

                    if let Some(blob_len) = blob.len().checked_sub(potential_mark.padding_len()) {
                        let (blob, _padding) = blob.split_at(blob_len);
                        return Ok(blob);
                    } else {
                        todo!("handle incorrect padding length")
                    }
                }
            }
            todo!("last blob not fully written")
        } else {
            Err(GetBlobError::Unaligned)
        }
    }

    /// Reloads the `Breccia` to reflect newly written blobs.
    pub fn reload(&mut self) -> io::Result<()> {
        // FIXME: check if last blob was written cleanly
        let new_map = unsafe {
            Mmap::map(&self.fd)?
        };

        let new_markers = Self::try_map_to_markers_slice(&new_map)?;

        self.map = new_map;
        self.markers = new_markers;

        Ok(())
    }
}

impl<H: Header> BrecciaMut<H> {
    /// Creates a new breccia file.
    pub fn create<P: AsRef<Path>>(path: P, header: H) -> io::Result<Self> {
        let fd = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(path)?;

        Self::create_from_file(fd, header)
    }

    /// Opens an existing breccia file.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let fd = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(path)?;

        Ok(Self {
            inner: Breccia::open_file(fd)?
        })
    }

    /// Creates a new breccia from a `File`.
    pub fn create_from_file(mut fd: File, header: H) -> io::Result<Self> {
        fd.seek(SeekFrom::Start(0))?;
        fd.write_all(H::MAGIC)?;

        let mut header_bytes = vec![0u8; H::SERIALIZED_SIZE];
        header.serialize(&mut header_bytes);
        fd.write_all(&header_bytes)?;

        let mut padding = &[0; size_of::<Marker>()][0 .. H::PADDING_SIZE];
        fd.write_all(&mut padding)?;

        fd.write_all(&Marker::new(Offset::<H>::new(0), 0, Clean).to_bytes())?;

        Ok(Self {
            inner: Breccia::open_file(fd)?
        })
    }
}

impl<H: Header> BrecciaMut<H> {
    /// Writes a new blob to the breccia.
    ///
    /// Returns the `Offset` of the written blob.
    pub fn write_blob(&mut self, blob: &[u8]) -> io::Result<Offset<H>> {
        let mut batch = self.start_batch()?;
        let offset = batch.write_blob(blob)?;
        batch.commit()?;
        Ok(offset)
    }

    /// Starts a new `Batch` of blobs.
    pub fn start_batch<'a>(&'a mut self) -> io::Result<Batch<'a, H>> {
        Batch::new(self)
    }
}


impl<H: Header> Breccia<H> {
    /// Returns an iterator over all blobs stored.
    pub fn blobs<'a>(&'a self) -> Blobs<'a, H> {
        Blobs::new(self.map(), Offset::new(0))
    }
}

/// An iterator over the blobs (and their offsets) in a `Breccia`.
pub struct Blobs<'a, H> {
    /// The map of marker words this iterator is acting over
    map: &'a [Marker],

    /// The offset of the first marker in the map.
    ///
    /// Thus, `&self.map[0] + self.offset` gives the true offset of the first marker in the map.
    offset: Offset<H>,
}

impl<H> fmt::Debug for Blobs<'_, H> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Blobs")
            .field("offset", &self.offset)
            .field("map", &self.map)
            .finish()
    }
}

impl<'a, H: Header> Blobs<'a, H> {
    fn new(mut map: &'a [Marker], mut offset: Offset<H>) -> Self {
        // Find the first marker
        while let Some((potential_marker, rest)) = map.split_first() {
            if potential_marker.offset() == offset {
                break
            } else {
                map = rest;
                offset += 1;
            }
        }

        Self {
            offset,
            map,
        }
    }
}

impl<'a, H: Header> Iterator for Blobs<'a, H> {
    type Item = (Offset<H>, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let mut blob_len_words = 0;
        while let Some(potential_marker) = self.map.get(1 + blob_len_words) {
            let end_offset = self.offset.offset(blob_len_words + 1);
            if potential_marker.offset() == end_offset {
                let blob = Marker::slice_to_bytes(&self.map[1 .. 1 + blob_len_words]);

                if let Some(blob_len) = blob.len().checked_sub(potential_marker.padding_len()) {
                    let (blob, _padding) = blob.split_at(blob_len);
                    let offset = self.offset;
                    self.offset = self.offset.offset(blob_len_words + 1);
                    self.map = &self.map[blob_len_words + 1 .. ];
                    return Some((offset, blob))
                } else {
                    // TODO: think more about how padding should be treated
                    self.map = &self.map[1 .. ];
                    self.offset = self.offset.offset(1);
                    continue
                }
            }
            blob_len_words += 1;
        }
        None
    }
}

impl<H: Header> std::iter::FusedIterator for Blobs<'_, H> {
}

impl<'a, H: Header> std::iter::DoubleEndedIterator for Blobs<'a, H> {
    fn next_back(&mut self) -> Option<Self::Item> {
        // Consume padding
        loop {
            if let &[.., maybe_marker, maybe_padding] = self.map {
                if maybe_marker.offset() == self.offset + self.map.len() - 2 &&
                   maybe_padding.offset() == self.offset + self.map.len() - 1 &&
                   maybe_padding.is_padding()
                {
                    self.map = &self.map[.. self.map.len() - 1];
                } else {
                    break
                }
            } else {
                break
            }
        };

        if self.map.len() < 2 {
            return None
        }

        let end_marker = self.map.last().expect("map not empty");

        let mut start_offset = self.map.len().checked_sub(2).expect("map not empty");
        loop {
            // Check if start_offset is the beginning of a valid blob
            if self.map[start_offset].offset() == Offset::<H>::new(start_offset) {
                // It is, so check if the blob itself is valid.
                let blob = Marker::slice_to_bytes(&self.map[start_offset + 1 .. self.map.len() - 1]);
                if let Some(blob_len) = blob.len().checked_sub(end_marker.padding_len()) {
                    self.map = &self.map[0 .. start_offset + 1];
                    let (blob, _padding) = blob.split_at(blob_len);
                    return Some((Offset::new(start_offset), blob))
                } else {
                    unreachable!("we already consumed all padding")
                }
            }

            // Should be true, as we already consumed all the padding, and checked that there is at
            // least one valid blob.
            assert!(start_offset > 0);
            start_offset -= 1;
        }
    }
}

/// Enum used for binary searching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Search {
    /// The target should be to the left of this blob.
    Left,

    /// The target should be to the right of this blob.
    Right,

    /// This blob gave no useful information; try the next one.
    Next,
}

impl<H: Header> Breccia<H> {
    /// Binary searches for a given blob.
    pub fn binary_search<F, R>(&self, f: F) -> Option<R>
        where F: FnMut(Offset<H>, &[u8]) -> Result<Option<R>, Search>
    {
        let last_offset = Offset::new(self.map().len());
        self.binary_search_in_range(f, Offset::new(0) .. last_offset)
    }

    /// Binary searches for a given blob, within an `Offset` range.
    ///
    /// # Panics
    ///
    /// Panics if range `start > end`.
    pub fn binary_search_in_range<F, R>(&self, mut f: F, range: Range<Offset<H>>) -> Option<R>
        where F: FnMut(Offset<H>, &[u8]) -> Result<Option<R>, Search>
    {
        if range.start.raw > range.end.raw {
            panic!("range.start > range.end")
        }

        // TODO: should we panic if range.start is past the end of the map?

        // If the range is empty, we're done.
        if range.start == range.end {
            return None;
        }

        let midpoint = range.start.midpoint(range.end);
        let mut blobs = Blobs::<H>::new(&self.map()[midpoint.raw as usize ..], midpoint);

        loop {
            if let Some((offset, blob)) = blobs.next() {
                if offset < range.end {
                    match f(offset, blob) {
                        Ok(Some(r)) => break Some(r),
                        Ok(None) => break None,
                        Err(Search::Next) => {
                            continue
                        },
                        Err(Search::Right) => break self.binary_search_in_range(f, midpoint.offset(1) .. range.end),
                        Err(Search::Left) => break self.binary_search_in_range(f, range.start .. midpoint),
                    }
                }
            }

            // We've search the entire range, starting from the midpoint, without finding the
            // target.
            //
            // If the left side is non-empty, we still need to search it.
            if range.start != midpoint {
                break self.binary_search_in_range(f, range.start .. midpoint)
            } else {
                break None
            }
        }
    }
}

/// Batch writing.
#[derive(Debug)]
pub struct Batch<'a, H> {
    target: &'a mut BrecciaMut<H>,
    blob_offset: Offset<H>,
    fd: BufWriter<File>,
    pending_marker: Option<Marker>,
}

impl<'a, H: Header> Batch<'a, H> {
    fn new(target: &'a mut BrecciaMut<H>) -> io::Result<Self> {
        let mut fd = target.fd.try_clone()?;

        let blob_offset = fd.seek(SeekFrom::End(-(size_of::<Marker>() as i64)))?;
        let blob_offset = Offset::<H>::try_from_file_offset(blob_offset as u64)
                                      .expect("TODO: handle unclean file");

        let mut buf = [0u8; size_of::<Marker>()];
        fd.read_exact(&mut buf)?;

        let end_marker = Marker::from(buf);
        if end_marker.state() == Dirty {
            todo!("handle dirty file")
        }

        Ok(Self {
            target,
            blob_offset,
            fd: BufWriter::new(fd),
            pending_marker: None,
        })
    }

    /// Writes a blob.
    ///
    /// Returns the `Offset` of the newly-written blob.
    pub fn write_blob(&mut self, blob: &[u8]) -> io::Result<Offset<H>> {
        if let Some(pending_marker) = self.pending_marker.take() {
            self.fd.write_all(&pending_marker.to_bytes())?;
        }

        // determine how much padding we need
        let mut padding = 0;
        'outer: loop {
            // Note that the last chunk can't actually collide except for truly enormous files.
            // FIXME: should we use 0 padding so we can actually test this?
            let (chunks, tail) = blob.as_chunks::<{size_of::<Marker>()}>();
            let last_chunk = if tail.len() > 0 {
                let mut b = [0xfe; size_of::<Marker>()];
                (&mut b[0 .. tail.len()]).copy_from_slice(tail);
                Some(b)
            } else {
                None
            };

            let chunks = chunks.into_iter().chain(last_chunk.as_ref());
            for (i, chunk) in chunks.enumerate() {
                let possible_marker = Marker::from(chunk);
                if self.blob_offset.offset(1).offset(padding).offset(i) == possible_marker.offset() {
                    padding += 1;
                    continue 'outer
                }
            }
            break
        }

        for i in 0 .. padding {
            let pad_offset = self.blob_offset.offset(1 + i);
            let marker = Marker::new_padding(pad_offset);
            self.fd.write(&marker.to_bytes())?;
        }
        let blob_offset = self.blob_offset.offset(padding);

        self.fd.write(blob)?;

        let end_padding_len = blob.len().next_multiple_of(size_of::<Marker>()) - blob.len();
        let end_padding = &[0xfe; size_of::<Marker>() - 1][0 .. end_padding_len];
        self.fd.write(end_padding)?;

        let end_marker_offset = blob_offset.offset(1 + ((blob.len() + end_padding.len()) / size_of::<Marker>()));
        self.pending_marker = Some(Marker::new(end_marker_offset, end_padding.len(), Dirty));

        self.blob_offset += 1 + padding + ((blob.len() + end_padding.len()) / size_of::<Marker>());
        Ok(blob_offset)
    }

    /// Commits this batch of blobs.
    pub fn commit(mut self) -> io::Result<()> {
        if let Some(mut pending_marker) = self.pending_marker.take() {
            pending_marker.set_state(Clean);
            self.fd.write_all(&pending_marker.to_bytes())?;
        } else {
            panic!("no blobs written");
        }

        self.fd.flush()?;
        self.fd.get_mut().sync_all()?;
        self.target.reload()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempfile;

    use super::*;

    #[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
    struct TestHeader(u8);

    impl Header for TestHeader {
        const MAGIC: &[u8] = b"\x00";
        const SERIALIZED_SIZE: usize = 1;

        fn serialize(&self, dst: &mut [u8]) {
            dst[0] = self.0;
        }

        type DeserializeError = std::convert::Infallible;
        fn deserialize(src: &[u8]) -> Result<Self, Self::DeserializeError> {
            Ok(Self(src[0]))
        }
    }

    #[test]
    fn create() -> io::Result<()> {
        let breccia = BrecciaMut::create_from_file(tempfile()?, TestHeader(0x42))?;

        assert_eq!(&breccia.map[..],
                   b"\x00\x42\x00\x00\x00\x00\x00\x00\
                     \x00\x00\x00\x00\x00\x00\x00\x00");
        assert_eq!(&breccia.map(),
                   &[Marker(0)]);
        Ok(())
    }

    #[test]
    fn write_blob() -> io::Result<()> {
        let mut breccia = BrecciaMut::create_from_file(tempfile()?, TestHeader(0x42))?;

        let offset = breccia.write_blob(&[])?;
        assert_eq!(offset.raw, 0);

        let offset = breccia.write_blob(&[])?;
        assert_eq!(offset.raw, 1);

        let offset = breccia.write_blob(&[42])?;
        assert_eq!(offset.raw, 2);

        assert_eq!(&breccia.map[..],
            &[0,0x42,0,0,0,0,0,0,
              0,0,0,0,0,0,0,0,
              1,0,0,0,0,0,0,0,
              2,0,0,0,0,0,0,0,
              42,0xfe,0xfe,0xfe,0xfe,0xfe,0xfe,0xfe,
              4,0,0,0,0,0,0,0b111_0_0000]);

        let mut blobs = breccia.blobs();
        assert_eq!(blobs.next(),
                   Some((Offset::new(0), &[][..])));
        assert_eq!(blobs.next(),
                   Some((Offset::new(1), &[][..])));

        assert_eq!(blobs.next(),
                   Some((Offset::new(2), &[42][..])));
        assert_eq!(blobs.next(),
                   None);

        Ok(())
    }

    #[test]
    fn blobs_reversed() -> io::Result<()> {
        let mut breccia = BrecciaMut::create_from_file(tempfile()?, TestHeader(0x42))?;

        let mut blobs = breccia.blobs();
        assert_eq!(blobs.next_back(), None);
        assert_eq!(blobs.next_back(), None);

        let offset = breccia.write_blob(&[])?;
        assert_eq!(offset.raw, 0);

        assert_eq!(&breccia.map[..],
            &[0,0x42,0,0,0,0,0,0,
              0,0,0,0,0,0,0,0,
              1,0,0,0,0,0,0,0]);

        let mut blobs = breccia.blobs();
        assert_eq!(blobs.next_back(),
                   Some((Offset::new(0), &[][..])));
        assert_eq!(blobs.next_back(), None);
        assert_eq!(blobs.next_back(), None);

        breccia.write_blob(b"very blobby blob")?;

        let mut blobs = breccia.blobs();
        assert_eq!(blobs.next_back(),
                   Some((Offset::new(1), &b"very blobby blob"[..])));
        assert_eq!(blobs.next_back(),
                   Some((Offset::new(0), &[][..])));
        assert_eq!(blobs.next_back(), None);
        assert_eq!(blobs.next_back(), None);

        Ok(())
    }

    #[test]
    fn write_colliding_blob() -> io::Result<()> {
        let mut b = BrecciaMut::create_from_file(tempfile()?, TestHeader(0x42))?;

        let offset = b.write_blob(&[1,0,0,0,0,0,0,0b1110_0000])?;
        assert_eq!(offset.raw, 1);

        // validate that the padding is actually skipped over
        let mut blobs = b.blobs();
        assert_eq!(blobs.next(),
                   Some((Offset::new(1), &[1,0,0,0,0,0,0,0b1110_0000][..])));
        assert_eq!(blobs.next(), None);

        assert_eq!(&b.map[..],
            &[0,0x42,0,0,0,0,0,0,
              0,0,0,0,0,0,0,0,
              1,0,0,0,0,0,0,0b1111_0000,
              1,0,0,0,0,0,0,0b1110_0000,
              3,0,0,0,0,0,0,0]);

        Ok(())
    }

    #[test]
    fn binary_search_on_empty_blobs() -> io::Result<()> {
        let mut breccia = BrecciaMut::create_from_file(tempfile()?, TestHeader(0x42))?;

        assert_eq!(breccia.binary_search(|_offset, _blob| panic!("should not be called")),
                   None::<()>);

        breccia.write_blob(&[])?;
        assert_eq!(breccia.binary_search(|offset, blob| {
                assert_eq!(blob, &[]);
                Ok(Some(offset))
            }),
            Some(Offset::new(0))
        );

        breccia.write_blob(&[])?;
        assert_eq!(breccia.binary_search(|offset, blob| {
                assert_eq!(blob, &[]);
                Ok(Some(offset))
            }),
            Some(Offset::new(1))
        );

        Ok(())
    }

    #[test]
    fn binary_search_for_ints() -> io::Result<()> {
        let mut b = BrecciaMut::create_from_file(tempfile()?, TestHeader(0x42))?;

        let mut batch = b.start_batch()?;
        let mut expected_offsets = vec![];
        for i in 0 .. 100u32 {
            let offset = batch.write_blob(&i.to_le_bytes())?;
            expected_offsets.push((i, offset));
        }

        batch.commit()?;

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
            }),
            Some(*expected_offset));
        }

        Ok(())
    }

    #[test]
    fn binary_search_for_ints_with_padding() -> io::Result<()> {
        let mut b = BrecciaMut::create_from_file(tempfile()?, TestHeader(0x42))?;

        let mut batch = b.start_batch()?;
        let mut expected_offsets = vec![];
        for i in 0 .. 100u32 {
            let mut blob: Vec<u8> = vec![];
            blob.extend(&i.to_le_bytes());
            blob.extend(&[0u8; 100]);

            let offset = batch.write_blob(&i.to_le_bytes())?;
            expected_offsets.push((i, offset));
        }

        batch.commit()?;

        for (i, expected_offset) in &expected_offsets {
            let mut seen_offsets = std::collections::HashSet::new();
            assert_eq!(b.binary_search(|offset, blob| {
                // Make sure each offset is only tried once.
                assert!(seen_offsets.insert(offset));

                let found = u32::from_le_bytes(blob[0 .. 4].try_into().unwrap());
                if *i == found {
                    Ok(Some(offset))
                } else if *i < found {
                    Err(Search::Left)
                } else { // if i > found
                    Err(Search::Right)
                }
            }),
            Some(*expected_offset));
        }

        Ok(())
    }

    /// Make sure midpoints aren't tried twice.
    #[test]
    fn binary_search_on_large_singular_blob() -> io::Result<()> {
        let mut b = BrecciaMut::create_from_file(tempfile()?, TestHeader(0x42))?;

        b.write_blob(&[0u8; 1000])?;

        assert!(b.binary_search(|offset, blob| {
            assert_eq!(offset.raw, 0);
            assert_eq!(blob, &[0u8; 1000]);

            Result::<Option<()>, _>::Err(Search::Right)
        }).is_none());
        Ok(())
    }

    #[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
    struct BigHeader(u8);

    impl Header for BigHeader {
        const MAGIC: &[u8] = b"\x00\x01\x02\x03";
        const SERIALIZED_SIZE: usize = 1;

        fn serialize(&self, dst: &mut [u8]) {
            dst[0] = self.0;
        }

        type DeserializeError = std::convert::Infallible;
        fn deserialize(src: &[u8]) -> Result<Self, Self::DeserializeError> {
            Ok(Self(src[0]))
        }
    }

    #[test]
    fn create_big_header() -> io::Result<()> {
        let breccia = BrecciaMut::create_from_file(tempfile()?, BigHeader(0x42))?;

        assert_eq!(&breccia.map[..],
                   b"\x00\x01\x02\x03\x42\x00\x00\x00\
                     \x00\x00\x00\x00\x00\x00\x00\x00");
        assert_eq!(&breccia.map(),
                   &[Marker(0)]);
        Ok(())
    }
}
