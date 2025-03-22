use std::collections::VecDeque;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::fmt;

#[derive(Clone)]
pub struct Buffer<R> {
    /// The reader we're wrapping
    inner: R,

    /// The seek offset within R that represents the *start* of the buffer
    offset: u64,

    // How many bytes of buf have been used at the beginning
    start: usize,
    end: usize,
    buf: Vec<u8>,
}

impl<R: fmt::Debug> fmt::Debug for Buffer<R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Blobs")
         .field("inner", &self.inner)
         .field("offset", &self.offset)
         .field("start", &self.start)
         .field("end", &self.end)
         .field("buffer", &self.buffer())
         .finish()
    }
}


impl<R> Buffer<R> {
    pub fn new(inner: R) -> Self {
        Self::new_with_offset(inner, 0)
    }

    pub fn new_with_offset(inner: R, offset: u64) -> Self {
        Self {
            inner,
            offset,
            start: 0,
            end: 0,
            buf: vec![],
        }
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buf[self.start .. self.end]
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn consume(&mut self, amt: usize) -> &[u8] {
        assert!(self.start + amt <= self.end);
        let old_start = self.start;
        self.start += amt;
        self.offset += amt as u64;
        &self.buf[old_start .. self.start]
    }
}

impl<R: Read> Buffer<R> {
    /// Fills the buffer with up to `additional` bytes.
    pub fn fill(&mut self, additional: usize) -> io::Result<()> {
        // If the additional bytes would overflow the buffer, move all unused bytes to the front.
        if self.end + additional > self.buf.len() {
            self.buf.copy_within(self.start .. self.end, 0);
            self.end -= self.start;
            self.start = 0;
        }

        // If we still don't have enough space, expand the buffer itself.
        if self.end + additional > self.buf.len() {
            self.buf.resize((self.end + additional).next_power_of_two(), 0);
        }

        let target_end = self.end + additional;
        while self.end <= target_end {
            let unused = &mut self.buf[self.end .. target_end];
            match self.inner.read(unused) {
                Ok(0) => break,
                Ok(amt) => {
                    self.end += amt;
                },
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {},
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(())
    }
}

impl<R: Seek> Buffer<R> {
    /// Seek to a new position.
    pub fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        // TODO: avoid flushing the buffer
        self.offset = self.inner.seek(pos)?;
        self.start = 0;
        self.end = 0;
        Ok(self.offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;

    #[test]
    fn empty_buffer() {
        let inner = Cursor::new(vec![0u8; 0]);
        let mut buffer = Buffer::new(inner);

        assert_eq!(buffer.buffer(), &[]);
        assert_eq!(buffer.offset(), 0);
        assert_eq!(buffer.consume(0), &[]);
        assert_eq!(buffer.offset(), 0);
        assert_eq!(buffer.buffer(), &[]);

        buffer.fill(100).unwrap();
        assert_eq!(buffer.buffer(), &[]);
        assert_eq!(buffer.offset(), 0);
    }

    #[test]
    fn nonempty_buffer() {
        let inner = Cursor::new(vec![0xde, 0xad, 0xbe, 0xef]);
        let mut buffer = Buffer::new(inner);

        buffer.fill(1).unwrap();

        assert_eq!(buffer.buffer(), &[0xde]);
        assert_eq!(buffer.consume(1), &[0xde]);
        assert_eq!(buffer.offset(), 1);

        buffer.fill(100).unwrap();
        assert_eq!(buffer.buffer(), &[0xad, 0xbe, 0xef]);
        assert_eq!(buffer.consume(2), &[0xad, 0xbe]);
        assert_eq!(buffer.offset(), 3);

        buffer.fill(100).unwrap();
        buffer.fill(100).unwrap();
        buffer.fill(100).unwrap();
        assert_eq!(buffer.buffer(), &[0xef]);
        assert_eq!(buffer.offset(), 3);
    }
}
