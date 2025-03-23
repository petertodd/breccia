use std::cmp;
use std::fmt;
use std::marker::PhantomData;
use std::ops;

use super::Header;

pub struct Offset<H> {
    pub(crate) raw: u64,
    _marker: PhantomData<fn(&H) -> ()>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TryFromFileOffsetError {
    WithinHeader,
    Unaligned,
}

impl<H: Header> Offset<H> {
    pub fn new(raw: u64) -> Self {
        Self {
            raw,
            _marker: PhantomData,
        }
    }

    pub fn try_from_file_offset(file_offset: u64) -> Result<Self, TryFromFileOffsetError> {
        let offset = file_offset.checked_sub((H::MAGIC.len() + H::SIZE) as u64)
                                .ok_or(TryFromFileOffsetError::WithinHeader)?;

        if (offset & 0b111) != 0 {
            return Err(TryFromFileOffsetError::Unaligned);
        }

        Ok(Self::new(file_offset / 8))
    }

    pub(crate) fn offset(self, n: usize) -> Self {
        Self::new(self.raw + n as u64)
    }

    pub(crate) fn to_marker(self) -> [u8; 8] {
        self.raw.to_le_bytes()
    }
}

impl<H> fmt::Debug for Offset<H> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("Offset")
         .field(&self.raw)
         .finish()
    }
}

impl<H> Clone for Offset<H> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<H> Copy for Offset<H> {
}

impl<H> cmp::PartialEq for Offset<H> {
    fn eq(&self, rhs: &Self) -> bool {
        self.raw == rhs.raw
    }
}

impl<H> cmp::Eq for Offset<H> {
}

impl<H> cmp::PartialOrd for Offset<H> {
    fn partial_cmp(&self, rhs: &Self) -> Option<cmp::Ordering> {
        Some(self.raw.cmp(&rhs.raw))
    }
}

impl<H> cmp::Ord for Offset<H> {
    fn cmp(&self, rhs: &Self) -> cmp::Ordering {
        self.raw.cmp(&rhs.raw)
    }
}
