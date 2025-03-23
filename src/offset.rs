use std::cmp;
use std::fmt;
use std::marker::PhantomData;
use std::ops;

use super::Header;

/// An offset to a blob inside of a `Breccia`.
pub struct Offset<H> {
    pub(crate) raw: u64,
    _marker: PhantomData<fn(&H) -> ()>,
}

/// Error returns when conversion from a 'u64' file offset fails.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum TryFromFileOffsetError {
    /// The file offset was within the header.
    #[error("file offset within header")]
    WithinHeader,

    /// The offset was not aligned to a marker.
    #[error("file offset unaligned")]
    Unaligned,
}

impl<H> Offset<H> {
    /// Creates a new offset.
    ///
    /// The value is *not* a file offset.
    pub(crate) const fn new(raw: u64) -> Self {
        Self {
            raw,
            _marker: PhantomData,
        }
    }
}

impl<H: Header> Offset<H> {
    pub(crate) fn try_from_file_offset(file_offset: u64) -> Result<Self, TryFromFileOffsetError> {
        let offset = file_offset.checked_sub((H::MAGIC.len() + H::SIZE) as u64)
                                .ok_or(TryFromFileOffsetError::WithinHeader)?;

        if (offset & 0b111) != 0 {
            return Err(TryFromFileOffsetError::Unaligned);
        }

        Ok(Self::new(file_offset / 8))
    }

    pub(crate) fn to_file_offset(self) -> u64 {
        (self.raw * 8) + (H::MAGIC.len() + H::SIZE) as u64
    }

    pub(crate) fn offset(self, n: usize) -> Self {
        Self::new(self.raw + n as u64)
    }

    pub(crate) fn midpoint(self, rhs: Offset<H>) -> Self {
        Self::new(self.raw.midpoint(rhs.raw))
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
