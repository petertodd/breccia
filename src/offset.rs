use std::cmp;
use std::fmt;
use std::marker::PhantomData;
use std::ops;

use super::{Header, HeaderExt, Marker};

/// An offset to a blob inside of a `Breccia`.
pub struct Offset<H> {
    pub(crate) raw: usize,
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
    pub(crate) const fn new(raw: usize) -> Self {
        Self {
            raw,
            _marker: PhantomData,
        }
    }
}

impl<H: Header> Offset<H> {
    pub(crate) fn try_from_file_offset(file_offset: u64) -> Result<Self, TryFromFileOffsetError> {
        let file_offset = usize::try_from(file_offset).expect("u64 to usize conversion should be lossless");

        let offset = file_offset.checked_sub(H::SIZE_WITH_PADDING)
                                .ok_or(TryFromFileOffsetError::WithinHeader)?;

        if (offset % size_of::<Marker>()) != 0 {
            return Err(TryFromFileOffsetError::Unaligned);
        }

        Ok(Self::new(offset / size_of::<Marker>()))
    }

    pub(crate) fn offset(self, n: usize) -> Self {
        Self::new(self.raw + n)
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

impl<H> ops::AddAssign<usize> for Offset<H> {
    fn add_assign(&mut self, rhs: usize) {
        // TODO: check for overflow
        self.raw += rhs;
    }
}

impl<H> ops::Add<usize> for Offset<H> {
    type Output = Self;

    fn add(self, rhs: usize) -> Self {
        Offset::new(self.raw + rhs)
    }
}

impl<H> ops::Sub<usize> for Offset<H> {
    type Output = Self;

    fn sub(self, rhs: usize) -> Self {
        Offset::new(self.raw - rhs)
    }
}
