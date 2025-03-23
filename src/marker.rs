use super::{Offset, Header};

/// A marker used to distinguish blob boundaries, and determine blob length.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Marker([u8; Self::SIZE]);

impl Marker {
    /// The size of a serialized marker.
    pub const SIZE: usize = size_of::<u64>();

    /// The offset, in bits, that the padding length is encoded in.
    const PADDING_LEN_OFFSET: u32 = u64::BITS - 3;

    /// Creates a new `Marker` from an `Offset` and a padding length.
    pub const fn new<H>(offset: Offset<H>, padding_len: usize) -> Self {
        Marker(((offset.raw & !(0b111 << Self::PADDING_LEN_OFFSET)) | ((padding_len as u64) << Self::PADDING_LEN_OFFSET)).to_le_bytes())
    }

    /// Returns the `Offset` this `Marker` represents.
    pub const fn offset<H>(self) -> Offset<H> {
        Offset::new(u64::from_le_bytes(self.0) & !(0b111 << Self::PADDING_LEN_OFFSET))
    }

    /// Returns the padding length encoded in this `Marker`.
    pub const fn padding_len(self) -> usize {
        (u64::from_le_bytes(self.0) >> Self::PADDING_LEN_OFFSET) as usize
    }

    /// Converts this `Marker` to the raw, serialized, byte format.
    pub const fn to_bytes(self) -> [u8; Self::SIZE] {
        self.0
    }
}

impl From<[u8; Self::SIZE]> for Marker {
    fn from(inner: [u8; Self::SIZE]) -> Self {
        Self(inner)
    }
}

impl From<&[u8; Self::SIZE]> for Marker {
    fn from(inner: &[u8; Self::SIZE]) -> Self {
        Self(*inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let marker = Marker::new(Offset::<()>::new(42), 0b101);
        assert_eq!(marker.offset(), Offset::<()>::new(42));
        assert_eq!(marker.padding_len(), 0b101);
        assert_eq!(Marker::new(Offset::<()>::new(42), 0b101).0,
                  [42,0,0,0,0,0,0,0b101_0_0000]);
    }
}
