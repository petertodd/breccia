use super::Offset;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum State {
    Clean = 0,
    Dirty = 1,
}
use self::State::*;

/// A marker used to distinguish blob boundaries, and determine blob length.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Marker(pub(crate) usize);

impl Marker {
    /// The offset, in bits, that the padding length is encoded in.
    const PADDING_LEN_OFFSET: u32 = usize::BITS - 3;

    const STATE_BIT_OFFSET: u32 = usize::BITS - 4;

    /// Creates a new `Marker` from an `Offset` and a padding length.
    pub const fn new<H>(offset: Offset<H>, padding_len: usize, dirty: State) -> Self {
        let mut raw = offset.raw;
        raw |= padding_len << Self::PADDING_LEN_OFFSET;
        raw |= (dirty as usize) << Self::STATE_BIT_OFFSET;
        Marker(raw.to_le())
    }

    pub const fn new_padding<H>(offset: Offset<H>) -> Self {
        Self::new(offset, 7, Dirty)
    }

    pub fn is_padding(&self) -> bool {
        if self.padding_len() == 7 && self.state() == Dirty {
            true
        } else {
            false
        }
    }

    /// Returns the `Offset` this `Marker` represents.
    pub const fn offset<H>(self) -> Offset<H> {
        Offset::new(usize::from_le(self.0) & !(0b1111 << Self::STATE_BIT_OFFSET))
    }

    /// Returns the state of this marker.
    pub const fn state(self) -> State {
        if self.0 & (0b1 << Self::STATE_BIT_OFFSET) == 0 {
            State::Clean
        } else {
            State::Dirty
        }
    }

    /// Sets the state of this marker.
    pub const fn set_state(&mut self, state: State) {
        self.0 = (self.0 & !(0b1 << Self::STATE_BIT_OFFSET)) | ((state as usize) << Self::STATE_BIT_OFFSET);
    }

    /// Returns the padding length encoded in this `Marker`.
    pub const fn padding_len(self) -> usize {
        usize::from_le(self.0) >> Self::PADDING_LEN_OFFSET
    }

    /// Converts this `Marker` to the raw, serialized, byte format.
    pub const fn to_bytes(self) -> [u8; size_of::<Self>()] {
        self.0.to_le_bytes()
    }

    pub(crate) fn slice_to_bytes(slice: &[Marker]) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                slice.as_ptr() as *const u8,
                slice.len() * size_of::<Marker>()
            )
        }
    }
}

impl From<[u8; size_of::<Self>()]> for Marker {
    fn from(buf: [u8; size_of::<Self>()]) -> Self {
        Self(usize::from_le_bytes(buf))
    }
}

impl From<&[u8; size_of::<Self>()]> for Marker {
    fn from(inner: &[u8; size_of::<Self>()]) -> Self {
        Self::from(*inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let marker = Marker::new(Offset::<()>::new(42), 0b101, Clean);
        assert_eq!(marker.offset(), Offset::<()>::new(42));
        assert_eq!(marker.padding_len(), 0b101);
        assert_eq!(Marker::new(Offset::<()>::new(42), 0b101, Clean).to_bytes(),
                  [42,0,0,0,0,0,0,0b101_0_0000]);
    }

    #[test]
    fn set_state() {
        let mut marker = Marker::new(Offset::<()>::new(0), 0, State::Clean);
        assert_eq!(marker.state(), State::Clean);

        marker.set_state(State::Dirty);
        assert_eq!(marker.state(), State::Dirty);
    }
}
