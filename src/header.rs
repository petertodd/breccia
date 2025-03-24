use std::convert::Infallible;

use crate::Marker;

pub trait Header : Sized {
    const MAGIC: &'static [u8];
    const SERIALIZED_SIZE: usize;

    fn serialize(&self, dst: &mut [u8]);

    type DeserializeError: 'static + std::error::Error + Send + Sync;
    fn deserialize(src: &[u8]) -> Result<Self, Self::DeserializeError>;
}

pub(crate) trait HeaderExt {
    const PADDING_SIZE: usize;
    const SIZE_WITH_PADDING: usize;
}

impl<T: Header> HeaderExt for T {
    const PADDING_SIZE: usize = size_of::<Marker>() - ((T::MAGIC.len() + Self::SERIALIZED_SIZE) & (size_of::<Marker>() - 1));
    const SIZE_WITH_PADDING: usize = T::MAGIC.len() + T::SERIALIZED_SIZE + Self::PADDING_SIZE;
}

impl Header for () {
    const MAGIC: &[u8] = b"";
    const SERIALIZED_SIZE: usize = 0;

    fn serialize(&self, _dst: &mut [u8]) {
    }

    type DeserializeError = Infallible;
    fn deserialize(_src: &[u8]) -> Result<Self, Self::DeserializeError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
    }
}
