use std::convert::Infallible;

pub trait Header : Sized {
    const MAGIC: &'static [u8];
    const SIZE: usize;

    fn serialize(&self, dst: &mut [u8]);

    type DeserializeError: 'static + std::error::Error + Send + Sync;
    fn deserialize(src: &[u8]) -> Result<Self, Self::DeserializeError>;
}

impl Header for () {
    const MAGIC: &[u8] = b"";
    const SIZE: usize = 0;

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
