use std::convert::Infallible;

use crate::Marker;

/// A type that can be used as a breccia file header.
///
/// Since every breccia blob store contains its own type of data, headers are needed to figure out
/// what we're actually working with.
///
/// Headers should contain a major version, and an unknown major version should be an error.
///
/// # Example
///
/// ```
/// use breccia::Header;
///
/// pub struct ExampleHeader {
///     major: u8,
/// }
///
/// #[derive(Debug)]
/// pub struct UnknownMajorVersionError(u8);
///
/// impl std::fmt::Display for UnknownMajorVersionError {
///     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
///         write!(f, "Unknown major version: {}", self.0)
///     }
/// }
///
/// impl std::error::Error for UnknownMajorVersionError {}
///
/// impl Header for ExampleHeader {
///     const MAGIC: &'static [u8] = b"\x00Example\x00\xd6\xfb\xa4\xe9\xac\xd3";
///     const SERIALIZED_SIZE: usize = 1;
///
///     fn serialize(&self, dst: &mut [u8]) {
///         dst[0] = self.major;
///     }
///
///     type DeserializeError = UnknownMajorVersionError;
///     fn deserialize(src: &[u8]) -> Result<Self, Self::DeserializeError> {
///         let major = src[0];
///         if major < 2 {
///             Ok(Self { major })
///         } else {
///             Err(UnknownMajorVersionError(major))
///         }
///     }
/// }
/// ```
pub trait Header : Sized {
    /// The magic bytes at the beginning of the file.
    ///
    /// Good magic bytes should have three elements:
    ///
    /// 1. The first byte should be 0x00 to make it clear that this file is binary data, not text.
    /// 2. Human readable text, to make it possible to figure out what the file might be in a
    ///    hex-dump.
    /// 3. At least 6 randomly chosen bytes, (48-bits), to ensure the magic bytes are globally
    ///    unique. Set the high-bits on these bytes to ensure they're not valid ASCII.
    ///
    /// A good example of this is the magic bytes from the OpenTimestamps Proof format:
    ///
    /// ```no_run
    /// const MAGIC: &'static [u8] = b"\x00OpenTimestamps\x00\x00Proof\x00\xbf\x89\xe2\xe8\x84\xe8\x92\x94";
    /// ```
    ///
    /// ...which, including the major version byte, is exactly 16 bytes in size to look nice in hex
    /// dumps:
    ///
    /// ```no_test
    /// $ hexdump -C example.txt.ots
    /// 00000000  00 4f 70 65 6e 54 69 6d  65 73 74 61 6d 70 73 00  |.OpenTimestamps.|
    /// 00000010  00 50 72 6f 6f 66 00 bf  89 e2 e8 84 e8 92 94 01  |.Proof..........|
    /// ```
    const MAGIC: &'static [u8];

    /// The serialized size, in bytes, *not* including the magic bytes.
    const SERIALIZED_SIZE: usize;

    /// Serialize an instance of this type to bytes.
    ///
    /// `dst` will be a slice of exactly `SERIALIZED_SIZE` in length.
    fn serialize(&self, dst: &mut [u8]);

    /// The error returned when deserialize fails.
    type DeserializeError: 'static + std::error::Error + Send + Sync;

    /// Deserialize an instance of this type from bytes.
    ///
    /// `src` will be a slice of exactly `SERIALIZED_SIZE` in length.
    fn deserialize(src: &[u8]) -> Result<Self, Self::DeserializeError>;
}

/// Extentions to `Header` to calculate some constants we use a lot.
pub(crate) trait HeaderExt {
    const PADDING_SIZE: usize;
    const SIZE_WITH_PADDING: usize;
}

impl<T: Header> HeaderExt for T {
    const PADDING_SIZE: usize = size_of::<Marker>() - ((T::MAGIC.len() + Self::SERIALIZED_SIZE) & (size_of::<Marker>() - 1));
    const SIZE_WITH_PADDING: usize = T::MAGIC.len() + T::SERIALIZED_SIZE + Self::PADDING_SIZE;
}

/// For testing only.
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
