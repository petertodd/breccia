# Breccia: Append-Only Blob Storage

Single-file, append-only, blob storage with efficient random access. In
particular, binary search can be done on a breccia file, allowing you to
efficiently find any blob stored in it so long as the blobs themselves have an
ordering.


# The Name

In geology, breccia is a type of rock characterized by rough, unprocessed,
angular, fragments cemented together in a fine-grained matrix. The
[design](/DESIGN.md) of the breccia blob storage file format is similar: your
raw data blobs are held together to small marker words, allowing for them to be
retrieved later even though the blobs are written to the file verbatim.


# Limitations

Since the file is memory-mapped, only 64-bit platforms are supported; if
`usize` is not 64-bits compilation will fail.
