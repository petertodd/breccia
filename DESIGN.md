# The Design of the Breccia Append-Only Blob Store

Let's start with our requirements:

1. Single-file, append-only: a breccia is stored in a single file and used
   efficiently with no external indexes, and it must be possible to set the
   append-bit on the file itself (`chattr +a`). In normal operation once a byte
   is written to a file, it is *never* changed. This means that mirroring
   breccia's is particularly easy: just download new bytes as they are written
   and append those bytes to your local copy.
2. Discoverable: starting from an arbitrary offset in the file, the next (or
   previous) blob can be discovered reliably. This means that blobs can be
   iterated in both directions, and ordered blobs can be found via binary search.
3. Low (space) overhead: even when the average blob size is small (<100bytes)
   the supermajority of the file size is taken up by blob data, not breccia
   overhead.
4. Memory-map compatible: blobs are stored in the breccia file verbatim,
   without being broken up into multiple parts, allowing the exact bytes of a
   stored blob to be accessed via memory mappings.

What are *not* requirements:

0. Efficient binary search on large blobs: for breccia's taking advantage of
   binary search, it is assumed that blobs are sufficiently small that the cost
   of finding blob start and end offsets is small relative to random seek
   costs.
1. Ease-of-use: breccia is a low-level blob storage only. The application is
   expected to implement higher level functionality like serialization,
   versioning, indexing, file headers, etc.


# Blobs, Words, and Marks

Since memory-mapping is a requirement, the blobs themselves must be stored
verbatim in the breccia file. Which means that we really have just one
question: how do we find the beginning and end of a blob?

Breccia solves this problem by:

0. Splitting up the file into 64-bit words.
1. Marking the beginning of each blob with a 64-bit mark word, equal in value
   to the offset of the mark.
2. When blobs are written, they are first checked for conflicts: the blob is
   itself split into 64-bit words (with padding at the end), and each word is
   checked to see if it would be unintentionally interpreted as a mark at the
   offset that we are going to write the blob to. In the event of a collision,
   padding words are adding until the blob is collision free.

This approach works because for a given word value V, a collision is only
possible at a single offset i. Once enough padding has been added to avoid the
collision, any additional amount of padding is also non-colliding. Thus even in
the worst possible case, a blob consisting of N words can only have N
collisions, and thus at most N bytes of padding are necessary to avoid all
collisions.
