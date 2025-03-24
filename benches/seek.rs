#![feature(test)]
#![feature(array_windows)]
#![feature(array_chunks)]

extern crate test;
use test::bench::{Bencher, black_box};

use std::io;

use breccia::{BrecciaMut, Search};

use tempfile::tempfile;

#[bench]
fn write_blobs(bencher: &mut Bencher) -> io::Result<()> {
    bencher.iter(|| {
        let mut b = BrecciaMut::create_from_file(tempfile().unwrap(), ()).unwrap();
        for i in 0u64 .. 10_000 {
            let mut blob = vec![0u8; 0];
            blob.extend_from_slice(&i.to_le_bytes());

            blob.resize(8 + rand::random_range(0 .. 100), 42u8);

            b.write_blob(&blob).unwrap();
        }
    });

    Ok(())
}

#[bench]
fn random_seeks(bencher: &mut Bencher) -> io::Result<()> {
    let mut b = BrecciaMut::create_from_file(tempfile()?, ())?;

    let mut offsets = vec![];
    for i in 0u64 .. 50_000 {
        let mut blob = vec![0u8; 0];
        blob.extend_from_slice(&i.to_le_bytes());

        blob.resize(8 + rand::random_range(0 .. 100), 42u8);

        offsets.push((i, b.write_blob(&blob)?));
    }

    bencher.iter(|| {
        for (i, expected_offset) in &offsets[1000..2000] {
            assert_eq!(b.binary_search(|offset, blob| {
                let blob = (&blob[0 .. 8]).try_into().unwrap();
                let found = u64::from_le_bytes(blob);

                if *i == found {
                    Ok(Some(offset))
                } else if *i < found {
                    Err(Search::Left)
                } else { // if i > found
                    Err(Search::Right)
                }
            }),
            Some(*expected_offset));
        }
    });

    Ok(())
}
