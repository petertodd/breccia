#![feature(test)]
#![feature(array_windows)]
#![feature(array_chunks)]

extern crate test;

use test::bench::{Bencher, black_box};

#[inline(never)]
pub fn find_marker(buf: &[u8]) -> Option<usize> {
    for (i, chunk) in buf.array_windows::<8>().enumerate() {
        let chunk = u64::from_le_bytes(*chunk);
        let chunk = !chunk;
        if chunk == i as u64 {
            return Some(i);
        }
    };
    None
}

#[bench]
fn marker_seek(bencher: &mut Bencher) {
    //let buf = [0x00, 0x00, 0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
    //dbg!(find_marker(&buf));

    let buf = vec![0x34; 100_000_000];
    bencher.iter(|| {
        assert_eq!(find_marker(black_box(&buf)), None);
    });
}

#[inline(never)]
pub fn find_marker2(buf: &[u8]) -> Option<usize> {
    for (i, chunk) in buf.array_chunks::<8>().enumerate() {
        let chunk = u64::from_le_bytes(*chunk);
        let chunk = !chunk;
        if chunk == i as u64 {
            return Some(i);
        }
    };
    None
}

#[bench]
fn marker_seek2(bencher: &mut Bencher) {
    let buf = vec![0x34; 100_000_000];
    bencher.iter(|| {
        assert_eq!(find_marker2(black_box(&buf)), None);
    });
}
