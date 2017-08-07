//-------------------------------------------------------------------------
// HashIndex benches
//-------------------------------------------------------------------------
#![feature(test)]

extern crate test;
extern crate eve;

use eve::indexes::*;
use self::test::Bencher;
use std::num::Wrapping;

fn rand(rseed:u32) -> u32 {
    return ((Wrapping(rseed) * Wrapping(1103515245) + Wrapping(12345)) & Wrapping(0x7fffffff)).0;
}


#[bench]
fn hash_write(b:&mut Bencher) {
    let mut index = HashIndex::new();
    let mut seed = 0;
    // for ix in 0..10_000_000 {
    //     let e = rand(seed);
    //     seed = e;
    //     let a = rand(seed);
    //     seed = a;
    //     let val = rand(seed);
    //     seed = val;
    //     index.insert(e % 10000, (a % 50) + 1, val % 10000);
    // }
    b.iter(|| {
        let e = rand(seed);
        seed = e;
        let a = rand(seed);
        seed = a;
        let val = rand(seed);
        seed = val;
        index.insert(e % 100000, (a % 50) + 1, val % 100000);
        // if(index.size > 100000) {
        //     index = HashIndex3::new();
        // }
        // total += index.size;
    });
    // println!("{:?} : {:?}", times, index.size);
}

#[bench]
fn hash_write_200_000(b:&mut Bencher) {
    let mut seed = 0;
    b.iter(|| {
        let mut index = HashIndex::new();
        for _ in 0..200_000 {
            let e = rand(seed);
            seed = e;
            let a = rand(seed);
            seed = a;
            let val = rand(seed);
            seed = val;
            index.insert(e % 100000, (a % 50) + 1, val % 100000);
        }
    });
    // println!("{:?} : {:?}", times, index.size);
}

#[bench]
fn hash_read(b:&mut Bencher) {
    let mut index = HashIndex::new();
    let mut seed = 0;
    for _ in 0..100_000 {
        let e = rand(seed);
        seed = e;
        let a = rand(seed);
        seed = a;
        let val = rand(seed);
        seed = val;
        index.insert(e % 100000, (a % 50) + 1, val % 100000);
    }
    seed = 0;
    // let mut v = vec![];
    b.iter(|| {
        let e = rand(seed);
        seed = e;
        let a = rand(seed);
        seed = a;
        let val = rand(seed);
        seed = val;
        index.check(e % 100000, (a % 50) + 1, val % 100000);
    });
    // println!("results: {:?}", total);
}
