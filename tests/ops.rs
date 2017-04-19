extern crate eve;

use eve::ops::*;
use eve::indexes::{DistinctIter};

#[test]
fn test_check_bits() {
    let solved = 45;
    let checking = 41;
    assert!(check_bits(solved, checking));
}

#[test]
fn test_set_bit() {
    let mut solved = 41;
    let setting = 2;
    solved = set_bit(solved, setting);
    assert_eq!(45, solved);
}

fn check_output_rounds(existing: Vec<(u32, i32)>, neue_rounds: Vec<i32>, expected: Vec<(u32, i32)>) {
    let mut holder = RoundHolder::new();
    let iter = DistinctIter::new(&neue_rounds);
    holder.output_rounds = existing;
    holder.compute_output_rounds(iter);
    assert_eq!(holder.output_rounds, expected);

}

#[test]
fn round_holder_compute_output_rounds() {
    check_output_rounds(vec![(3,1), (5,1)], vec![1,-1,0,0,1,0,-1], vec![(4,1), (5,1), (6,-2)]);
    check_output_rounds(vec![(3,1), (5,1)], vec![1,-1,0,1,0,0,-1], vec![(3,1), (5,1), (6,-2)]);
    check_output_rounds(vec![(3,1), (5,1)], vec![1,-1,0,0], vec![]);
    check_output_rounds(vec![(3,1), (5,1)], vec![1,0,0,0,0,0,-1], vec![(3,1), (5,1), (6,-2)]);
    check_output_rounds(vec![(0,1), (6,-1)], vec![1,0,0,0,0,0,-1], vec![(0,1), (6,-1)]);
}
