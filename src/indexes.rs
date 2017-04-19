//-------------------------------------------------------------------------
// Indexes
//-------------------------------------------------------------------------

use std::collections::HashMap;
use ops::{EstimateIter, Change, RoundHolder};
use std::cmp;
use std::collections::hash_map::Entry;

extern crate fnv;
use indexes::fnv::FnvHasher;
use std::hash::BuildHasherDefault;

pub type MyHasher = BuildHasherDefault<FnvHasher>;

//-------------------------------------------------------------------------
// Utils
//-------------------------------------------------------------------------

pub fn ensure_len(vec:&mut Vec<i32>, len:usize) {
    if vec.len() < len {
        vec.resize(len, 0);
    }
}

pub fn get_delta(last:i32, next:i32) -> i32 {
    if last == 0 && next > 0 { 1 }
    else if last > 0 && next == 0 { -1 }
    else if last > 0 && next < 0 { -1 }
    else if last < 0 && next > 0 { 1 }
    else { 0 }
}

//-------------------------------------------------------------------------
// HashIndexLevel
//-------------------------------------------------------------------------

pub struct HashIndexLevel {
    e: HashMap<u32, Vec<u32>, MyHasher>,
    v: HashMap<u32, Vec<u32>, MyHasher>,
    es: Vec<u32>,
    vs: Vec<u32>,
    size: u32,
}

impl HashIndexLevel {
    pub fn new() -> HashIndexLevel {
        HashIndexLevel { e: HashMap::default(), v: HashMap::default(), es:vec![], vs:vec![], size: 0 }
    }

    pub fn insert(&mut self, e: u32, v:u32) -> bool {
        let added = match self.e.entry(e) {
            Entry::Occupied(mut o) => {
                let mut vs = o.get_mut();
                vs.push(v);
                true
            }
            Entry::Vacant(o) => {
                self.es.push(e);
                o.insert(vec![v]);
                true
            },
        };
        if added {
            self.size += 1;
            match self.v.entry(v) {
                Entry::Occupied(mut o) => {
                    let mut es = o.get_mut();
                    es.push(e);
                }
                Entry::Vacant(o) => {
                    self.vs.push(v);
                    o.insert(vec![e]);
                },
            };
        }
        added
    }

    pub fn check(&self, e: u32, v:u32) -> bool {
        if e > 0 && v > 0 {
            match self.e.get(&e) {
                Some(es) => es.contains(&v),
                None => false,
            }
        } else if e > 0 {
            self.e.contains_key(&e)
        } else if v > 0 {
            self.v.contains_key(&v)
        } else {
            self.size > 0
        }
    }

    pub fn find_values(&self, e:u32) -> Option<&Vec<u32>>  {
        self.e.get(&e)
    }

    pub fn find_entities(&self, v:u32) -> Option<&Vec<u32>> {
        self.v.get(&v)
    }

    pub fn get(&self, e:u32, v:u32) -> Option<&Vec<u32>> {
        if e > 0 {
            // println!("here looking for v {:?}", e);
            self.find_values(e)
        } else if v > 0 {
            self.find_entities(v)
        } else {
            let es_len = self.e.len();
            let vs_len = self.v.len();
            if es_len < vs_len {
                if es_len > 0 {
                    Some(&self.es)
                } else {
                    None
                }
            } else {
                if vs_len > 0 {
                    Some(&self.vs)
                } else {
                    None
                }
            }
        }
    }

    pub fn propose(&self, iter:&mut EstimateIter, e:u32, v:u32) {
        match *iter {
            EstimateIter::Scan { ref mut estimate, ref mut output, ref mut values_ptr, ref mut len, .. } => {
                if e > 0 {
                    if let Some(vs) = self.find_values(e) {
                        let vs_len = vs.len();
                        *values_ptr = vs.as_ptr();
                        *len = vs_len;
                        *estimate = vs_len as u32;
                        *output = 2;
                    }
                } else if v > 0 {
                    if let Some(es) = self.find_entities(v) {
                        let es_len = es.len();
                        *values_ptr = es.as_ptr();
                        *len = es_len;
                        *estimate = es_len as u32;
                        *output = 0;
                    }
                } else {
                    let es_len = self.e.len();
                    let vs_len = self.v.len();
                    if es_len < vs_len {
                        // only if we have values do we fill in the iter
                        if es_len > 0 {
                            *values_ptr = self.es.as_ptr();
                            *len = es_len;
                            *estimate = es_len as u32;
                            *output = 0;
                        }
                    } else {
                        // only if we have values do we fill in the iter
                        if vs_len > 0 {
                            *values_ptr = self.vs.as_ptr();
                            *len = vs_len;
                            *estimate = vs_len as u32;
                            *output = 2;
                        }
                    }
                }
            }
            _ => panic!("Non scan iter passed to index propose"),
        }
    }
}

//-------------------------------------------------------------------------
// HashIndex
//-------------------------------------------------------------------------

pub struct HashIndex {
    a: HashMap<u32, HashIndexLevel, MyHasher>,
    eavs: HashMap<(u32, u32, u32), bool, MyHasher>,
    attrs: Vec<u32>,
    pub size: u32,
}

impl HashIndex {
    pub fn new() -> HashIndex{
        HashIndex { a: HashMap::default(), eavs: HashMap::default(), size: 0, attrs: vec![] }
    }

    pub fn insert(&mut self, e: u32, a:u32, v:u32) -> bool {
        let added = match self.eavs.entry((e,a,v)) {
            Entry::Occupied(_) => {
                false
            }
            Entry::Vacant(o) => {
                o.insert(true);
                true
            },

        };
        if added {
            self.size += 1;
            match self.a.entry(a) {
                Entry::Occupied(mut o) => {
                    let mut level = o.get_mut();
                    level.insert(e, v)
                }
                Entry::Vacant(o) => {
                    self.attrs.push(a);
                    let mut level = HashIndexLevel::new();
                    level.insert(e,v);
                    o.insert(level);
                    true
                },
            };
        }
        added
    }

    #[inline(never)]
    pub fn check(&self, e: u32, a:u32, v:u32) -> bool {
        if e > 0 && a > 0 && v > 0 {
            self.eavs.contains_key(&(e,a,v))
        } else if a > 0 {
            match self.a.get(&a) {
                Some(level) => level.check(e, v),
                None => false,
            }
        } else {
            panic!("Haven't implemented check for free a")
        }
    }

    pub fn get(&self, e:u32, a:u32, v:u32) -> Option<&Vec<u32>> {
        if a == 0 {
            if self.attrs.len() > 0 {
                Some(&self.attrs)
            } else {
                None
            }
        } else {
            let level = match self.a.get(&a) {
                None => return None,
                Some(level) => level,
            };
            level.get(e, v)
        }
    }

    pub fn propose(&self, iter: &mut EstimateIter, e:u32, a:u32, v:u32) {
        if a == 0 {
            // @FIXME: this isn't always safe. In the case where we have an arbitrary lookup, if we
            // then propose, we might propose values that we then never actually check are correct.
            let mut vals = vec![];
            for key in self.a.keys() {
                vals.push(*key);
            }
            match iter {
                &mut EstimateIter::Scan { ref mut estimate, ref mut pos, ref mut values_ptr, ref mut len, ref mut output, .. } => {
                    let attrs_len = self.attrs.len();
                    *output = 1;
                    *pos = 0;
                    *len = attrs_len;
                    *estimate = attrs_len as u32;
                    *values_ptr = self.attrs.as_ptr();
                },
                _ => panic!("Non scan iter passed to propose"),
            }
        } else {
            let level = match self.a.get(&a) {
                None => return,
                Some(level) => level,
            };
            level.propose(iter, e, v);
        }
    }
}

//-------------------------------------------------------------------------
// Distinct Index
//-------------------------------------------------------------------------

pub struct DistinctIter<'a> {
    ix: usize,
    total: i32,
    len: usize,
    rounds: &'a Vec<i32>,
}

impl<'a> DistinctIter<'a> {
    pub fn new(rounds:&'a Vec<i32>) -> DistinctIter<'a> {
        DistinctIter { rounds, ix: 0, total: 0, len: rounds.len() }
    }
}

impl<'a> Iterator for DistinctIter<'a> {
    type Item = (u32, i32);

    fn next(&mut self) -> Option<(u32, i32)> {
        let mut ix = self.ix;
        let mut total = self.total;
        let ref mut rounds = self.rounds;
        let mut delta = 0;
        while ix < self.len && delta == 0 {
            let next = rounds[ix];
            delta = get_delta(total, total + next);
            total += next;
            ix += 1;
        }
        self.ix = ix;
        self.total = total;
        if delta == 0 {
            None
        } else {
            Some(((ix - 1) as u32, delta))
        }
    }
}

pub struct DistinctIndex {
    index: HashMap<(u32, u32, u32), Vec<i32>>,
    empty: Vec<i32>,
}

impl DistinctIndex {
    pub fn new() -> DistinctIndex {
        DistinctIndex { index: HashMap::new(), empty: vec![] }
    }

    pub fn iter(&self, e:u32, a:u32, v:u32) -> DistinctIter {
        let key = (e, a, v);
        match self.index.get(&key) {
            Some(rounds) => DistinctIter::new(rounds),
            None => DistinctIter::new(&self.empty),
        }
    }

    #[allow(dead_code)]
    pub fn raw_insert(&mut self, e:u32, a:u32, v:u32, round:u32, count:i32) {
        let key = (e,a,v);
        let mut counts = self.index.entry(key).or_insert_with(|| vec![]);
        ensure_len(counts, (round + 1) as usize);
        counts[round as usize] += count;
    }

    pub fn distinct(&mut self, input:&Change, rounds:&mut RoundHolder) {
        let key = (input.e, input.a, input.v);
        let input_count = input.count;
        let mut counts = self.index.entry(key).or_insert_with(|| vec![]);
        // println!("Pre counts {:?}", counts);
        ensure_len(counts, (input.round + 1) as usize);
        let counts_len = counts.len() as u32;
        let min = cmp::min(input.round + 1, counts_len);
        let mut cur_count = 0;
        for ix in 0..min {
           cur_count += counts[ix as usize];
        };

        // @TODO: handle Infinity/-Infinity for commits at round 0

        let next_count = cur_count + input_count;
        let delta = get_delta(cur_count, next_count);
        if delta != 0 {
            rounds.insert(input.with_round_count(input.round, delta));
        }

        cur_count = next_count;
        counts[input.round as usize] += input.count;

        for round_ix in (input.round + 1)..counts_len {
            let round_count = counts[round_ix as usize];
            if round_count == 0 { continue; }

            let last_count = cur_count - input_count;
            let next_count = last_count + round_count;
            let delta = get_delta(last_count, next_count);

            let last_count_changed = cur_count;
            let next_count_changed = cur_count + round_count;
            let delta_changed = get_delta(last_count_changed, next_count_changed);

            let mut final_delta = 0;
            if delta != 0 && delta != delta_changed {
                //undo the delta
                final_delta = -delta;
            } else if delta != delta_changed {
                final_delta = delta_changed;
            }

            if final_delta != 0 {
                // println!("HERE {:?} {:?} | {:?} {:?}", round_ix, final_delta, delta, delta_changed);
                rounds.insert(input.with_round_count(round_ix, final_delta));
            }

            cur_count = next_count_changed;
        }
        // println!("Post counts {:?}", counts);
    }
}

//-------------------------------------------------------------------------
// Distinct tests
//-------------------------------------------------------------------------

#[cfg(test)]
mod DistinctTests {
    extern crate test;

    use super::*;
    use self::test::Bencher;
    use ops::{EstimateIterPool};

    fn round_counts_to_changes(counts: Vec<(u32, i32)>) -> Vec<Change> {
        let mut changes = vec![];
        let cur = Change { e: 1, a: 2, v: 3, n: 4, transaction: 1, round: 0, count: 0 };
        for &(round, count) in counts.iter() {
            changes.push(cur.with_round_count(round, count));
        }
        changes
    }

    fn test_distinct(counts: Vec<(u32, i32)>, expected: Vec<(u32, i32)>) {
        let mut index = DistinctIndex::new();
        let changes = round_counts_to_changes(counts);

        let mut final_results: HashMap<u32, i32> = HashMap::new();
        let mut distinct_changes = RoundHolder::new();
        for change in changes.iter() {
            index.distinct(change, &mut distinct_changes);
        }
        let mut iter = distinct_changes.iter();
        while let Some(distinct) = iter.next(&mut distinct_changes) {
            println!("distinct: {:?}", distinct);
            let cur = if final_results.contains_key(&distinct.round) { final_results[&distinct.round] } else { 0 };
            final_results.insert(distinct.round, cur + distinct.count);
        }

        for (round, count) in index.iter(changes[0].e, changes[0].a, changes[0].v) {
            let valid = match final_results.get(&round) {
                Some(&actual) => actual == count,
                None => count == 0,
            };
            assert!(valid, "iterator round {:?} :: expected {:?}, actual {:?}", round, count, final_results.get(&round));
        }

        println!("final {:?}", final_results);

        let mut expected_map = HashMap::new();
        for &(round, count) in expected.iter() {
            expected_map.insert(round, count);
            let valid = match final_results.get(&round) {
                Some(&actual) => actual == count,
                None => count == 0,
            };
            assert!(valid, "round {:?} :: expected {:?}, actual {:?}", round, count, final_results.get(&round));
        }

        for (round, count) in final_results.iter() {
            let valid = match expected_map.get(&round) {
                Some(&actual) => actual == *count,
                None => *count == 0,
            };
            assert!(valid, "round {:?} :: expected {:?}, actual {:?}", round, expected_map.get(&round), count);
        }

    }

    #[test]
    fn basic() {
        test_distinct(vec![
            (1,1),
            (2,-1),

            (1, 1),
            (3, -1),
        ], vec![
            (1, 1),
            (3, -1)
        ])
    }

    #[test]
    fn basic_2() {
        test_distinct(vec![
            (1,1),
            (2,-1),

            (3, 1),
            (4, -1),
        ], vec![
            (1, 1),
            (2, -1),
            (3, 1),
            (4, -1),
        ])
    }

    #[test]
    fn basic_2_reverse_order() {
        test_distinct(vec![
            (3,1),
            (4,-1),

            (1, 1),
            (2, -1),
        ], vec![
            (1, 1),
            (2, -1),
            (3, 1),
            (4, -1),
        ])
    }

    #[test]
    fn basic_2_undone() {
        test_distinct(vec![
            (1,1),
            (2,-1),

            (3, 1),
            (4, -1),

            (1,-1),
            (2,1),
        ], vec![
            (3, 1),
            (4, -1),
        ])
    }

    #[test]
    fn basic_multiple() {
        test_distinct(vec![
            (1,1),
            (1,1),
            (1,1),
            (1,1),
            (2,-1),
            (2,-1),
            (2,-1),
            (2,-1),

            (3, 1),
            (3, 1),
            (3, 1),
            (4, -1),
            (4, -1),
            (4, -1),
        ], vec![
            (1, 1),
            (2, -1),
            (3, 1),
            (4, -1),
        ])
    }

    #[test]
    fn basic_multiple_reversed() {
        test_distinct(vec![
            (3, 1),
            (3, 1),
            (3, 1),
            (4, -1),
            (4, -1),
            (4, -1),

            (1,1),
            (1,1),
            (1,1),
            (1,1),
            (2,-1),
            (2,-1),
            (2,-1),
            (2,-1),
        ], vec![
            (1, 1),
            (2, -1),
            (3, 1),
            (4, -1),
        ])
    }

    #[test]
    fn basic_interleaved() {
        test_distinct(vec![
            (3, 1),
            (4, -1),
            (3, 1),
            (4, -1),
            (3, 1),
            (4, -1),

            (1,1),
            (2,-1),
            (1,1),
            (2,-1),
            (1,1),
            (2,-1),
            (1,1),
            (2,-1),
        ], vec![
            (1, 1),
            (2, -1),
            (3, 1),
            (4, -1),
        ])
    }

    #[test]
    fn basic_multiple_negative_first() {
        test_distinct(vec![
            (2,-1),
            (2,-1),
            (2,-1),
            (1,1),
            (1,1),
            (1,1),

            (4, -1),
            (4, -1),
            (4, -1),
            (3, 1),
            (3, 1),
            (3, 1),
        ], vec![
            (1, 1),
            (2, -1),
            (3, 1),
            (4, -1),
        ])
    }

    #[test]
    fn basic_multiple_undone() {
        test_distinct(vec![
            (1,1),
            (1,1),
            (1,1),
            (1,1),
            (2,-1),
            (2,-1),
            (2,-1),
            (2,-1),

            (3, 1),
            (3, 1),
            (3, 1),
            (4, -1),
            (4, -1),
            (4, -1),

            (1,-1),
            (1,-1),
            (1,-1),
            (1,-1),
            (2,1),
            (2,1),
            (2,1),
            (2,1),
        ], vec![
            (3, 1),
            (4, -1),
        ])
    }

    #[test]
    fn basic_multiple_undone_interleaved() {
        test_distinct(vec![
            (1,1),
            (1,1),
            (1,1),
            (1,1),
            (2,-1),
            (2,-1),
            (2,-1),
            (2,-1),

            (1,-1),
            (1,-1),
            (1,-1),
            (1,-1),

            (3, 1),
            (3, 1),
            (3, 1),
            (4, -1),
            (4, -1),
            (4, -1),

            (2,1),
            (2,1),
            (2,1),
            (2,1),
        ], vec![
            (3, 1),
            (4, -1),
        ])
    }

    #[test]
    fn basic_multiple_different_counts() {
        test_distinct(vec![
            (1,1),
            (1,1),
            (1,1),
            (1,1),
            (2,-1),
            (2,-1),
            (2,-1),
            (2,-1),

            (3, 1),
            (4, -1),
        ], vec![
            (1, 1),
            (2, -1),
            (3, 1),
            (4, -1),
        ])
    }

    #[test]
    fn basic_multiple_different_counts_extra_removes() {
        test_distinct(vec![
            (1,1),
            (1,1),
            (1,1),
            (1,1),
            (2,-1),
            (2,-1),
            (2,-1),
            (2,-1),

            (1,-1),
            (1,-1),
            (1,-1),
            (1,-1),
            (2,1),
            (2,1),
            (2,1),
            (2,1),

            (3, 1),
            (4, -1),
        ], vec![
            (3, 1),
            (4, -1),
        ])
    }

    #[test]
    fn simple_round_promotion() {
        test_distinct(vec![
            (8,1),
            (9,-1),

            (5,1),
            (6,-1),
            (8,-1),
            (9,1),
        ], vec![
            (5, 1),
            (6, -1)
        ])
    }

    #[test]
    fn full_promotion() {
        test_distinct(vec![
            (9,1),
            (9,1),
            (10,-1),
            (10,-1),

            (9,1),
            (9,1),
            (10,-1),
            (10,-1),

            (9,-1),
            (10,1),
            (9,-1),
            (10,1),

            (9,-1),
            (10,1),
            (9,-1),
            (10,1),
        ], vec![
            (9, 0),
            (10, 0)
        ])
    }

    #[test]
    fn positive_full_promotion() {
        test_distinct(vec![
            (7,1),
            (8,-1),
            (8,1),
            (7,1),
            (8,-1),
            (4,1),
            (8, -1),
            (7, 1),
            (8, -1),
            (8, 1),
            (5, -1),
            (7, -3),
            (8, 1),
            (8, 3),
            (5, 1),
            (8, 1),
            (8, -2),
            (8, -1),
        ], vec![
            (4, 1),
        ])
    }
}

//-------------------------------------------------------------------------
// HashIndex Tests
//-------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    extern crate test;

    use super::*;
    use self::test::Bencher;
    use std::num::Wrapping;

    #[test]
    fn basic() {
        let mut index = HashIndex::new();
        index.insert(1,1,1);
        index.insert(1,2,1);
        index.insert(2,3,1);
        index.insert(1,3,100);
        assert!(index.check(1,1,1));
        assert!(index.check(1,2,1));
        assert!(index.check(2,3,1));
        assert!(index.check(1,3,100));
        assert!(!index.check(100,300,100));
    }

    #[test]
    fn basic2() {
        let mut index = HashIndex::new();
        index.insert(5,3,8);
        index.insert(9,3,8);
        assert!(index.check(5,3,8));
        assert!(index.check(9,3,8));
        assert!(!index.check(100,300,100));
    }

    #[test]
    fn find_entities() {
        let mut index = HashIndexLevel::new();
        index.insert(1,1);
        index.insert(2,1);
        index.insert(300,1);
        let entities = index.get(0, 1).unwrap();
        assert!(entities.contains(&1));
        assert!(entities.contains(&2));
        assert!(entities.contains(&300));
        assert!(!entities.contains(&3));
    }

    #[test]
    fn find_values() {
        let mut index = HashIndexLevel::new();
        index.insert(1,1);
        index.insert(1,2);
        index.insert(1,300);
        {
            let values = index.get(1, 0).unwrap();
            assert!(values.contains(&1));
            assert!(values.contains(&2));
            assert!(values.contains(&300));
            assert!(!values.contains(&3));
        }

        index.insert(5,8);
        index.insert(9,8);
        let values2 = index.get(9, 0).unwrap();
        assert!(values2.contains(&8));
    }

     #[test]
    fn basic_propose() {
        let mut index = HashIndex::new();
        let mut pool = EstimateIterPool::new();
        index.insert(1,1,1);
        index.insert(2,1,1);
        index.insert(2,1,7);
        index.insert(3,1,1);
        index.insert(2,3,1);
        index.insert(1,3,100);
        let mut proposal1 = pool.get();
        index.propose(&mut proposal1, 0,1,1);
        assert_eq!(proposal1.estimate(), 3);
        let mut proposal2 = pool.get();
        index.propose(&mut proposal2, 2,1,0);
        assert_eq!(proposal2.estimate(), 2);
    }


    fn rand(rseed:u32) -> u32 {
        return ((Wrapping(rseed) * Wrapping(1103515245) + Wrapping(12345)) & Wrapping(0x7fffffff)).0;
    }


    #[bench]
    fn bench_hash_write(b:&mut Bencher) {
        let mut total = 0;
        let mut times = 0;
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
        seed = 0;
        b.iter(|| {
            times += 1;
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
        println!("{:?} : {:?}", times, index.size);
    }

    #[bench]
    fn bench_hash_write_200_000(b:&mut Bencher) {
        let mut total = 0;
        let mut times = 0;
        let mut seed = 0;
        seed = 0;
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
    fn bench_hash_read(b:&mut Bencher) {
        let mut total = 0;
        let mut times = 0;
        let mut levels = 0;
        let mut index = HashIndex::new();
        let mut seed = 0;
        for ix in 0..100_000 {
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
            total += seed;
            index.check(e % 100000, (a % 50) + 1, val % 100000);
        });
        println!("results: {:?}", total);
    }



}

