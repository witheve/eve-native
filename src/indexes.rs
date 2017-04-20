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
