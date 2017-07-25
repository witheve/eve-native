//-------------------------------------------------------------------------
// Indexes
//-------------------------------------------------------------------------

// use std::collections::HashMap;
use ops::{EstimateIter, Change, RoundHolder, Interned, Round, Count, IntermediateChange, Internable, Interner, AggregateFunction};
use std::cmp;

extern crate fnv;
use indexes::fnv::FnvHasher;
use std::hash::{BuildHasherDefault};
use hash::map::{GetDangerousKeys, HashMap, Entry, DangerousKeys};
use std::collections::btree_map;
use std::iter::{Iterator};
use std::collections::BTreeMap;

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
// HashIndexIter
//-------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum HashIndexIter {
    Empty,
    Single { value:Interned, returned:bool },
    Root(DangerousKeys<Interned, HashIndexLevel>),
    Middle(DangerousKeys<Interned, HashIndexLeaf>),
    Leaf(DangerousKeys<Interned, ()>),
}

impl HashIndexIter {
    pub fn len(&self) -> usize {
        match self {
            &HashIndexIter::Empty => 0,
            &HashIndexIter::Single {..} => 1,
            &HashIndexIter::Root(ref iter) => iter.len(),
            &HashIndexIter::Middle(ref iter) => iter.len(),
            &HashIndexIter::Leaf(ref iter) => iter.len(),
        }
    }
}

impl Iterator for HashIndexIter {
    type Item = Interned;

    fn next(&mut self) -> Option<Interned> {
        match self {
            &mut HashIndexIter::Empty => None,
            &mut HashIndexIter::Single { value, ref mut returned } => if *returned { None } else { *returned = true; Some(value) },
            &mut HashIndexIter::Root(ref mut iter) => iter.next().map(|x| *x),
            &mut HashIndexIter::Middle(ref mut iter) => iter.next().map(|x| *x),
            &mut HashIndexIter::Leaf(ref mut iter) => iter.next().map(|x| *x),
        }
    }
}

//-------------------------------------------------------------------------
// HashIndexLeaf
//-------------------------------------------------------------------------

#[derive(Clone)]
pub enum HashIndexLeaf {
    Single(Interned),
    Many(HashMap<Interned, (), MyHasher>),
}

impl HashIndexLeaf {
    pub fn insert(&mut self, neue_value:Interned) {
        match self {
            &mut HashIndexLeaf::Single(prev) => {
                let mut neue = HashMap::default();
                neue.insert(prev, ());
                neue.insert(neue_value, ());
                *self = HashIndexLeaf::Many(neue);
            },
            &mut HashIndexLeaf::Many(ref mut prev) => {
                prev.insert(neue_value, ());
            },
        }
    }

    pub fn remove(&mut self, neue_value:Interned) -> bool {
        match self {
            &mut HashIndexLeaf::Single(prev) => {
                prev == neue_value
            },
            &mut HashIndexLeaf::Many(ref mut prev) => {
                prev.remove(&neue_value);
                prev.len() == 0
            },
        }
    }

    pub fn check(&self, v:Interned) -> bool {
        match self {
            &HashIndexLeaf::Single(cur) => cur == v,
            &HashIndexLeaf::Many(ref cur) => cur.contains_key(&v),
        }
    }

    pub fn iter(&self) -> HashIndexIter {
        match self {
            &HashIndexLeaf::Single(value) => HashIndexIter::Single{ value, returned: false },
            &HashIndexLeaf::Many(ref index) => HashIndexIter::Leaf(index.get_dangerous_keys()),
        }
    }
}

//-------------------------------------------------------------------------
// HashIndexLevel
//-------------------------------------------------------------------------

#[derive(Clone)]
pub struct HashIndexLevel {
    e: HashMap<Interned, HashIndexLeaf, MyHasher>,
    v: HashMap<Interned, HashIndexLeaf, MyHasher>,
    size: u32,
}

impl HashIndexLevel {
    pub fn new() -> HashIndexLevel {
        HashIndexLevel { e: HashMap::default(), v: HashMap::default(), size: 0 }
    }

    pub fn insert(&mut self, e: Interned, v:Interned) -> bool {
        let added = match self.e.entry(e) {
            Entry::Occupied(mut o) => {
                o.get_mut().insert(v);
                true
            }
            Entry::Vacant(o) => {
                o.insert(HashIndexLeaf::Single(v));
                true
            },
        };
        if added {
            self.size += 1;
            match self.v.entry(v) {
                Entry::Occupied(mut o) => {
                    o.get_mut().insert(e);
                }
                Entry::Vacant(o) => {
                    o.insert(HashIndexLeaf::Single(e));
                },
            };
        }
        added
    }

    pub fn remove(&mut self, e:Interned, v:Interned) -> bool {
        let added = match self.e.entry(e) {
            Entry::Occupied(mut o) => {
                let is_empty = o.get_mut().remove(v);
                if is_empty {
                    o.remove_entry();
                }
                true
            }
            Entry::Vacant(_) => {
                false
            },
        };
        if added {
            self.size -= 1;
            match self.v.entry(v) {
                Entry::Occupied(mut o) => {
                    let is_empty = o.get_mut().remove(e);
                    if is_empty {
                        o.remove_entry();
                    }
                }
                Entry::Vacant(_) => { },
            };
        }
        added
    }

    pub fn check(&self, e: Interned, v:Interned) -> bool {
        if e > 0 && v > 0 {
            match self.e.get(&e) {
                Some(leaf) => leaf.check(v),
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

    pub fn find_values(&self, e:Interned) -> Option<HashIndexIter>  {
        match self.e.get(&e) {
            Some(leaf) => Some(leaf.iter()),
            None => None,
        }
    }

    pub fn find_entities(&self, v:Interned) -> Option<HashIndexIter> {
        match self.v.get(&v) {
            Some(leaf) => Some(leaf.iter()),
            None => None,
        }
    }

    pub fn get(&self, e:Interned, v:Interned) -> Option<HashIndexIter> {
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
                    Some(HashIndexIter::Middle(self.e.get_dangerous_keys()))
                } else {
                    None
                }
            } else {
                if vs_len > 0 {
                    Some(HashIndexIter::Middle(self.v.get_dangerous_keys()))
                } else {
                    None
                }
            }
        }
    }

    pub fn propose(&self, iter:&mut EstimateIter, e:Interned, v:Interned) {
        match *iter {
            EstimateIter::Scan { ref mut estimate, ref mut output, ref mut iter, .. } => {
                if e > 0 {
                    if let Some(hash_iter) = self.find_values(e) {
                        *estimate = hash_iter.len() as u32;
                        *iter = hash_iter;
                        *output = 2;
                    }
                } else if v > 0 {
                    if let Some(hash_iter) = self.find_entities(v) {
                        *estimate = hash_iter.len() as u32;
                        *iter = hash_iter;
                        *output = 0;
                    }
                } else {
                    let es_len = self.e.len();
                    let vs_len = self.v.len();
                    if es_len < vs_len {
                        // only if we have values do we fill in the iter
                        if es_len > 0 {
                            let hash_iter = self.e.get_dangerous_keys();
                            *estimate = hash_iter.len() as u32;
                            *iter = HashIndexIter::Middle(hash_iter);
                            *output = 0;
                        }
                    } else {
                        // only if we have values do we fill in the iter
                        if vs_len > 0 {
                            let hash_iter = self.v.get_dangerous_keys();
                            *estimate = hash_iter.len() as u32;
                            *iter = HashIndexIter::Middle(hash_iter);
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
// Generic distinct
//-------------------------------------------------------------------------

pub fn generic_distinct<F>(counts:&mut Vec<Count>, input_count:Count, input_round:Round, mut insert:F)
    where F: FnMut(Round, Count)
{
    // println!("Pre counts {:?}", counts);
    ensure_len(counts, (input_round + 1) as usize);
    let counts_len = counts.len() as u32;
    let min = cmp::min(input_round + 1, counts_len);
    let mut cur_count = 0;
    for ix in 0..min {
        cur_count += counts[ix as usize];
    };

    // handle Infinity/-Infinity for commits at round 0
    if input_round == 0 {
        if input_count < 0 {
            cur_count = input_count.abs();
            counts[input_round as usize] = cur_count; // Cancel out the addition we do below.
        } else if cur_count < 0 {
            cur_count = 0;
        }
    }

    let next_count = cur_count + input_count;
    let delta = get_delta(cur_count, next_count);
    if delta != 0 {
        insert(input_round, delta);
    }

    cur_count = next_count;
    counts[input_round as usize] += input_count;

    for round_ix in (input_round + 1)..counts_len {
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
            insert(round_ix, final_delta);
        }

        cur_count = next_count_changed;
    }
    // println!("Post counts {:?}", counts);
}

//-------------------------------------------------------------------------
// HashIndex
//-------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct RoundEntry {
    inserted: bool,
    pub rounds: Vec<i32>,
    active_rounds: Vec<i32>,
}

impl RoundEntry {
    pub fn update_active(&mut self, round:Round, count:Count) {
        if count > 0 {
            let pos = match self.active_rounds.binary_search(&(round as i32)) {
                Ok(_) => panic!("Adding a round that is already in the index: {:?}", round),
                Err(pos) => pos,
            };
            self.active_rounds.insert(pos, round as i32);
        } else {
            let (pos, remove) = match self.active_rounds.binary_search(&(round as i32)) {
                Ok(pos) => (pos, true),
                Err(pos) => (pos, false),
            };
            if remove {
                self.active_rounds.remove(pos);
            } else {
                self.active_rounds.insert(pos, (round as i32) * -1);
            }
        }
    }
}

pub struct HashIndex {
    a: HashMap<Interned, HashIndexLevel, MyHasher>,
    pub eavs: HashMap<(Interned, Interned, Interned), RoundEntry, MyHasher>,
    empty: Vec<i32>,
    pub size: u32,
}

impl HashIndex {
    pub fn new() -> HashIndex{
        HashIndex { a: HashMap::default(), eavs: HashMap::default(), size: 0, empty: vec![] }
    }

    pub fn insert(&mut self, e: Interned, a:Interned, v:Interned, round:Round) -> bool {
        let added = match self.eavs.entry((e,a,v)) {
            Entry::Occupied(mut entry) => {
                let info = entry.get_mut();
                let needs_insert = info.inserted;
                info.inserted = true;
                info.update_active(round, 1);
                !needs_insert
            }
            Entry::Vacant(o) => {
                o.insert(RoundEntry { inserted: true, rounds: vec![], active_rounds:vec![round as i32] });
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
                    let mut level = HashIndexLevel::new();
                    level.insert(e,v);
                    o.insert(level);
                    true
                },
            };
        }
        added
    }

    pub fn remove(&mut self, e: Interned, a:Interned, v:Interned, round:Round) -> bool {
        let removed = match self.eavs.entry((e,a,v)) {
            Entry::Occupied(mut entry) => {
                // There are two possibilities we have to worry about here. One is that we have
                // completely removed all traces of this eav, which we identify by having the
                // rounds array contain all zeros. The other is that there are rounds the index
                // doesn't know about yet, but as far as the index is concerned, this value isn't
                // in here. In the latter case, we need to remove the value from the index, but
                // leave the entry containing the round information in. In the former, we nuke the
                // whole entry.
                let (should_remove_entry, remove_indexed) = {
                    let info = entry.get_mut();
                    info.update_active(round, -1);
                    (!info.rounds.iter().any(|x| *x != 0), info.active_rounds.len() == 0)
                };
                if should_remove_entry && remove_indexed {
                    entry.remove_entry();
                }
                remove_indexed
            }
            Entry::Vacant(_) => { false },
        };
        if removed {
            self.size -= 1;
            match self.a.entry(a) {
                Entry::Occupied(mut o) => {
                    let mut level = o.get_mut();
                    level.remove(e, v);
                }
                Entry::Vacant(_) => { },
            };
        }
        removed
    }

    #[inline(never)]
    pub fn check(&self, e: Interned, a:Interned, v:Interned) -> bool {
        if e > 0 && a > 0 && v > 0 {
            self.eavs.contains_key(&(e,a,v))
        } else if a > 0 {
            match self.a.get(&a) {
                Some(level) => level.check(e, v),
                None => false,
            }
        } else {
            for level in self.a.values() {
                if level.check(e, v) {
                    return true;
                }
            }
            false
        }
    }

    pub fn is_available(&self, e:Interned, a:Interned, v:Interned) -> bool {
        if e == 0 || a == 0 || v == 0 {
            panic!("Can't check availability of an unformed EAV ({}, {}, {})", e, a, v);
        }
        match self.eavs.get(&(e,a,v)) {
            Some(rounds) => {
                rounds.rounds.iter().fold(0, |prev, x| prev + x) > 0
            }
            None => false,
        }
    }

    pub fn get(&self, e:Interned, a:Interned, v:Interned) -> Option<HashIndexIter> {
        if a == 0 {
            if self.a.len() > 0 {
                Some(HashIndexIter::Root(self.a.get_dangerous_keys()))
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

    pub fn propose(&self, iter: &mut EstimateIter, e:Interned, a:Interned, v:Interned) {
        if a == 0 {
            // @NOTE: In the case where we have an arbitrary lookup we may propose values that may not be correct, but
            // get_rounds should handle this for us.
            // if e != 0 && v != 0 {
            //     panic!("ERROR: Proposing for unsafe a");
            // }
            match iter {
                &mut EstimateIter::Scan { ref mut estimate, ref mut iter, ref mut output, .. } => {
                    let attrs_iter = self.a.get_dangerous_keys();
                    *output = 1;
                    *estimate = attrs_iter.len() as u32;
                    *iter = HashIndexIter::Root(attrs_iter);
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

    //---------------------------------------------------------------------
    // Distinct methods
    //---------------------------------------------------------------------

    pub fn insert_distinct(&mut self, e:Interned, a:Interned, v:Interned, round:Round, count:Count) {
        let key = (e, a, v);
        let needs_insert = {
            let info = self.eavs.entry(key).or_insert_with(|| RoundEntry { inserted:false, rounds: vec![], active_rounds:vec![] });
            let ref mut counts = info.rounds;
            ensure_len(counts, (round + 1) as usize);
            if round == 0 {
                if count < 0 {
                    counts[round as usize] = 0;
                } else {
                    counts[round as usize] = 1;
                }
            } else {
                counts[round as usize] += count;
            }
            // if the passed count is less than 0, this is actually a remove and we should send it
            // through that path
            !info.inserted || count < 0
        };
        if needs_insert {
            if count > 0 {
                self.insert(e,a,v,round);
            } else if count < 0 {
                self.remove(e,a,v,round);
            }
        }
    }

    pub fn distinct_iter(&self, e:Interned, a:Interned, v:Interned) -> DistinctIter {
        let key = (e, a, v);
        match self.eavs.get(&key) {
            Some(&RoundEntry { ref active_rounds, .. }) => DistinctIter::new(active_rounds),
            None => DistinctIter::new(&self.empty),
        }
    }

    pub fn distinct(&mut self, input:&Change, rounds:&mut RoundHolder) {
        let key = (input.e, input.a, input.v);
        let insert = |round, delta| {
            rounds.insert(input.with_round_count(round, delta));
        };
        let needs_remove = {
            let entry = self.eavs.entry(key).or_insert_with(|| RoundEntry { inserted:false, rounds: vec![], active_rounds:vec![] });
            generic_distinct(&mut entry.rounds, input.count, input.round, insert);
            entry.active_rounds.len() == 0 && !entry.rounds.iter().any(|x| *x != 0)
        };
        if needs_remove {
            self.eavs.remove(&(input.e, input.a, input.v));
        }
    }
}

//-------------------------------------------------------------------------
// Distinct Iter
//-------------------------------------------------------------------------

pub struct DistinctIter<'a> {
    ix: usize,
    len: usize,
    rounds: &'a Vec<i32>,
}

impl<'a> DistinctIter<'a> {
    pub fn new(rounds:&'a Vec<i32>) -> DistinctIter<'a> {
        DistinctIter { rounds, ix: 0, len: rounds.len() }
    }
}

impl<'a> Iterator for DistinctIter<'a> {
    type Item = (Round, Count);

    fn next(&mut self) -> Option<(Round, Count)> {
        if self.ix >= self.len { return None; }
        let cur = self.rounds[self.ix];
        self.ix += 1;
        let count = if cur < 0 { -1 } else { 1 };
        Some((cur.abs() as u32, count))
    }
}

//-------------------------------------------------------------------------
// Intermediate Index
//-------------------------------------------------------------------------

#[derive(Debug)]
pub enum AggregateEntry {
    Empty,
    Result(f32),
    Counted { sum: f32, count: f32, result: f32 },
}

impl AggregateEntry {
    pub fn get_result(&self) -> f32 {
        match self {
            &AggregateEntry::Result(res) => res,
            &AggregateEntry::Counted { result, .. } => result,
            &AggregateEntry::Empty => panic!("Asked for result of AggregateEntry::Empty")
        }
    }
}

enum IntermediateLevel {
    Value(HashMap<Vec<Interned>, RoundEntry, MyHasher>),
    KeyOnly(RoundEntry),
    SumAggregate(BTreeMap<Round, AggregateEntry>),
}

pub struct IntermediateIndex {
    index: HashMap<Vec<Interned>, IntermediateLevel, MyHasher>,
    pub rounds: HashMap<Round, HashMap<Vec<Interned>, IntermediateChange, MyHasher>, MyHasher>,
    round_buffer: Vec<(Vec<Interned>, Vec<Interned>, Vec<Interned>, Round, Count, bool)>,
    empty: Vec<i32>,
}

// FIXME: attack of the clones.
fn intermediate_distinct(index:&mut HashMap<Vec<Interned>, IntermediateLevel, MyHasher>,
                         rounds:&mut HashMap<Round, HashMap<Vec<Interned>, IntermediateChange, MyHasher>, MyHasher>,
                         full_key:Vec<Interned>, key:Vec<Interned>, value:Vec<Interned>,
                         round:Round, count:Count, negate:bool) {
    let cloned = full_key.clone();
    let value_pos = key.len();
    let insert = |round, delta| {
        match rounds.entry(round) {
            Entry::Occupied(mut ent) => {
                let cur = ent.get_mut();
                let val = cur.entry(cloned.clone()).or_insert_with(|| {
                    IntermediateChange { key:cloned.clone(), round, count:0, negate, value_pos }
                });
                val.count += delta;
            }
            Entry::Vacant(ent) => {
                let mut neue = HashMap::default();
                neue.insert(cloned.clone(), IntermediateChange { key:cloned.clone(), round, count:delta, negate, value_pos });
                ent.insert(neue);
            }
        }
    };
    let entry = index.entry(key.clone()).or_insert_with(|| {
        let entry = RoundEntry { inserted:false, rounds: vec![], active_rounds: vec![] };
        if value.len() == 0 {
            IntermediateLevel::KeyOnly(entry)
        } else {
            let mut sub = HashMap::default();
            sub.insert(value.clone(), entry);
            IntermediateLevel::Value(sub)
        }
    });
    let counts = match entry {
        &mut IntermediateLevel::KeyOnly(ref mut entry) => &mut entry.rounds,
        &mut IntermediateLevel::Value(ref mut lookup) => {
            &mut lookup.entry(value.clone())
                .or_insert_with(|| RoundEntry { inserted:false, rounds: vec![], active_rounds:vec![] }).rounds
        }
        &mut IntermediateLevel::SumAggregate(..) => {
            unimplemented!();
        }
    };
    generic_distinct(counts, count, round, insert);
}

pub fn insert_change(rounds: &mut HashMap<Round, HashMap<Vec<Interned>, IntermediateChange, MyHasher>, MyHasher>, mut change: IntermediateChange) {
    match rounds.entry(change.round) {
        Entry::Occupied(mut ent) => {
            let cur = ent.get_mut();
            let delta = change.count;
            change.count = 0;
            let val = cur.entry(change.key.clone()).or_insert(change);
            val.count += delta;
        }
        Entry::Vacant(ent) => {
            let mut neue = HashMap::default();
            neue.insert(change.key.clone(), change);
            ent.insert(neue);
        }
    }
}

impl IntermediateIndex {

    pub fn new() -> IntermediateIndex {
        IntermediateIndex { index: HashMap::default(), rounds: HashMap::default(), round_buffer:vec![], empty: vec![] }
    }

    pub fn check(&self, key:&Vec<Interned>, value:&Vec<Interned>) -> bool {
        match self.index.get(key) {
            Some(level) => {
                match level {
                    &IntermediateLevel::KeyOnly(ref entry) => entry.active_rounds.len() > 0,
                    &IntermediateLevel::Value(ref lookup) => {
                        match lookup.get(value) {
                            Some(entry) => entry.active_rounds.len() > 0,
                            _ => false
                        }
                    },
                    &IntermediateLevel::SumAggregate(..) => {
                        unimplemented!();
                    }
                }
            }
            None => false,
        }
    }

    pub fn distinct_iter(&self, key:&Vec<Interned>, value:&Vec<Interned>) -> DistinctIter {
        match self.index.get(key) {
            Some(level) => {
                match level {
                    &IntermediateLevel::KeyOnly(ref entry) => DistinctIter::new(&entry.active_rounds),
                    &IntermediateLevel::Value(ref lookup) => {
                        match lookup.get(value) {
                            Some(ref entry) => DistinctIter::new(&entry.active_rounds),
                            None => DistinctIter::new(&self.empty),
                        }
                    }
                    &IntermediateLevel::SumAggregate(..) => {
                        unimplemented!();
                    }
                }
            }
            None => DistinctIter::new(&self.empty),
        }
    }

    pub fn aggregate(&mut self, interner:&mut Interner, group:Vec<Interned>, value:Vec<Internable>, round:Round, action:AggregateFunction, out:Vec<Interned>) {
        let cur = self.index.entry(group).or_insert_with(|| IntermediateLevel::SumAggregate(BTreeMap::new()));
        if let &mut IntermediateLevel::SumAggregate(ref mut rounds) = cur {
            match rounds.entry(round) {
                btree_map::Entry::Occupied(mut ent) => {
                    let cur_aggregate = ent.get_mut();
                    let prev = cur_aggregate.get_result();
                    action(cur_aggregate, value.clone());
                    let neue = cur_aggregate.get_result();
                    if neue != prev {
                        // add a remove for the previous value
                        let mut to_remove = out.clone();
                        let prev_interned = interner.number_id(prev);
                        to_remove.push(prev_interned);
                        self.round_buffer.push((to_remove, out.clone(), vec![prev_interned], round, -1, false));
                        // add an add for the new value
                        let mut to_add = out.clone();
                        let cur_interned = interner.number_id(neue);
                        to_add.push(cur_interned);
                        self.round_buffer.push((to_add, out.clone(), vec![cur_interned], round, 1, false));
                    }
                }
                btree_map::Entry::Vacant(ent) => {
                    let mut cur_aggregate = AggregateEntry::Empty;
                    action(&mut cur_aggregate, value.clone());
                    // add an add for the new value
                    let mut to_add = out.clone();
                    let cur_interned = interner.number_id(cur_aggregate.get_result());
                    to_add.push(cur_interned);
                    self.round_buffer.push((to_add, out.clone(), vec![cur_interned], round, 1, false));
                    ent.insert(cur_aggregate);
                }
            }
            for (k, v) in rounds.range_mut(round+1..) {
                let prev = v.get_result();
                action(v, value.clone());
                let neue = v.get_result();
                if neue != prev {
                    // add a remove for the previous value
                    let mut to_remove = out.clone();
                    let prev_interned = interner.number_id(prev);
                    to_remove.push(prev_interned);
                    self.round_buffer.push((to_remove, out.clone(), vec![prev_interned], *k, -1, false));
                    // add an add for the new value
                    let mut to_add = out.clone();
                    let cur_interned = interner.number_id(neue);
                    to_add.push(cur_interned);
                    self.round_buffer.push((to_add, out.clone(), vec![cur_interned], *k, 1, false));
                }
            }
        }
    }

    pub fn propose(&self, iter: &mut EstimateIter, key:Vec<Interned>) {
        match iter {
            &mut EstimateIter::Intermediate { ref mut estimate, ref mut iter, .. } => {
                match self.index.get(&key) {
                    Some(&IntermediateLevel::Value(ref lookup)) => {
                        *estimate = lookup.len() as u32;
                        *iter = Some(lookup.get_dangerous_keys());
                    },
                    Some(&IntermediateLevel::KeyOnly(_)) => { *estimate = 0 },
                    Some(&IntermediateLevel::SumAggregate(_)) => {
                        unimplemented!();
                    },
                    None => { *estimate = 0; }

                }
            }
            _ => panic!("Non intermediate iterator passed to intermediate propose")
        }
    }

    pub fn update_active_rounds(&mut self, change: &IntermediateChange) {
        let (key, value) = change.key.split_at(change.value_pos);
        let count = change.count;
        let should_remove = match self.index.get_mut(key) {
            Some(&mut IntermediateLevel::KeyOnly(ref mut info)) => {
                info.update_active(change.round, count);
                !info.rounds.iter().any(|x| *x != 0)
            }
            Some(&mut IntermediateLevel::Value(ref mut lookup)) => {
                let remove = match lookup.get_mut(value) {
                    Some(ref mut info) => {
                        info.update_active(change.round, count);
                        !info.rounds.iter().any(|x| *x != 0)
                    },
                    None => panic!("Updating active rounds for an intermediate that doesn't exist: {:?}", change)
                };
                if remove {
                    lookup.remove(value);
                }
                lookup.len() == 0
            }
            Some(&mut IntermediateLevel::SumAggregate(_)) => { unimplemented!(); },
            None => { panic!("Updating active rounds for an intermediate that doesn't exist: {:?}", change) }
        };
        if should_remove {
            self.index.remove(key);
        }
    }

    pub fn buffer(&mut self, full_key:Vec<Interned>, key:Vec<Interned>, value:Vec<Interned>, round:Round, count:Count, negate:bool) {
        // println!("    -> Intermediate! {:?} {:?} {:?}", full_key, round, count);
        self.round_buffer.push((full_key, key, value, round, count, negate));
    }

    pub fn consume_round(&mut self) -> Round {
        let mut max = 0;
        for (full_key, key, value, round, count, negate) in self.round_buffer.drain(..) {
            max = cmp::max(round, max);
            intermediate_distinct(&mut self.index, &mut self.rounds, full_key, key, value, round, count, negate);
        }
        max
    }

    pub fn distinct(&mut self, full_key:Vec<Interned>, key:Vec<Interned>, value:Vec<Interned>, round:Round, count:Count, negate:bool) {
        intermediate_distinct(&mut self.index, &mut self.rounds, full_key, key, value, round, count, negate);
    }
}

//-------------------------------------------------------------------------
// Collapsed changes
//-------------------------------------------------------------------------

pub struct CollapsedChanges {
    changes: HashMap<(Interned, Interned, Interned, Round), Change, MyHasher>
}

impl CollapsedChanges {
    pub fn new() -> CollapsedChanges {
        CollapsedChanges { changes: HashMap::default() }
    }

    pub fn insert(&mut self, change:Change) {
        let key = (change.e, change.a, change.v, change.round);
        match self.changes.entry(key) {
            Entry::Occupied(mut o) => {
                o.get_mut().count += change.count;
            }
            Entry::Vacant(o) => {
                o.insert(change);
            }
        };
    }

    pub fn iter<'a>(&'a self) -> Box<Iterator<Item=&'a Change> + 'a> {
        Box::new(self.changes.values().filter(|x| x.count != 0))
    }

    pub fn drain<'a>(&'a mut self) -> Box<Iterator<Item=Change> + 'a> {
        Box::new(self.changes.drain().map(|kv| kv.1).filter(|x| x.count != 0))
    }

    pub fn clear(&mut self) {
        self.changes.clear();
    }
}

//-------------------------------------------------------------------------
// Watch Index
//-------------------------------------------------------------------------

pub struct WatchIndex {
    cur: HashMap<Vec<Interned>, Count, MyHasher>,
    next: HashMap<Vec<Interned>, Count, MyHasher>,
}

#[derive(Debug)]
pub struct WatchDiff {
    pub adds: Vec<Vec<Interned>>,
    pub removes: Vec<Vec<Interned>>,
}

fn update_watch_count(index:&mut HashMap<Vec<Interned>, Count, MyHasher>, key:Vec<Interned>, count:Count) -> (Count, Count) {
    match index.entry(key) {
        Entry::Occupied(mut o) => {
            let prev = *o.get();
            let updated = prev + count;
            if updated != 0 {
                o.insert(updated);
            } else {
                o.remove_entry();
            }
            (prev, updated)
        }
        Entry::Vacant(o) => {
            o.insert(count);
            (0, count)
        }
    }
}

impl WatchIndex {
    pub fn new() -> WatchIndex {
        WatchIndex { cur: HashMap::default(), next: HashMap::default() }
    }

    pub fn dirty(&self) -> bool {
       self.next.len() > 0
    }

    pub fn insert(&mut self, key: Vec<Interned>, count: Count) {
        update_watch_count(&mut self.next, key, count);
    }

    pub fn reconcile(&mut self) -> WatchDiff {
        let mut adds = vec![];
        let mut removes = vec![];
        for (k, v) in self.next.drain() {
            let cloned = k.clone();
            let (prev, neue) = update_watch_count(&mut self.cur, k, v);
            if prev == 0 && neue > 0 {
                adds.push(cloned);
            } else if prev > 0 && neue == 0 {
                removes.push(cloned);
            }
        }
        WatchDiff { adds, removes }
    }
}
