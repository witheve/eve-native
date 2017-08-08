//-------------------------------------------------------------------------
// Indexes
//-------------------------------------------------------------------------

// use std::collections::HashMap;
use ops::{EstimateIter, OutputingIter, Change, RoundHolder, Interned, Round, Count, IntermediateChange, Internable, Interner, AggregateFunction};
use std::cmp;

extern crate fnv;
use indexes::fnv::FnvHasher;
use std::hash::{BuildHasherDefault};
use std::collections::hash_map::{Entry};
use std::iter::{Iterator, self};
use std::collections::{BTreeMap, HashMap, BTreeSet, btree_map};
use compiler::{FunctionKind};

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
// HashIndexLeaf
//-------------------------------------------------------------------------

#[derive(Clone)]
pub enum HashIndexLeaf {
    Single(Interned),
    Many(HashMap<Interned, (), MyHasher>),
}

impl HashIndexLeaf {
    pub fn insert(&mut self, neue_value:Interned) -> bool {
        match self {
            &mut HashIndexLeaf::Single(prev) => {
                if prev != neue_value {
                    let mut neue = HashMap::default();
                    neue.insert(prev, ());
                    neue.insert(neue_value, ());
                    *self = HashIndexLeaf::Many(neue);
                    true
                } else {
                    false
                }
            },
            &mut HashIndexLeaf::Many(ref mut prev) => {
                prev.insert(neue_value, ()).is_none()
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

    pub fn iter<'a>(&'a self) -> Box<ExactSizeIterator<Item=Interned> + 'a> {
        match self {
            &HashIndexLeaf::Single(value) => Box::new(iter::once(value)),
            &HashIndexLeaf::Many(ref index) => Box::new(index.keys().cloned()),
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
                o.get_mut().insert(v)
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
            // self.size -= 1;
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

    pub fn find_values<'a>(&'a self, e:Interned) -> Option<Box<ExactSizeIterator<Item=Interned> + 'a>> {
        match self.e.get(&e) {
            Some(leaf) => Some(leaf.iter()),
            None => None,
        }
    }

    pub fn find_entities<'a>(&'a self, v:Interned) -> Option<Box<ExactSizeIterator<Item=Interned> + 'a>> {
        match self.v.get(&v) {
            Some(leaf) => Some(leaf.iter()),
            None => None,
        }
    }

    pub fn get<'a>(&'a self, e:Interned, v:Interned) -> Option<Box<ExactSizeIterator<Item=Interned> + 'a>> {
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
                    Some(Box::new(self.e.keys().cloned()))
                } else {
                    None
                }
            } else {
                if vs_len > 0 {
                    Some(Box::new(self.v.keys().cloned()))
                } else {
                    None
                }
            }
        }
    }

    pub fn propose(&self, iter:&mut EstimateIter, e:Interned, v:Interned) -> bool {
        if e > 0 {
            if let Some(hash_iter) = self.find_values(e) {
                let estimate = hash_iter.len();
                if iter.is_better(estimate) {
                    iter.estimate = estimate;
                    iter.iter = OutputingIter::Single(2, OutputingIter::make_ptr(Box::new(hash_iter)));
                    true
                } else {
                    false
                }
            } else {
                iter.estimate = 0;
                iter.iter = OutputingIter::Empty;
                true
            }
        } else if v > 0 {
            if let Some(hash_iter) = self.find_entities(v) {
                let estimate = hash_iter.len();
                if iter.is_better(estimate) {
                    iter.estimate = estimate;
                    iter.iter = OutputingIter::Single(0, OutputingIter::make_ptr(Box::new(hash_iter)));
                    true
                } else {
                    false
                }
            } else {
                iter.estimate = 0;
                iter.iter = OutputingIter::Empty;
                true
            }
        } else {
            let es_len = self.e.len();
            let vs_len = self.v.len();
            if es_len < vs_len {
                let hash_iter = Box::new(self.e.keys().cloned());
                if iter.is_better(es_len) {
                    iter.estimate = es_len;
                    iter.iter = OutputingIter::Single(0, OutputingIter::make_ptr(hash_iter));
                    true
                } else {
                    false
                }
            } else {
                let hash_iter = Box::new(self.v.keys().cloned());
                if iter.is_better(vs_len) {
                    iter.estimate = vs_len;
                    iter.iter = OutputingIter::Single(2, OutputingIter::make_ptr(hash_iter));
                    true
                } else {
                    false
                }
            }
        }
    }
}

//-------------------------------------------------------------------------
// Generic distinct
//-------------------------------------------------------------------------

pub fn generic_distinct<F>(counts:&mut Vec<Count>, mut input_count:Count, input_round:Round, mut insert:F, handle_commits: bool)
    where F: FnMut(Round, Count)
{
    ensure_len(counts, (input_round + 1) as usize);
    let counts_len = counts.len() as u32;
    let min = cmp::min(input_round + 1, counts_len);
    let mut cur_count = 0;
    for ix in 0..min {
        cur_count += counts[ix as usize];
    };

    // handle Infinity/-Infinity for commits at round 0
    if handle_commits && input_round == 0 {
        if cur_count == 0 && input_count < 0 {
            input_count = 0;
        } else if input_count < 0 {
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

fn update_active_rounds_vec(active_rounds: &mut Vec<i32>, round:Round, count:Count) {
        let round_i32 = round as i32;
        if count > 0 {
            match active_rounds.binary_search_by(|probe| probe.abs().cmp(&round_i32)) {
                Ok(pos) =>  {
                    let cur = active_rounds[pos];
                    if cur < 0 {
                        let diff = round as i32 * count;
                        if cur + diff == 0 {
                            active_rounds.remove(pos);
                        } else {
                            active_rounds[pos] = round as i32;
                        }
                    } else {
                        panic!("Adding a round that is already in the index: {:?}", round)
                    }
                }
                Err(pos) => active_rounds.insert(pos, round as i32),
            };
        } else {
            let (pos, remove) = match active_rounds.binary_search_by(|probe| probe.abs().cmp(&round_i32)) {
                Ok(pos) => (pos, true),
                Err(pos) => (pos, false),
            };
            if remove {
                active_rounds.remove(pos);
                // we might be doing a swing from positive to negative, which we'd see by getting a
                // count of -2 instead of -1. If so, we need to insert the negative.
                if count < -1 {
                    active_rounds.insert(pos, (round as i32) * -1);
                }
            } else {
                active_rounds.insert(pos, (round as i32) * -1);
            }
        }
}

impl RoundEntry {
    pub fn update_active(&mut self, round:Round, count:Count) {
        update_active_rounds_vec(&mut self.active_rounds, round, count);
    }
}

pub struct HashIndex {
    a: HashMap<Interned, HashIndexLevel, MyHasher>,
    pub size: u32,
}

impl HashIndex {
    pub fn new() -> HashIndex{
        HashIndex { a: HashMap::default(), size: 0 }
    }

    pub fn insert(&mut self, e: Interned, a:Interned, v:Interned) -> bool {
        let added = match self.a.entry(a) {
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
        if added { self.size += 1 };
        added
    }

    pub fn remove(&mut self, e: Interned, a:Interned, v:Interned) -> bool {
        let removed = match self.a.entry(a) {
            Entry::Occupied(mut o) => {
                let mut level = o.get_mut();
                level.remove(e, v)
            }
            Entry::Vacant(_) => { false },
        };
        if removed { self.size -= 1; };
        removed
    }

    #[inline(never)]
    pub fn check(&self, e: Interned, a:Interned, v:Interned) -> bool {
        if a > 0 {
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

    pub fn fast_check(&self, distinct: &DistinctIndex, e: Interned, a:Interned, v:Interned) -> bool {
        if e > 0 && a > 0 && v > 0 {
            distinct.check(e, a, v)
        } else {
            self.check(e,a,v)
        }
    }

    pub fn get<'a>(&'a self, e:Interned, a:Interned, v:Interned) -> Option<Box<ExactSizeIterator<Item=Interned> + 'a>> {
        if a == 0 {
            if self.a.len() > 0 {
                Some(Box::new(self.a.keys().cloned()))
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

    pub fn propose(&self, iter: &mut EstimateIter, e:Interned, a:Interned, v:Interned) -> bool {
        if a == 0 {
            // @NOTE: In the case where we have an arbitrary lookup we may propose values that may not be correct, but
            // get_rounds should handle this for us.
            let attrs_iter = self.a.keys();
            let estimate = attrs_iter.len();
            if iter.is_better(estimate)  {
                iter.estimate = estimate;
                iter.iter = OutputingIter::Single(1, OutputingIter::make_ptr(Box::new(attrs_iter.cloned())));
                true
            } else {
                false
            }
        } else {
            let level = match self.a.get(&a) {
                None => {
                    iter.estimate = 0;
                    iter.iter = OutputingIter::Empty;
                    return true;
                },
                Some(level) => level,
            };
            level.propose(iter, e, v)
        }
    }
}

//-------------------------------------------------------------------------
// Distinct Index
//-------------------------------------------------------------------------

pub struct DistinctIndex {
    pub eavs: HashMap<(Interned, Interned, Interned), RoundEntry, MyHasher>,
    empty: Vec<i32>,
}

impl DistinctIndex {
    pub fn new() -> DistinctIndex {
        DistinctIndex { eavs: HashMap::default(), empty: vec![] }
    }

    pub fn insert_active(&mut self, e: Interned, a:Interned, v:Interned, round:Round) -> bool {
        match self.eavs.entry((e,a,v)) {
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
            }
        }
    }

    pub fn remove_active(&mut self, e: Interned, a:Interned, v:Interned, round:Round) -> bool {
        match self.eavs.entry((e,a,v)) {
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
        }
    }

    pub fn check(&self, e: Interned, a:Interned, v:Interned) -> bool {
        self.eavs.contains_key(&(e,a,v))
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

    pub fn get(&self, e:Interned, a:Interned, v:Interned) -> Option<&RoundEntry> {
        self.eavs.get(&(e,a,v))
    }

    pub fn raw_insert(&mut self, e:Interned, a:Interned, v:Interned, round:Round, count:Count) -> bool {
        let key = (e, a, v);
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
    }

    pub fn iter(&self, e:Interned, a:Interned, v:Interned) -> DistinctIter {
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
            generic_distinct(&mut entry.rounds, input.count, input.round, insert, true);
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

#[derive(Clone)]
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
    SortedSum { items: BTreeSet<Vec<Interned>>, bound: Interned, count: usize, limit: usize },
    Sorted { items: BTreeMap<Vec<Internable>, Vec<Count>>, current_round: Round, current_params:Option<Vec<Internable>>, changes: Vec<(Vec<Internable>, Round, Count)>, limit: usize },
}

impl AggregateEntry {
    pub fn get_result(&self, interner:&mut Interner) -> Vec<Interned> {
        match self {
            &AggregateEntry::Result(res) => vec![interner.number_id(res)],
            &AggregateEntry::Counted { result, .. } => vec![interner.number_id(result)],
            &AggregateEntry::SortedSum {..} => { unimplemented!() },
            &AggregateEntry::Sorted {..} => { unimplemented!() },
            &AggregateEntry::Empty => panic!("Asked for result of AggregateEntry::Empty")
        }
    }
}

enum IntermediateLevel {
    Value(HashMap<Vec<Interned>, RoundEntry, MyHasher>),
    KeyOnly(RoundEntry),
    SumAggregate(BTreeMap<Round, AggregateEntry>),
    SortAggregate(Vec<Round>, AggregateEntry),
}

pub struct IntermediateIndex {
    index: HashMap<Vec<Interned>, IntermediateLevel, MyHasher>,
    pub rounds: HashMap<Round, HashMap<Vec<Interned>, IntermediateChange, MyHasher>, MyHasher>,
    max_round: Round,
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
        &mut IntermediateLevel::SumAggregate(..) => { unimplemented!(); }
        &mut IntermediateLevel::SortAggregate(..) => { unimplemented!(); }
    };
    generic_distinct(counts, count, round, insert, false);
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

type AggregateChange = (Vec<Interned>, Vec<Interned>, Vec<Interned>, Round, Count, bool);

pub fn make_aggregate_change(out:&Vec<Interned>, value:Vec<Interned>, round:Round, count:Count) -> AggregateChange {
    let mut to_change = out.clone();
    to_change.extend(value.iter());
    (to_change, out.clone(), value, round, count, false)
}

pub fn update_aggregate(interner: &mut Interner, changes: &mut Vec<AggregateChange>, out: &Vec<Interned>, action:AggregateFunction, cur_aggregate:&mut AggregateEntry, value:&Vec<Internable>, round:Round) {
    let prev = cur_aggregate.get_result(interner);
    action(cur_aggregate, &value);
    let neue = cur_aggregate.get_result(interner);
    if neue != prev {
        // add a remove for the previous value
        changes.push(make_aggregate_change(&out, prev, round, -1));
        // add an add for the new value
        changes.push(make_aggregate_change(&out, neue, round, 1));
    }
}

impl IntermediateIndex {

    pub fn new() -> IntermediateIndex {
        IntermediateIndex { index: HashMap::default(), rounds: HashMap::default(), empty: vec![], max_round:0 }
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
                    &IntermediateLevel::SumAggregate(..) => { unimplemented!(); }
                    &IntermediateLevel::SortAggregate(..) => { unimplemented!(); }
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
                    &IntermediateLevel::SumAggregate(..) => { unimplemented!(); }
                    &IntermediateLevel::SortAggregate(..) => { unimplemented!(); }
                }
            }
            None => DistinctIter::new(&self.empty),
        }
    }

    pub fn aggregate(&mut self, interner:&mut Interner, group:Vec<Interned>, projection:Vec<Internable>, value:Vec<Internable>, round:Round, count:Count, action:AggregateFunction, out:Vec<Interned>, kind:FunctionKind) {
        let mut changes = vec![];
        {
            let cur = self.index.entry(group).or_insert_with(|| {
                if kind == FunctionKind::Sum {
                    IntermediateLevel::SumAggregate(BTreeMap::new())
                } else {
                    IntermediateLevel::SortAggregate(vec![], AggregateEntry::Sorted { items: BTreeMap::new(), current_round: 0, current_params:None, changes: vec![], limit: 0 })
                }
            });
            match cur {
                &mut IntermediateLevel::SumAggregate(ref mut rounds) => {
                    match rounds.entry(round) {
                        btree_map::Entry::Occupied(mut ent) => {
                            let cur_aggregate = ent.get_mut();
                            update_aggregate(interner, &mut changes, &out, action, cur_aggregate, &value, round);
                        }
                        btree_map::Entry::Vacant(ent) => {
                            let mut cur_aggregate = AggregateEntry::Empty;
                            action(&mut cur_aggregate, &value);
                            // add an add for the new value
                            changes.push(make_aggregate_change(&out, cur_aggregate.get_result(interner), round, 1));
                            ent.insert(cur_aggregate);
                        }
                    }
                    for (k, v) in rounds.range_mut(round+1..) {
                        update_aggregate(interner, &mut changes, &out, action, v, &value, *k);
                    }
                }
                &mut IntermediateLevel::SortAggregate(ref mut rounds, ref mut entry) => {
                    if let &mut AggregateEntry::Sorted { ref mut current_params, .. } = entry {
                        *current_params = Some(value);
                    }
                    let start = match rounds.binary_search(&round) {
                        Ok(pos) =>  { pos }
                        Err(pos) => { rounds.insert(pos, round); pos },
                    };
                    for round in rounds[start..].iter().cloned() {
                        if let &mut AggregateEntry::Sorted { ref mut current_round, .. } = entry {
                            *current_round = round;
                        }
                        action(entry, &projection);
                    }
                    if let &mut AggregateEntry::Sorted { ref mut items, changes:ref mut entry_changes, .. } = entry {
                        // Insert it into the items btree
                        match items.entry(projection) {
                            btree_map::Entry::Occupied(ref mut ent) => {
                                update_active_rounds_vec(ent.get_mut(), round, count);
                            },
                            btree_map::Entry::Vacant(ent) => {
                                ent.insert(vec![round as i32 * count]);
                            }
                        }
                        // and update the rounds to make sure we include this round in the future
                        changes.extend(entry_changes.drain(..).map(|(mut value, round, count)| make_aggregate_change(&out, value.drain(..).map(|x| interner.internable_to_id(x)).collect(), round, count)));
                    }

                }
                _ => { unreachable!() }
            }
        }
        for (full_key, key, value, round, count, negate) in changes {
           self.distinct(full_key, key, value, round, count, negate);
        }
    }

    pub fn propose(&self, iter: &mut EstimateIter, key:Vec<Interned>, outputs: Vec<usize>) -> bool {
        match self.index.get(&key) {
            Some(&IntermediateLevel::Value(ref lookup)) => {
                let estimate = lookup.len();
                if iter.is_better(estimate) {
                    iter.estimate = estimate;
                    // @TODO: This clone is going to really hurt, we should be able to come
                    // up with a way not to need to do this if we can turn these into
                    // references instead of owned values
                    iter.iter = OutputingIter::Multi(outputs, OutputingIter::make_multi_ptr(Box::new(lookup.keys().cloned().collect::<Vec<_>>().into_iter())));
                    true
                } else {
                    false
                }
            },
            Some(&IntermediateLevel::KeyOnly(_)) => {
                iter.estimate = 0;
                iter.iter = OutputingIter::Empty;
                true
            },
            Some(&IntermediateLevel::SumAggregate(_)) => { unimplemented!(); },
            Some(&IntermediateLevel::SortAggregate(..)) => { unimplemented!(); },
            None => {
                iter.iter = OutputingIter::Empty;
                iter.estimate = 0;
                true
            }
        }
    }

    pub fn update_active_rounds(&mut self, change: &IntermediateChange) {
        let (key, value) = change.key.split_at(change.value_pos);
        let count = change.count;
        let should_remove = match self.index.get_mut(key) {
            Some(&mut IntermediateLevel::KeyOnly(ref mut info)) => {
                info.update_active(change.round, count);
                !info.rounds.iter().any(|x| *x != 0) && info.active_rounds.len() == 0
            }
            Some(&mut IntermediateLevel::Value(ref mut lookup)) => {
                let remove = match lookup.get_mut(value) {
                    Some(ref mut info) => {
                        info.update_active(change.round, count);
                        !info.rounds.iter().any(|x| *x != 0) && info.active_rounds.len() == 0
                    },
                    None => panic!("Updating active rounds for an intermediate that doesn't exist: {:?}", change)
                };
                if remove {
                    lookup.remove(value);
                }
                lookup.len() == 0
            }
            Some(&mut IntermediateLevel::SumAggregate(_)) => { unimplemented!(); },
            Some(&mut IntermediateLevel::SortAggregate(..)) => { unimplemented!(); },
            None => { panic!("Updating active rounds for an intermediate that doesn't exist: {:?}", change) }
        };
        if should_remove {
            self.index.remove(key);
        }
    }

    pub fn consume_round(&mut self) -> Round {
        let cur = self.max_round;
        self.max_round = 0;
        cur
    }

    pub fn distinct(&mut self, full_key:Vec<Interned>, key:Vec<Interned>, value:Vec<Interned>, round:Round, count:Count, negate:bool) {
        // println!("    -> Intermediate! {:?} {:?} {:?}", full_key, round, count);
        self.max_round = cmp::max(self.max_round, round);
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
