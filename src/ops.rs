//-------------------------------------------------------------------------
// Ops
//-------------------------------------------------------------------------

extern crate time;
extern crate serde_json;
extern crate bincode;
extern crate term_painter;
extern crate data_encoding;
extern crate urlencoding;
extern crate natord;

use unicode_segmentation::UnicodeSegmentation;

use indexes::{HashIndex, DistinctIter, DistinctIndex, WatchIndex, IntermediateIndex, MyHasher, AggregateEntry,
    CollapsedChanges, RemoteIndex, RemoteChange, RawRemoteChange};
use solver::Solver;
use compiler::{make_block, parse_file, FunctionKind};
use std::collections::{HashMap, HashSet, Bound};
use std::mem::transmute;
use std::cmp::{self, Eq, PartialOrd};
use std::collections::hash_map::{DefaultHasher, Entry};
use std::hash::{Hash, Hasher};
use std::iter::{Iterator};
use std::fmt;
use watchers::{Watcher};
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;
use serde::ser::{Serialize, Serializer};
use serde::de::{Deserialize, Deserializer, Visitor};
use std::error::Error;
use std::thread::{self, JoinHandle};
use std::io::{Write, BufReader, BufWriter};
use std::fs::{OpenOptions, File};
use std::f32::consts::{PI};
use std::mem;
use std::usize;
use rand::{Rng, SeedableRng, XorShiftRng};
use self::data_encoding::base64;
use self::term_painter::ToStyle;
use self::term_painter::Color::*;


//-------------------------------------------------------------------------
// Interned value
//-------------------------------------------------------------------------

pub type Interned = u32;
pub type Round = u32;
pub type TransactionId = u32;
pub type Count = i32;

// When the interner is created, we automatically add the string "tag" to it
// as that is used specifically throughout the code to do filtering and the
// like.
const TAG_INTERNED_ID:Interned = 1;

//-------------------------------------------------------------------------
// Utils
//-------------------------------------------------------------------------

pub fn format_interned(interner:&Interner, v:Interned) -> String {
    let v_str = interner.get_value(v).print();
    if v_str.contains("|") {
        format!("<{}>", v)
    } else {
        v_str
    }
}

pub fn print_block_constraints(block:&Block) {
    println!("\n----------- Constraints ------------[{}] \n", block.name);
    for constraint in block.constraints.iter() {
        println!("  {:?}", constraint);
    }
    println!("");
}

//-------------------------------------------------------------------------
// Change
//-------------------------------------------------------------------------

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ChangeType {
    Insert,
    Remove,
}

#[derive(Debug, Copy, Clone)]
pub struct Change {
    pub e: Interned,
    pub a: Interned,
    pub v: Interned,
    pub n: Interned,
    pub round: Round,
    pub transaction: TransactionId,
    pub count: Count,
}

impl Change {
    pub fn with_round_count(&self, round:Round, count:Count) -> Change {
        Change {e: self.e, a: self.a, v: self.v, n: self.n, round, transaction: self.transaction, count}
    }
    pub fn print(&self, interner:&Interner) -> String {
        let a = interner.get_value(self.a).print();
        let mut v = interner.get_value(self.v).print();
        v = if v.contains("|") { format!("<{}>", self.v) } else { v };
        format!("Change (<{}>, {:?}, {})  {}:{}:{}", self.e, a, v, self.transaction, self.round, self.count)
    }

    pub fn to_raw(&self, interner:&Interner) -> RawChange {
        RawChange {
            e: interner.get_value(self.e).clone(),
            a: interner.get_value(self.a).clone(),
            v: interner.get_value(self.v).clone(),
            n: Internable::Null,
            count: self.count,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RawChange {
    pub e: Internable,
    pub a: Internable,
    pub v: Internable,
    pub n: Internable,
    pub count: Count,
}

impl RawChange {
    pub fn to_change(self, interner: &mut Interner) -> Change {
       Change {
           e: interner.internable_to_id(self.e),
           a: interner.internable_to_id(self.a),
           v: interner.internable_to_id(self.v),
           n: interner.internable_to_id(self.n),
           round: 0,
           transaction: 0,
           count: self.count,
       }
    }
}

#[derive(Debug, Clone)]
pub struct IntermediateChange {
    pub key: Vec<Interned>,
    pub count: Count,
    pub round: Round,
    pub value_pos: usize,
    pub negate: bool,
}

//-------------------------------------------------------------------------
// Block
//-------------------------------------------------------------------------

#[derive(Debug)]
pub enum PipeShape {
    Scan(Interned, Interned, Interned),
    Intermediate(Interned),
    Remote(Interned),
}

#[derive(Debug)]
pub struct Block {
    pub name: String,
    pub block_id: Interned,
    pub constraints: Vec<Constraint>,
    pub solver: Option<Solver>,
    pub shapes: Vec<Vec<PipeShape>>
}

impl Block {

    pub fn new(interner:&mut Interner, name:&str, block_id:Interned, constraints:Vec<Constraint>) -> Block {
       let mut me = Block { name:name.to_string(), block_id, constraints, solver:None, shapes: vec![] };
       let shapes = me.to_shapes();
       me.shapes.extend(shapes);
       me.solver = Some(Solver::new(interner, block_id, 0, None, &me.constraints));
       me
    }

    pub fn get_block_scans(&self) -> Vec<&Constraint> {
        self.constraints.iter().filter(|constraint| {
            match constraint {
                &&Constraint::Scan {..} => true,
                &&Constraint::LookupCommit {..} => true,
                &&Constraint::LookupRemote {..} => true,
                &&Constraint::AntiScan {..} => true,
                &&Constraint::IntermediateScan {..} => true,
                _ => false
            }
        }).collect()
    }

    pub fn run(&self, state: &mut RuntimeState, pool: &mut EstimateIterPool, frame: &mut Frame) {
        match self.solver {
            Some(ref solver) => solver.run(state, pool, frame),
            _ => unreachable!()
        }
    }

    pub fn gen_pipes(&mut self, interner: &mut Interner) -> Vec<Solver> {
        let scans = self.get_block_scans();
        let solvers = scans.iter().enumerate().map(|(ix, scan)| Solver::new(interner, self.block_id, ix + 1, Some(*scan), &self.constraints)).collect();
        solvers
    }

    pub fn to_shapes(&self) -> Vec<Vec<PipeShape>> {
        let scans = self.get_block_scans();
        let mut shapes = vec![];
        let tag = TAG_INTERNED_ID;
        let mut tag_mappings:HashMap<Field, Vec<Interned>> = HashMap::new();
        // find all the e -> tag mappings
        for scan in scans.iter() {
            match scan {
                &&Constraint::Scan {ref e, ref a, ref v, ..} |
                &&Constraint::LookupCommit { ref e, ref a, ref v, ..} => {
                    let actual_a = if let &Field::Value(val) = a { val } else { 0 };
                    let actual_v = if let &Field::Value(val) = v { val } else { 0 };
                    if actual_a == tag && actual_v != 0 {
                        let mut tags = tag_mappings.entry(e.clone()).or_insert_with(|| vec![]);
                        tags.push(actual_v);
                    }
                }
                _ => (),

            }
        }
        // go through each scan and create tag, a, v pairs where 0 is wildcard
        for scan in scans.iter() {
            let mut scan_shapes = vec![];
            match scan {
                &&Constraint::Scan {ref e, ref a, ref v, ..} |
                &&Constraint::LookupCommit { ref e, ref a, ref v, ..} => {
                    let actual_e = if let &Field::Value(val) = e { val } else { 0 };
                    let actual_a = if let &Field::Value(val) = a { val } else { 0 };
                    let actual_v = if let &Field::Value(val) = v { val } else { 0 };
                    if actual_e != 0 {
                        scan_shapes.push(PipeShape::Scan(actual_e, 0, 0));
                    } else if actual_a == tag {
                        scan_shapes.push(PipeShape::Scan(0, actual_a, actual_v));
                    } else {
                        match tag_mappings.get(e) {
                            Some(mappings) => {
                                for mapping in mappings {
                                    scan_shapes.push(PipeShape::Scan(*mapping, actual_a, actual_v))
                                }
                            },
                            None => {
                                scan_shapes.push(PipeShape::Scan(actual_e, actual_a, actual_v))
                            }
                        }
                    }
                },
                &&Constraint::LookupRemote { ref _for, .. } => {
                    if let &Field::Value(id) = _for {
                        scan_shapes.push(PipeShape::Remote(id));
                    } else {
                        scan_shapes.push(PipeShape::Remote(0));
                    }
                }
                &&Constraint::AntiScan { ref key, .. } => {
                    if let Field::Value(id) = key[0] {
                        scan_shapes.push(PipeShape::Intermediate(id));
                    } else {
                        panic!("Non value intremediate id: {:?}", scan);
                    }
                }
                &&Constraint::IntermediateScan { ref key, .. } => {
                    if let Field::Value(id) = key[0] {
                        scan_shapes.push(PipeShape::Intermediate(id));
                    } else {
                        panic!("Non value intremediate id: {:?}", scan);
                    }
                }
                _ => { panic!("Non-scan in pipe shapes: {:?}", scan) }
            }
            shapes.push(scan_shapes);
        }
        shapes
    }

}

//-------------------------------------------------------------------------
// row
//-------------------------------------------------------------------------

#[derive(Debug)]
pub struct Row {
    pub fields: Vec<Interned>,
    pub solved_fields: u64,
    pub solving_for:u64,
    solved_stack: Vec<u64>,
}

impl Row {
    pub fn new(size:usize) -> Row {
        Row { fields: vec![0; size], solved_fields: 0, solving_for: 0, solved_stack:vec![0; size] }
    }

    pub fn put_solved(&mut self, ix:usize) {
        self.solved_stack[ix + 1] = self.solved_fields;
    }

    pub fn clear_solved(&mut self, ix:usize) {
        self.solved_stack[ix + 1] = 0;
    }

    pub fn get_solved(&self, ix:usize) -> u64 {
        self.solved_stack[ix]
    }

    pub fn check(&self, field_index:usize, value:Interned) -> bool {
        let cur = self.fields[field_index];
        cur == 0 || cur == value
    }

    pub fn set(&mut self, field_index:usize, value:Interned) {
        self.fields[field_index] = value;
        self.solving_for = set_bit(0, field_index);
        self.solved_fields = set_bit(self.solved_fields, field_index);
    }

    pub fn set_multi(&mut self, field_index:usize, value:Interned) {
        self.fields[field_index] = value;
        self.solving_for = set_bit(self.solving_for, field_index);
        self.solved_fields = set_bit(self.solved_fields, field_index);
    }

    pub fn clear_solving_for(&mut self) {
        self.solving_for = 0;
    }

    pub fn clear(&mut self, field_index:usize) {
        self.fields[field_index] = 0;
        self.solving_for = 0;
        self.solved_fields = clear_bit(self.solved_fields, field_index);
    }

    pub fn reset(&mut self) {
        let size = 64;
        self.solved_fields = 0;
        self.solving_for = 0;
        for field_index in 0..size {
            self.fields[field_index] = 0;
        }
    }
}

//-------------------------------------------------------------------------
// Estimate Iter
//-------------------------------------------------------------------------

pub struct EstimateIterPool {
    iters: Vec<EstimateIter>,
}

impl EstimateIterPool {
    pub fn new() -> EstimateIterPool {
        let mut iters = vec![];
        for _ in 0..64 {
            iters.push(EstimateIter::new());
        }
        EstimateIterPool { iters }
    }

    pub fn get(&mut self, iter_ix:usize) -> &mut EstimateIter {
        &mut self.iters[iter_ix]
    }
}

#[derive(Debug)]
pub struct EstimateIter {
    pub pass_through: bool,
    pub estimate: usize,
    pub iter: OutputingIter,
    pub constraint: usize,
}

impl EstimateIter {
    pub fn new() -> EstimateIter {
       EstimateIter { pass_through:false, estimate:usize::MAX, iter:OutputingIter::Empty, constraint:0 }
    }

    pub fn is_better(&self, estimate:usize) -> bool {
        self.estimate > estimate
    }

    pub fn reset(&mut self) {
        self.pass_through = false;
        self.estimate = usize::MAX;
        self.iter = OutputingIter::Empty;
        self.constraint = 0;
    }

    pub fn next(&mut self, row:&mut Row, iterator: usize) -> bool {
        if self.pass_through {
            false
        } else {
            self.iter.next(row, iterator)
        }
    }

    pub fn clear(&self, row:&mut Row, iterator: usize) {
        self.iter.clear(row, iterator);
    }
}

pub enum OutputingIter {
    Empty,
    Single(usize, Box<Iterator<Item=Interned>>),
    Multi(Vec<usize>, Box<Iterator<Item=Vec<Interned>>>),
}

impl OutputingIter {

    pub fn make_ptr<'a>(value: Box<Iterator<Item=Interned> + 'a>) -> Box<Iterator<Item=Interned> + 'static> {
        unsafe { mem::transmute(value) }
    }

    pub fn make_multi_ptr<'a>(value: Box<Iterator<Item=Vec<Interned>> + 'a>) -> Box<Iterator<Item=Vec<Interned>> + 'static> {
        unsafe { mem::transmute(value) }
    }

    pub fn next(&mut self, row:&mut Row, iterator: usize) -> bool {
        match self {
            &mut OutputingIter::Empty => { false }
            &mut OutputingIter::Single(output, ref mut iter) => {
                if let Some(v) = (*iter).next() {
                    row.set(output, v);
                    true
                } else {
                    false
                }
            },
            &mut OutputingIter::Multi(ref outputs, ref mut iter) => {
                for result in iter {
                    let prev_solved = row.get_solved(iterator);
                    let mut valid = true;
                    row.clear_solving_for();
                    for (out, v) in outputs.iter().zip(result.iter()) {
                        if check_bit(prev_solved, *out) {
                            if !row.check(*out, *v) {
                                valid = false;
                                break;
                            }
                        } else {
                            row.set_multi(*out, *v);
                        }
                    }
                    if valid {
                        return true;
                    }
                }
                false
            },
        }
    }

    pub fn clear(&self, row:&mut Row, iterator: usize) {
        match self {
            &OutputingIter::Empty => {}
            &OutputingIter::Single(output, _) => {
                row.clear(output);
            },
            &OutputingIter::Multi(ref outputs, _) => {
                let prev_solved = row.get_solved(iterator);
                for output in outputs.iter() {
                    if !check_bit(prev_solved, *output) {
                        row.clear(*output);
                    }
                }
            }
        }
    }
}

impl fmt::Debug for OutputingIter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &OutputingIter::Empty => { write!(f, "OutputingIter::Empty") }
            &OutputingIter::Single(reg, ..) => { write!(f, "OutputingIter::Single({:?})", reg) }
            &OutputingIter::Multi(ref regs, ..) => { write!(f, "OutputingIter::Multi({:?})", regs) }
        }
    }
}


//-------------------------------------------------------------------------
// Frame
//-------------------------------------------------------------------------

pub struct Counters {
    pub total_ns: u64,
    pub instructions: u64,
    pub iter_next: u64,
    pub accept: u64,
    pub accept_bail: u64,
    pub accept_ns: u64,
    pub inserts: u64,
    pub considered: u64,
}

#[allow(unused_must_use)]
impl fmt::Debug for Counters {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Counters: [\n");
        write!(f, "  time:\n");
        write!(f, "     total:  {}\n", self.total_ns);
        write!(f, "     accept: {} ({})\n", self.accept_ns, (self.accept_ns as f64) / (self.total_ns as f64));
        write!(f, "\n");
        write!(f, "  counts:\n");
        write!(f, "     instructions:        {}\n", self.instructions);
        write!(f, "     iter_next:           {}\n", self.iter_next);
        write!(f, "     values_considered:   {}\n", self.considered);
        write!(f, "     accept:              {}\n", self.accept);
        write!(f, "     accept_bail:         {}\n", self.accept_bail);
        write!(f, "]")
    }
}

pub struct Frame {
    pub input: Option<Change>,
    pub intermediate: Option<IntermediateChange>,
    pub remote: Option<RemoteChange>,
    pub row: Row,
    pub block_ix: usize,
    pub results: Vec<Interned>,
    #[allow(dead_code)]
    pub counters: Counters,
}

impl Frame {
    pub fn new() -> Frame {
        Frame {row: Row::new(64), block_ix:0, input: None, intermediate: None, remote: None, results: vec![], counters: Counters {iter_next: 0, accept: 0, accept_bail: 0, inserts: 0, instructions: 0, accept_ns: 0, total_ns: 0, considered: 0}}
    }

    pub fn get_register(&self, register:usize) -> Interned {
        self.row.fields[register]
    }

    pub fn resolve(&self, field:&Field) -> Interned {
        match field {
            &Field::Register(cur) => self.row.fields[cur],
            &Field::Value(cur) => cur,
        }
    }

    pub fn reset(&mut self) {
        self.input = None;
        self.intermediate = None;
        self.results.clear();
        self.row.reset();
    }
}

//-------------------------------------------------------------------------
// Field
//-------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash, Copy, Clone)]
pub enum Field {
    Register(usize),
    Value(Interned),
}

pub fn register(ix: usize) -> Field {
    Field::Register(ix)
}

pub fn is_register(field:&Field) -> bool {
    if let &Field::Register(_) = field {
        true
    } else {
        false
    }
}

pub fn format_field(interner:&Interner, field:&Field) -> String{
    match field {
        &Field::Register(reg) => format!("Register({})", reg),
        &Field::Value(interned) => format!("Value({})", format_interned(interner, interned))
    }
}

//-------------------------------------------------------------------------
// Interner
//-------------------------------------------------------------------------

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Internable {
    Null,
    String(String),
    Number(u32),
}

impl PartialOrd for Internable {
    fn partial_cmp(&self, rhs:&Self) -> Option<cmp::Ordering> {
        let priority = self.to_sort_priority();
        let right_priority = self.to_sort_priority();
        if priority != right_priority {
            return Some(priority.cmp(&right_priority));
        }

        match (self, rhs) {
            (&Internable::Null, &Internable::Null) => { Some(cmp::Ordering::Equal) },
            (&Internable::String(ref s), &Internable::String(ref s2)) => { Some(natord::compare(s, s2)) },
            (&Internable::Number(n), &Internable::Number(n2)) => {
                let value = unsafe {transmute::<u32, f32>(n) };
                let value2 = unsafe {transmute::<u32, f32>(n2) };
                value.partial_cmp(&value2)
            },
            _ => { unreachable!() }
        }
    }
}

impl Ord for Internable {
    fn cmp(&self, rhs:&Self) -> cmp::Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}

impl Internable {
    pub fn to_number(intern: &Internable) -> f32 {
        match intern {
            &Internable::Number(num) => unsafe { transmute::<u32, f32>(num) },
            _ => { panic!("to_number on non-number") }
        }
    }

    pub fn to_string(intern: &Internable) -> String {
        match intern {
            &Internable::String(ref string) => string.to_string(),
            &Internable::Number(_) => Internable::to_number(intern).to_string(),
            _ => { panic!("to_string on non-string/number") }
        }
    }

    pub fn from_number(num: f32) -> Internable {
        let value = unsafe { transmute::<f32, u32>(num) };
        Internable::Number(value)
    }

    pub fn from_str(s: &str) -> Internable {
        Internable::String(s.to_string())
    }

    pub fn print(&self) -> String {
        match self {
            &Internable::String(ref s) => {
                s.to_string()
            }
            &Internable::Number(_) => {
                Internable::to_number(self).to_string()
            }
            &Internable::Null => {
                "Null!".to_string()
            }
        }
    }

    pub fn to_json(&self) -> JSONInternable {
        match self {
            &Internable::String(ref s) => { JSONInternable::String(s.to_owned()) }
            &Internable::Number(n) => { JSONInternable::Number(n) }
            &Internable::Null => { JSONInternable::Null }
        }
    }

    pub fn to_sort_priority(&self) -> usize {
        match self {
            &Internable::Null => { 0 }
            &Internable::Number(_) => { 1 }
            &Internable::String(_) => { 2 }
        }
    }
}

impl From<JSONInternable> for Internable {
    fn from(json: JSONInternable) -> Self {
        match json {
            JSONInternable::String(s) => { Internable::String(s) }
            JSONInternable::Number(n) => { Internable::Number(n) }
            JSONInternable::Null => { Internable::Null }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum JSONInternable {
    String(String),
    Number(u32),
    Null,
}

impl JSONInternable {
    pub fn to_number(intern: &JSONInternable) -> f32 {
        match intern {
            &JSONInternable::Number(num) => unsafe { transmute::<u32, f32>(num) },
            _ => { panic!("to_number on non-number") }
        }
    }

    pub fn from_number(num: f32) -> JSONInternable {
        let value = unsafe { transmute::<f32, u32>(num) };
        JSONInternable::Number(value)
    }

    pub fn print(&self) -> String {
        match self {
            &JSONInternable::String(ref s) => {
                s.to_string()
            }
            &JSONInternable::Number(_) => {
                JSONInternable::to_number(self).to_string()
            }
            &JSONInternable::Null => {
                "Null!".to_string()
            }
        }
    }
}

impl From<Internable> for JSONInternable {
    fn from(internable: Internable) -> Self {
        match internable {
            Internable::String(s) => { JSONInternable::String(s) }
            Internable::Number(n) => { JSONInternable::Number(n) }
            Internable::Null => { JSONInternable::Null }
        }
    }
}

impl<'a> From<&'a Internable> for JSONInternable {
    fn from(internable: &'a Internable) -> Self {
        match internable {
            &Internable::String(ref s) => { JSONInternable::String(s.to_owned()) }
            &Internable::Number(n) => { JSONInternable::Number(n) }
            &Internable::Null => { JSONInternable::Null }
        }
    }
}

impl Serialize for JSONInternable {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        match self {
            &JSONInternable::String(ref s) => serializer.serialize_str(s),
            &JSONInternable::Number(_) => serializer.serialize_f32(JSONInternable::to_number(self)),
            _ => serializer.serialize_unit(),
        }
    }
}

impl<'de> Deserialize<'de> for JSONInternable {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        struct InternableVisitor;

        impl<'de> Visitor<'de> for InternableVisitor {
            type Value = JSONInternable;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("Internable")
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
                where E: Error
            {
                Ok(JSONInternable::from_number(v as f32))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
                where E: Error
            {
                Ok(JSONInternable::from_number(v as f32))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
                where E: Error
            {
                Ok(JSONInternable::from_number(v as f32))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where E: Error
            {
                Ok(JSONInternable::String(v.to_owned()))
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
                where E: Error
            {
                Ok(JSONInternable::Null)
            }
        }

        deserializer.deserialize_any(InternableVisitor)
    }
}

pub struct Interner {
    id_to_value: HashMap<Internable, Interned, MyHasher>,
    value_to_id: Vec<Internable>,
    next_id: Interned,
}

impl Interner {
    pub fn new() -> Interner {
        let mut me = Interner {id_to_value: HashMap::default(), value_to_id:vec![Internable::Null], next_id:1};
        me.string("tag");
        me
    }

    pub fn internable_to_id(&mut self, thing:Internable) -> Interned {
        match self.id_to_value.get(&thing) {
            Some(&id) => id,
            None => {
                let next = self.next_id;
                self.value_to_id.push(thing.clone());
                self.id_to_value.insert(thing, next);
                self.next_id += 1;
                next
            }
        }
    }

    pub fn string(&mut self, string:&str) -> Field {
        let thing = Internable::String(string.to_string());
        Field::Value(self.internable_to_id(thing))
    }

    pub fn string_id(&mut self, string:&str) -> Interned {
        let thing = Internable::String(string.to_string());
        self.internable_to_id(thing)
    }

    #[allow(dead_code)]
    pub fn number(&mut self, num:f32) -> Field {
        let bitpattern = unsafe {
            transmute::<f32, u32>(num)
        };
        let thing = Internable::Number(bitpattern);
        Field::Value(self.internable_to_id(thing))
    }

    pub fn number_id(&mut self, num:f32) -> Interned {
        let bitpattern = unsafe {
            transmute::<f32, u32>(num)
        };
        let thing = Internable::Number(bitpattern);
        self.internable_to_id(thing)
    }

    #[allow(dead_code)]
    pub fn get_value(&self, id:u32) -> &Internable {
        &self.value_to_id[id as usize]
    }

    #[allow(dead_code)]
    pub fn get_string(&self, id:u32) -> Option<String> {
        match self.get_value(id) {
            &Internable::String(ref str) => Some(str.to_owned()),
            _ => None
        }
    }
}

//-------------------------------------------------------------------------
// Constraint
//-------------------------------------------------------------------------

type FilterFunction = fn(&Internable, &Internable) -> bool;
type Function = fn(Vec<&Internable>) -> Option<Internable>;
type MultiFunction = fn(Vec<&Internable>) -> Option<Vec<Vec<Internable>>>;
pub type AggregateFunction = fn(&mut AggregateEntry, &Vec<Internable>);

pub enum Constraint {
    Scan {e: Field, a: Field, v: Field, register_mask: u64},
    LookupCommit {e: Field, a: Field, v: Field, register_mask: u64},
    LookupRemote {e: Field, a: Field, v: Field, _for: Field, _type: Field, from: Field, to: Field, register_mask: u64},
    AntiScan {key: Vec<Field>, register_mask: u64},
    IntermediateScan {full_key:Vec<Field>, key: Vec<Field>, value: Vec<Field>, register_mask: u64, output_mask: u64},
    Function {op: String, output: Field, func: Function, params: Vec<Field>, param_mask: u64, output_mask: u64},
    MultiFunction {op: String, outputs: Vec<Field>, func: MultiFunction, params: Vec<Field>, param_mask: u64, output_mask: u64},
    Aggregate {op: String, output: Vec<Field>, add: AggregateFunction, remove:AggregateFunction, group:Vec<Field>, projection:Vec<Field>, params: Vec<Field>, param_mask: u64, output_mask: u64, output_key:Vec<Field>, kind: FunctionKind},
    Filter {op: String, func: FilterFunction, left: Field, right: Field, param_mask: u64},
    Insert {e: Field, a: Field, v:Field, commit:bool},
    InsertIntermediate {key:Vec<Field>, value:Vec<Field>, negate:bool},
    Remove {e: Field, a: Field, v:Field},
    RemoveAttribute {e: Field, a: Field},
    RemoveEntity {e: Field },
    DynamicCommit {e: Field, a: Field, v:Field, _type: Field},
    Project {registers: Vec<usize>},
    Watch {name: String, registers: Vec<Field>},
}

fn filter_registers(fields:&Vec<&Field>) -> Vec<Field> {
    fields.iter().filter(|v| is_register(**v)).map(|v| (**v).clone()).collect()
}

fn replace_registers(fields:&mut Vec<&mut Field>, lookup:&HashMap<Field,Field>) {
    for field in fields {
        if is_register(*field) {
            **field = *lookup.get(field).unwrap();
        }
    }
}

impl Constraint {
    pub fn get_registers(&self) -> Vec<Field> {
        match self {
            &Constraint::Scan { ref e, ref a, ref v, ..} => { filter_registers(&vec![e,a,v]) }
            &Constraint::LookupCommit { ref e, ref a, ref v, ..} => { filter_registers(&vec![e,a,v]) }
            &Constraint::LookupRemote { ref e, ref a, ref v, ref _for, ref _type, ref from, ref to, ..} => { filter_registers(&vec![e,a,v, _for, _type, from, to]) }
            &Constraint::AntiScan { ref key, ..} => { filter_registers(&key.iter().collect()) }
            &Constraint::IntermediateScan { ref full_key, ..} => {
                filter_registers(&full_key.iter().collect())
            }
            &Constraint::Function {ref output, ref params, ..} => {
                let mut vs = vec![output];
                vs.extend(params);
                filter_registers(&vs)
            }
            &Constraint::MultiFunction {ref outputs, ref params, ..} => {
                let mut vs = vec![];
                vs.extend(outputs);
                vs.extend(params);
                filter_registers(&vs)
            }
            &Constraint::Aggregate {ref group, ref projection, ref params, ..} => {
                let mut vs = vec![];
                vs.extend(params);
                vs.extend(group);
                vs.extend(projection);
                filter_registers(&vs)
            }
            &Constraint::Filter {ref left, ref right, ..} => {
                filter_registers(&vec![left, right])
            }
            &Constraint::Insert { ref e, ref a, ref v, .. } => { filter_registers(&vec![e,a,v]) },
            &Constraint::InsertIntermediate { ref key, ref value, .. } => {
                let mut cur:Vec<&Field> = key.iter().collect();
                cur.extend(value);
                filter_registers(&cur)
            }
            &Constraint::Remove { ref e, ref a, ref v } => { filter_registers(&vec![e,a,v]) },
            &Constraint::RemoveAttribute { ref e, ref a } => { filter_registers(&vec![e,a]) },
            &Constraint::RemoveEntity { ref e } => { filter_registers(&vec![e]) },
            &Constraint::DynamicCommit { ref e, ref a, ref v, ref _type } => { filter_registers(&vec![e,a,v,_type]) },
            &Constraint::Project {ref registers} => { registers.iter().map(|v| Field::Register(*v)).collect() },
            &Constraint::Watch {ref registers, ..} => { filter_registers(&registers.iter().collect()) },
        }
    }

    pub fn get_output_registers(&self) -> Vec<Field> {
        match self {
            &Constraint::Scan { ref e, ref a, ref v, ..} => { filter_registers(&vec![e,a,v]) }
            &Constraint::LookupCommit { ref e, ref a, ref v, ..} => { filter_registers(&vec![e,a,v]) }
            &Constraint::LookupRemote { ref e, ref a, ref v, ref _for, ref _type, ref from, ref to, ..} => { filter_registers(&vec![e,a,v, _for, _type, from, to]) }
            &Constraint::Function {ref output, ..} => { filter_registers(&vec![output]) }
            &Constraint::MultiFunction {ref outputs, ..} => { filter_registers(&outputs.iter().collect()) }
            &Constraint::Aggregate {ref output, ..} => { filter_registers(&output.iter().collect()) }
            &Constraint::IntermediateScan {ref value, ..} => { filter_registers(&value.iter().collect()) }
            _ => { vec![] }
        }
    }

    pub fn get_filtering_registers(&self) -> Vec<Field> {
        match self {
            &Constraint::Scan { ref e, ref a, ref v, ..} => { filter_registers(&vec![e,a,v]) }
            &Constraint::LookupCommit { ref e, ref a, ref v, ..} => { filter_registers(&vec![e,a,v]) }
            &Constraint::LookupRemote { ref e, ref a, ref v, ref _for, ref _type, ref from, ref to, ..} => { filter_registers(&vec![e,a,v, _for, _type, from, to]) }
            &Constraint::Function {ref output, ..} => { filter_registers(&vec![output]) }
            &Constraint::MultiFunction {ref outputs, ..} => { filter_registers(&outputs.iter().collect()) }
            &Constraint::Filter {ref left, ref right, ..} => { filter_registers(&vec![left, right]) }
            &Constraint::AntiScan {ref key, ..} => { filter_registers(&key.iter().collect()) }
            &Constraint::IntermediateScan {ref full_key, ..} => { filter_registers(&full_key.iter().collect()) }
            _ => { vec![] }
        }
    }

    pub fn replace_registers(&mut self, lookup:&HashMap<Field, Field>) {
        match self {
            &mut Constraint::Scan { ref mut e, ref mut a, ref mut v, ref mut register_mask} => {
                replace_registers(&mut vec![e,a,v], lookup);
                *register_mask = make_register_mask(vec![e,a,v]);
            }
            &mut Constraint::LookupCommit { ref mut e, ref mut a, ref mut v, ref mut register_mask} => {
                replace_registers(&mut vec![e,a,v], lookup);
                *register_mask = make_register_mask(vec![e,a,v]);
            }
            &mut Constraint::LookupRemote { ref mut e, ref mut a, ref mut v, ref mut _type, ref mut _for, ref mut to, ref mut from, ref mut register_mask} => {
                replace_registers(&mut vec![e,a,v,_for,_type,from,to], lookup);
                *register_mask = make_register_mask(vec![e,a,v,_type,_for,from,to]);
            }
            &mut Constraint::AntiScan { ref mut key, ref mut register_mask} => {
                replace_registers(&mut key.iter_mut().collect(), lookup);
                *register_mask = make_register_mask(key.iter().collect());
            }
            &mut Constraint::IntermediateScan { ref mut full_key, ref mut key, ref mut value, ref mut register_mask, ref mut output_mask} => {
                replace_registers(&mut full_key.iter_mut().collect(), lookup);
                replace_registers(&mut key.iter_mut().collect(), lookup);
                *register_mask = make_register_mask(key.iter().collect());
                replace_registers(&mut value.iter_mut().collect(), lookup);
                *output_mask = make_register_mask(value.iter().collect());
            }
            &mut Constraint::Function {ref mut output, ref mut params, ref mut param_mask, ref mut output_mask, ..} => {
                {
                    let mut vs = vec![];
                    vs.extend(params.iter_mut());
                    replace_registers(&mut vs, lookup);
                }
                *param_mask = make_register_mask(params.iter().collect());
                *output = *lookup.get(output).unwrap();
                *output_mask = make_register_mask(vec![output]);
            }
            &mut Constraint::MultiFunction {ref mut outputs, ref mut params, ref mut param_mask, ref mut output_mask, ..} => {
                {
                    let mut vs = vec![];
                    vs.extend(outputs.iter_mut());
                    vs.extend(params.iter_mut());
                    replace_registers(&mut vs, lookup);
                }
                *param_mask = make_register_mask(params.iter().collect());
                *output_mask = make_register_mask(outputs.iter().collect());
            }
            &mut Constraint::Aggregate {ref mut params, ref mut group, ref mut projection, ref mut param_mask, ref mut output_key, ..} => {
                {
                    let mut vs = vec![];
                    vs.extend(output_key.iter_mut());
                    vs.extend(params.iter_mut());
                    vs.extend(group.iter_mut());
                    vs.extend(projection.iter_mut());
                    replace_registers(&mut vs, lookup);
                }
                let mut vs2 = vec![];
                vs2.extend(params.iter());
                vs2.extend(group.iter());
                vs2.extend(projection.iter());
                *param_mask = make_register_mask(vs2);
            }
            &mut Constraint::Filter {ref mut left, ref mut right, ref mut param_mask, ..} => {
                replace_registers(&mut vec![left, right], lookup);
                *param_mask = make_register_mask(vec![left, right]);
            }
            &mut Constraint::Insert { ref mut e, ref mut a, ref mut v, ..} => { replace_registers(&mut vec![e,a,v], lookup); },
            &mut Constraint::InsertIntermediate { ref mut key, ref mut value, .. } => {
                replace_registers(&mut key.iter_mut().collect(), lookup);
                replace_registers(&mut value.iter_mut().collect(), lookup);
            }
            &mut Constraint::Remove { ref mut e, ref mut a, ref mut v } => { replace_registers(&mut vec![e,a,v], lookup); },
            &mut Constraint::RemoveAttribute { ref mut e, ref mut a } => { replace_registers(&mut vec![e,a], lookup); },
            &mut Constraint::RemoveEntity { ref mut e } => { replace_registers(&mut vec![e], lookup); },
            &mut Constraint::DynamicCommit { ref mut e, ref mut a, ref mut v, ref mut _type } => { replace_registers(&mut vec![e,a,v,_type], lookup); },
            &mut Constraint::Project {ref mut registers} => {
                for reg in registers.iter_mut() {
                    if let &Field::Register(neue) = lookup.get(&Field::Register(*reg)).unwrap() {
                        *reg = neue;
                    }
                }
            },
            &mut Constraint::Watch {ref mut registers, ..} => { replace_registers(&mut registers.iter_mut().collect(), lookup); },
        }
    }
}

impl Clone for Constraint {
    fn clone(&self) -> Self {
        match self {
            &Constraint::Scan { e, a, v, register_mask } => { Constraint::Scan {e,a,v,register_mask} }
            &Constraint::LookupCommit { e, a, v, register_mask } => { Constraint::LookupCommit {e,a,v,register_mask} }
            &Constraint::LookupRemote { e, a, v, _for, _type, from, to, register_mask } => { Constraint::LookupRemote { e,a,v,_for,_type,from,to,register_mask } }
            &Constraint::AntiScan { ref key, register_mask } => { Constraint::AntiScan {key:key.clone(),register_mask} }
            &Constraint::IntermediateScan { ref full_key, ref key, ref value, register_mask, output_mask } => {
                Constraint::IntermediateScan {full_key:full_key.clone(), key:key.clone(), value:value.clone(), register_mask, output_mask}
            }
            &Constraint::Function {ref op, ref output, ref func, ref params, ref param_mask, ref output_mask} => {
                Constraint::Function{ op:op.clone(), output:output.clone(), func:*func, params:params.clone(), param_mask:*param_mask, output_mask:*output_mask }
            }
            &Constraint::MultiFunction {ref op, ref outputs, ref func, ref params, ref param_mask, ref output_mask} => {
                Constraint::MultiFunction{ op:op.clone(), outputs:outputs.clone(), func:*func, params:params.clone(), param_mask:*param_mask, output_mask:*output_mask }
            }
            &Constraint::Aggregate {ref op, ref output, ref add, ref remove, ref group, ref projection, ref params, ref param_mask, ref output_mask, ref output_key, kind} => {
                Constraint::Aggregate { op:op.clone(), output:output.clone(), add:*add, remove:*remove, group:group.clone(), projection:projection.clone(), params:params.clone(), param_mask:*param_mask, output_mask:*output_mask, output_key:output_key.clone(), kind }
            }
            &Constraint::Filter {ref op, ref func, ref left, ref right, ref param_mask} => {
                Constraint::Filter{ op:op.clone(), func:*func, left:left.clone(), right:right.clone(), param_mask:*param_mask }
            }
            &Constraint::Insert { e,a,v,commit } => { Constraint::Insert { e,a,v,commit } },
            &Constraint::InsertIntermediate { ref key, ref value, negate } => { Constraint::InsertIntermediate {key:key.clone(), value:value.clone(), negate} }
            &Constraint::Remove { e,a,v } => { Constraint::Remove { e,a,v } },
            &Constraint::RemoveAttribute { e,a } => { Constraint::RemoveAttribute { e,a } },
            &Constraint::RemoveEntity { e } => { Constraint::RemoveEntity { e } },
            &Constraint::DynamicCommit { e,a,v,_type } => { Constraint::DynamicCommit { e,a,v,_type } },
            &Constraint::Project {ref registers} => { Constraint::Project { registers:registers.clone() } },
            &Constraint::Watch {ref name, ref registers} => { Constraint::Watch { name:name.clone(), registers:registers.clone() } },

        }
    }
}

// @FIXME it's ridiculous that I have to do this just because there's a function pointer in the
// enum
impl PartialEq for Constraint {
    fn eq(&self, other:&Constraint) -> bool {
        match (self, other) {
            (&Constraint::Scan { e, a, v, ..}, &Constraint::Scan {e:e2, a:a2, v:v2, ..} ) => { e == e2 && a == a2 && v == v2 },
            (&Constraint::LookupCommit { e, a, v, ..}, &Constraint::LookupCommit {e:e2, a:a2, v:v2, ..} ) => { e == e2 && a == a2 && v == v2 },
            (&Constraint::LookupRemote { e, a, v, _for, _type, from, to, ..}, &Constraint::LookupRemote {e:e2, a:a2, v:v2, _for:for2, _type:type2, from: from2, to:to2, ..} ) => { e == e2 && a == a2 && v == v2 && _for == for2 && _type == type2 && from == from2 && to == to2 },
            (&Constraint::AntiScan { ref key, ..}, &Constraint::AntiScan { key:ref key2, ..})  => { key == key2 }
            (&Constraint::IntermediateScan { ref full_key, ..}, &Constraint::IntermediateScan { full_key:ref full_key2, ..}) => { full_key == full_key2 }
            (&Constraint::Function {ref op, ref output, ref params, ..}, &Constraint::Function {op:ref op2, output:ref output2, params:ref params2, ..}) => { op == op2 && output == output2 && params == params2 }
            (&Constraint::MultiFunction {ref op, ref outputs, ref params, ..}, &Constraint::MultiFunction {op:ref op2, outputs:ref outputs2, params:ref params2, ..}) => { op == op2 && outputs == outputs2 && params == params2 }
            (&Constraint::Aggregate {ref op, ref output, ref group, ref projection, ref params, ..}, &Constraint::Aggregate {op:ref op2, output:ref output2, group:ref group2, projection:ref projection2, params:ref params2, ..}) => { op == op2 && output == output2 && params == params2 && group == group2 && projection == projection2 }
            (&Constraint::Filter {ref op, ref left, ref right, ..}, &Constraint::Filter {op:ref op2, left:ref left2, right:ref right2, ..}) => { op == op2 && left == left2 && right == right2 }
            (&Constraint::Insert { e,a,v,commit }, &Constraint::Insert { e:e2, a:a2, v:v2, commit:commit2 }) => {  e == e2 && a == a2 && v == v2 && commit == commit2 },
            (&Constraint::InsertIntermediate { ref key, ref value, negate }, &Constraint::InsertIntermediate { key:ref key2, value:ref value2, negate:negate2 }) => { key == key2 && value == value2 && negate == negate2 }
            (&Constraint::Remove { e,a,v }, &Constraint::Remove { e:e2, a:a2, v:v2 }) => {  e == e2 && a == a2 && v == v2 },
            (&Constraint::RemoveAttribute { e,a }, &Constraint::RemoveAttribute { e:e2, a:a2 }) => {  e == e2 && a == a2 },
            (&Constraint::RemoveEntity { e }, &Constraint::RemoveEntity { e:e2 }) => {  e == e2 },
            (&Constraint::DynamicCommit { e,a,v,_type }, &Constraint::DynamicCommit { e:e2, a:a2, v:v2, _type:type2 }) => {  e == e2 && a == a2 && v == v2 && _type == type2 },
            (&Constraint::Project { ref registers }, &Constraint::Project { registers:ref registers2 }) => {  registers == registers2 },
            (&Constraint::Watch { ref name, ref registers }, &Constraint::Watch { name:ref name2, registers:ref registers2 }) => { name == name2 && registers == registers2 },
            _ => false

        }
    }
}
impl Eq for Constraint {}

impl Hash for Constraint {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            &Constraint::Scan { e, a, v, ..} => { e.hash(state); a.hash(state); v.hash(state); },
            &Constraint::LookupCommit { e, a, v, ..} => { e.hash(state); a.hash(state); v.hash(state); },
            &Constraint::LookupRemote { e, a, v, _for, _type, from, to, ..} => { e.hash(state); a.hash(state); v.hash(state); _for.hash(state); _type.hash(state); from.hash(state); to.hash(state); },
            &Constraint::AntiScan { ref key, ..}  => { key.hash(state); }
            &Constraint::IntermediateScan { ref full_key, ..} => { full_key.hash(state) }
            &Constraint::Function {ref op, ref output, ref params, ..} => { op.hash(state); output.hash(state); params.hash(state); }
            &Constraint::MultiFunction {ref op, ref outputs, ref params, ..} => { op.hash(state); outputs.hash(state); params.hash(state); }
            &Constraint::Aggregate {ref op, ref output, ref group, ref projection, ref params, ..} => { op.hash(state); output.hash(state); group.hash(state); projection.hash(state); params.hash(state); }
            &Constraint::Filter {ref op, ref left, ref right, ..} => { op.hash(state); left.hash(state); right.hash(state); }
            &Constraint::Insert { e,a,v,commit } => { e.hash(state); a.hash(state); v.hash(state); commit.hash(state); },
            &Constraint::InsertIntermediate { ref key, ref value, negate } => { key.hash(state); value.hash(state); negate.hash(state); }
            &Constraint::Remove { e,a,v } => { e.hash(state); a.hash(state); v.hash(state); },
            &Constraint::RemoveAttribute { e,a } => { e.hash(state); a.hash(state); },
            &Constraint::RemoveEntity { e } => { e.hash(state); },
            &Constraint::DynamicCommit { e,a,v,_type } => { e.hash(state); a.hash(state); v.hash(state); _type.hash(state); },
            &Constraint::Project { ref registers } => { registers.hash(state); },
            &Constraint::Watch { ref name, ref registers } => { name.hash(state); registers.hash(state); },
        }
    }
}



impl fmt::Debug for Constraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Constraint::Scan { e, a, v, .. } => { write!(f, "Scan ( {:?}, {:?}, {:?} )", e, a, v) }
            &Constraint::LookupCommit { e, a, v, .. } => { write!(f, "LookupCommit ( {:?}, {:?}, {:?} )", e, a, v) }
            &Constraint::LookupRemote { e, a, v, _for, _type, from, to, .. } => { write!(f, "LookupRemote ( {:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?} )", e, a, v, _for, _type, from, to) }
            &Constraint::AntiScan { ref key, .. } => { write!(f, "AntiScan ({:?})", key) }
            &Constraint::IntermediateScan { ref key, ref value, .. } => { write!(f, "IntermediateScan ( {:?}, {:?} )", key, value) }
            &Constraint::Insert { e, a, v, .. } => { write!(f, "Insert ( {:?}, {:?}, {:?} )", e, a, v) }
            &Constraint::InsertIntermediate { ref key, ref value, negate } => { write!(f, "InsertIntermediate ({:?}, {:?}, negate? {:?})", key, value, negate) }
            &Constraint::Remove { e, a, v, .. } => { write!(f, "Remove ( {:?}, {:?}, {:?} )", e, a, v) }
            &Constraint::RemoveAttribute { e, a, .. } => { write!(f, "RemoveAttribute ( {:?}, {:?} )", e, a) }
            &Constraint::RemoveEntity { e, .. } => { write!(f, "RemoveEntity ( {:?} )", e) }
            &Constraint::DynamicCommit { e, a, v, _type, .. } => { write!(f, "Remove ( {:?}, {:?}, {:?}, {:?} )", e, a, v, _type) }
            &Constraint::Function { ref op, ref params, ref output, .. } => { write!(f, "{:?} = {}({:?})", output, op, params) }
            &Constraint::MultiFunction { ref op, ref params, ref outputs, .. } => { write!(f, "{:?} = {}({:?})", outputs, op, params) }
            &Constraint::Aggregate { ref op, ref group, ref projection, ref params, ref output_key, .. } => { write!(f, "{:?} = {}(per: {:?}, for: {:?}, {:?})", output_key, op, group, projection, params) }
            &Constraint::Filter { ref op, ref left, ref right, .. } => { write!(f, "Filter ( {:?} {} {:?} )", left, op, right) }
            &Constraint::Project { ref registers } => { write!(f, "Project {:?}", registers) }
            &Constraint::Watch { ref name, ref registers } => { write!(f, "Watch {}{:?}", name, registers) }
        }
    }
}


pub fn make_register_mask(fields: Vec<&Field>) -> u64 {
    let mut mask = 0;
    for field in fields {
        match field {
            &Field::Register(r) => mask = set_bit(mask, (r % 64)),
            _ => {},
        }
    }
    mask
}

pub fn make_scan(e:Field, a:Field, v:Field) -> Constraint {
    let register_mask = make_register_mask(vec![&e,&a,&v]);
    Constraint::Scan{e, a, v, register_mask }
}

pub fn make_commit_lookup(e:Field, a:Field, v:Field) -> Constraint {
    let register_mask = make_register_mask(vec![&e,&a,&v]);
    Constraint::LookupCommit{e, a, v, register_mask }
}

pub fn make_remote_lookup(e:Field, a:Field, v:Field, _for:Field, _type:Field, from:Field, to:Field) -> Constraint {
    let register_mask = make_register_mask(vec![&e,&a,&v,&_for,&_type,&from,&to]);
    Constraint::LookupRemote{e, a, v, _for, _type, from, to, register_mask }
}

pub fn make_anti_scan(key: Vec<Field>) -> Constraint {
    let register_mask = make_register_mask(key.iter().collect::<Vec<&Field>>());
    Constraint::AntiScan{key, register_mask }
}

pub fn make_intermediate_scan(key: Vec<Field>, value: Vec<Field>) -> Constraint {
    let mut full_key = key.clone();
    full_key.extend(value.iter());
    let register_mask = make_register_mask(key.iter().collect::<Vec<&Field>>());
    let output_mask = make_register_mask(value.iter().collect::<Vec<&Field>>());
    Constraint::IntermediateScan{full_key, key, value, register_mask, output_mask }
}

pub fn make_intermediate_insert(key: Vec<Field>, value:Vec<Field>, negate:bool) -> Constraint {
    Constraint::InsertIntermediate {key, value, negate}
}

pub fn make_function(op: &str, params: Vec<Field>, output: Field) -> Constraint {
    let param_mask = make_register_mask(params.iter().collect::<Vec<&Field>>());
    let output_mask = make_register_mask(vec![&output]);
    let func = match op {
        "+" => add,
        "-" => subtract,
        "*" => multiply,
        "/" => divide,
        "math/sin" => math_sin,
        "math/cos" => math_cos,
        "math/absolute" => math_absolute,
        "math/mod" => math_mod,
        "math/pow" => math_pow,
        "math/to-fixed" => math_to_fixed,
        "math/to-hex" => math_to_hex,
        "math/ceiling" => math_ceiling,
        "math/floor" => math_floor,
        "math/round" => math_round,
        "random/number" => random_number,
        "string/replace" => string_replace,
        "string/contains" => string_contains,
        "string/lowercase" => string_lowercase,
        "string/uppercase" => string_uppercase,
        "string/substring" => string_substring,
        "string/length" => string_length,
        "string/encode" => string_encode,
        "string/url-encode" => string_urlencode,
        "concat" => concat,
        "gen_id" => gen_id,
        _ => panic!("Unknown function: {:?}", op)
    };
    Constraint::Function {op: op.to_string(), func, params, output, param_mask, output_mask }
}

pub fn make_multi_function(op: &str, params: Vec<Field>, outputs: Vec<Field>) -> Constraint {
    let param_mask = make_register_mask(params.iter().collect::<Vec<&Field>>());
    let output_mask = make_register_mask(outputs.iter().collect::<Vec<&Field>>());
    let func = match op {
        "eve-internal/string/split-reverse" => string_split_reverse,
        "string/split" => string_split,
        "string/index-of" => string_index_of,
        "math/range" => math_range,
        _ => panic!("Unknown multi function: {:?}", op)
    };
    Constraint::MultiFunction {op: op.to_string(), func, params, outputs, param_mask, output_mask }
}

pub fn make_aggregate(op: &str, group: Vec<Field>, projection:Vec<Field>, params: Vec<Field>, output: Vec<Field>, kind:FunctionKind) -> Constraint {
    let param_mask = make_register_mask(params.iter().collect::<Vec<&Field>>());
    let output_mask = make_register_mask(output.iter().collect::<Vec<&Field>>());
    let (add, remove):(AggregateFunction, AggregateFunction) = match op {
        "gather/sum" => (aggregate_sum_add, aggregate_sum_remove),
        "gather/count" => (aggregate_count_add, aggregate_count_remove),
        "gather/average" => (aggregate_avg_add, aggregate_avg_remove),
        "gather/top" => (aggregate_top_add, aggregate_top_remove),
        "gather/bottom" => (aggregate_bottom_add, aggregate_bottom_remove),
        "gather/next" => (aggregate_next_add, aggregate_next_remove),
        "gather/previous" => (aggregate_prev_add, aggregate_prev_remove),
        _ => panic!("Unknown function: {:?}", op)
    };
    Constraint::Aggregate {op: op.to_string(), add, remove, group, projection, params, output, param_mask, output_mask, output_key:vec![], kind, }
}

pub fn make_filter(op: &str, left: Field, right:Field) -> Constraint {
    let param_mask = make_register_mask(vec![&left,&right]);
    let func = match op {
        "=" => eq,
        "!=" => not_eq,
        ">" => gt,
        ">=" => gte,
        "<" => lt,
        "<=" => lte,
        _ => panic!("Unknown filter {:?}", op)
    };
    Constraint::Filter {op:op.to_string(), func, left, right, param_mask }
}

//-------------------------------------------------------------------------
// Filters
//-------------------------------------------------------------------------

pub fn eq(left:&Internable, right:&Internable) -> bool {
    left == right
}

pub fn not_eq(left:&Internable, right:&Internable) -> bool {
    left != right
}

macro_rules! numeric_filter {
    ($name:ident, $op:tt) => {
        pub fn $name(left:&Internable, right:&Internable) -> bool {
            match (left, right) {
                (&Internable::Number(_), &Internable::Number(_)) => {
                    let a = Internable::to_number(left);
                    let b = Internable::to_number(right);
                    a $op b
                },
                (&Internable::String(ref a), &Internable::String(ref b)) => {
                    a $op b
                },
                _ => { false }
            }
        }
    };
}

numeric_filter!(gt, >);
numeric_filter!(gte, >=);
numeric_filter!(lt, <);
numeric_filter!(lte, <=);

//-------------------------------------------------------------------------
// Functions
//-------------------------------------------------------------------------

macro_rules! binary_math {
    ($name:ident, $op:tt) => {
        pub fn $name(params: Vec<&Internable>) -> Option<Internable> {
            match params.as_slice() {
                &[&Internable::Number(_), &Internable::Number(_)] => {
                    let a = Internable::to_number(params[0]);
                    let b = Internable::to_number(params[1]);
                    Some(Internable::from_number(a $op b))
                },
                _ => { None }
            }
        }
    };
}

binary_math!(add, +);
binary_math!(subtract, -);
binary_math!(multiply, *);
binary_math!(divide, /);


pub fn math_sin(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::Number(_)] => {
            let a = Internable::to_number(params[0]);
            Some(Internable::from_number((a * PI / 180.0).sin()))
        },
        _ => { None }
    }
}

pub fn math_cos(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::Number(_)] => {
            let a = Internable::to_number(params[0]);
            Some(Internable::from_number((a * PI / 180.0).cos()))
        },
        _ => { None }
    }
}

pub fn math_absolute(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::Number(_)] => {
            let a = Internable::to_number(params[0]);
            Some(Internable::from_number(a.abs()))
        },
        _ => { None }
    }
}

pub fn math_mod(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::Number(_), &Internable::Number(_)] => {
            let a = Internable::to_number(params[0]);
            let b = Internable::to_number(params[1]);
            Some(Internable::from_number(a % b))
        },
        _ => { None }
    }
}

pub fn math_pow(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::Number(_), &Internable::Number(_)] => {
            let value = Internable::to_number(params[0]);
            let exp = Internable::to_number(params[1]);
            Some(Internable::from_number(value.powf(exp)))
        },
        _ => { None }
    }
}

pub fn math_to_fixed(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::Number(_), &Internable::Number(_)] => {
            let value = Internable::to_number(params[0]);
            let places = Internable::to_number(params[1]);
            Some(Internable::String(format!("{:.*}", places as usize, value)))
        },
        _ => { None }
    }
}

pub fn math_to_hex(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::Number(_)] => {
            let value = Internable::to_number(params[0]);
            Some(Internable::String(format!("{:x}", value as i64)))
        },
        _ => { None }
    }
}

pub fn math_ceiling(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::Number(_)] => {
            let a = Internable::to_number(params[0]);
            Some(Internable::from_number(a.ceil()))
        },
        _ => { None }
    }
}

pub fn math_floor(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::Number(_)] => {
            let a = Internable::to_number(params[0]);
            Some(Internable::from_number(a.floor()))
        },
        _ => { None }
    }
}

pub fn math_round(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::Number(_)] => {
            let a = Internable::to_number(params[0]);
            Some(Internable::from_number(a.round()))
        },
        _ => { None }
    }
}

pub fn math_range(params: Vec<&Internable>) -> Option<Vec<Vec<Internable>>> {
    match params.as_slice() {
        &[&Internable::Number(_), &Internable::Number(_)] => {
            let from = Internable::to_number(params[0]) as i64;
            let to = Internable::to_number(params[1]) as i64;

            Some((from..to+1).map(|x| vec![Internable::from_number(x as f32)]).collect())
        },
        _ => { None }
    }
}

pub fn random_number(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::Number(seed)] => {
            let mut rng = XorShiftRng::from_seed([0x123, seed, !seed, seed]);
            Some(Internable::from_number(rng.next_f32()))
        },
        &[&Internable::String(ref text)] => {
            let mut hash = DefaultHasher::new();
            text.hash(&mut hash);
            let seed = hash.finish();
            let top = (seed << 32) as u32;
            let bottom = (seed >> 32) as u32;
            let mut rng = XorShiftRng::from_seed([0x123, top, top - bottom, bottom]);
            Some(Internable::from_number(rng.next_f32()))
        },
        _ => { None }
    }
}

pub fn string_replace(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::String(ref text), &Internable::String(ref replace), &Internable::String(ref with)] => {
            Some(Internable::String(text.replace(replace, with)))
        },
        _ => { None }
    }
}

pub fn string_contains(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::String(ref text), &Internable::String(ref substring)] => {
            if text.contains(substring) {
                Some(Internable::String("true".to_owned()))
            } else {
                None
            }
        },
        _ => { None }
    }
}

pub fn string_lowercase(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::String(ref text)] => Some(Internable::String(text.to_lowercase())),
        _ => None
    }
}

pub fn string_uppercase(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::String(ref text)] => Some(Internable::String(text.to_uppercase())),
        _ => None
    }
}

pub fn string_length(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::String(ref text)] => {
            Some(Internable::from_number(UnicodeSegmentation::graphemes(text.as_str(), true).count() as f32))
        },
        _ => None
    }
}

pub fn string_encode(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::String(ref text)] => Some(Internable::String(base64::encode(text.as_bytes()))),
        _ => None
    }
}

pub fn string_urlencode(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::String(ref text)] => Some(Internable::String(urlencoding::encode(text))),
        _ => None
    }
}


pub fn string_substring(params: Vec<&Internable>) -> Option<Internable> {
    let params_slice = params.as_slice();
    match params_slice {
        &[&Internable::String(ref text), ..] => {
            let graphemes:Vec<&str> = UnicodeSegmentation::graphemes(text.as_str(), true).collect();
            let length = graphemes.len();

            let (from, to) = match params_slice {
                &[_, &Internable::Number(_), &Internable::Number(_)] => (Internable::to_number(params[1]) as isize, Internable::to_number(params[2]) as isize),
                &[_, _, &Internable::Number(_)] => (1 as isize, Internable::to_number(params[2]) as isize),
                &[_, &Internable::Number(_), _] => (Internable::to_number(params[1]) as isize, (length + 1) as isize),
                _ => (1 as isize, 1 as isize)
            };
            let start = if from < 1 { length - from.abs() as usize } else { (from - 1) as usize };
            let end = if to < 1 { length - to.abs() as usize} else { (to - 1) as usize };

            if start > end {
                None
            } else {
                Some(Internable::String(graphemes[start..end].join("")))
            }
        },
        _ => None
    }
}


pub fn string_index_of(params: Vec<&Internable>) -> Option<Vec<Vec<Internable>>> {
    match params.as_slice() {
        &[&Internable::String(ref text), &Internable::String(ref substring)] => {
            let results = text.match_indices(substring).map(|(ix, _)| {
                vec![Internable::from_number((ix + 1) as f32)]
            }).collect();
            Some(results)
        },
        _ => { None }
    }
}

pub fn string_split(params: Vec<&Internable>) -> Option<Vec<Vec<Internable>>> {
    match params.as_slice() {
        &[&Internable::String(ref text), &Internable::String(ref by)] => {
            let results = text.split(by).enumerate().map(|(ix, v)| {
                vec![Internable::String(v.to_string()), Internable::from_number((ix + 1) as f32)]
            }).collect();
            Some(results)
        },
        _ => { None }
    }
}
pub fn string_split_reverse(params: Vec<&Internable>) -> Option<Vec<Vec<Internable>>> {
    match params.as_slice() {
        &[&Internable::String(ref text), &Internable::String(ref by)] => {
            let results = text.rsplit(by).enumerate().map(|(ix, v)| {
                vec![Internable::String(v.to_string()), Internable::from_number((ix + 1) as f32)]
            }).collect();
            Some(results)
        },
        _ => { None }
    }
}

pub fn concat(params: Vec<&Internable>) -> Option<Internable> {
    let mut result = String::new();
    for param in params {
        match param {
            &Internable::String(ref string) => {
                result.push_str(string);
            },
            &Internable::Number(_) => {
                result.push_str(&Internable::to_number(param).to_string());
            },
            _ => {}
        }
    }
    Some(Internable::String(result))
}

pub fn gen_id(params: Vec<&Internable>) -> Option<Internable> {
    let mut result = String::new();
    for param in params {
        match param {
            &Internable::String(ref string) => {
                result.push_str(string);
                result.push_str("|");
            },
            &Internable::Number(_) => {
                result.push_str(&Internable::to_number(param).to_string());
                result.push_str("|");
            },
            _ => {}
        }
    }
    Some(Internable::String(result))
}

//-------------------------------------------------------------------------
// Aggregates
//-------------------------------------------------------------------------

pub fn aggregate_sum_add(current: &mut AggregateEntry, params: &Vec<Internable>) {
    match params.as_slice() {
        &[ref param @ Internable::Number(_)] => {
            let value = Internable::to_number(param);
            match current {
                &mut AggregateEntry::Result(ref mut res) => { *res = *res + value; }
                _ => { *current = AggregateEntry::Result(value); }
            }
        }
        _ => {}
    };
}

pub fn aggregate_sum_remove(current: &mut AggregateEntry, params: &Vec<Internable>) {
    match params.as_slice() {
        &[ref param @ Internable::Number(_)] => {
            let value = Internable::to_number(param);
            match current {
                &mut AggregateEntry::Result(ref mut res) => { *res = *res - value; }
                _ => { *current = AggregateEntry::Result(-1.0 * value); }
            }
        }
        _ => {}
    };
}

pub fn aggregate_count_add(current: &mut AggregateEntry, _: &Vec<Internable>) {
    match current {
        &mut AggregateEntry::Result(ref mut res) => { *res = *res + 1.0; }
        _ => { *current = AggregateEntry::Result(1.0); }
    }
}

pub fn aggregate_count_remove(current: &mut AggregateEntry, _: &Vec<Internable>) {
    match current {
        &mut AggregateEntry::Result(ref mut res) => { *res = *res - 1.0; }
        _ => { *current = AggregateEntry::Result(-1.0); }
    }
}

pub fn aggregate_avg_add(current: &mut AggregateEntry, params: &Vec<Internable>) {
    match params.as_slice() {
        &[ref param @ Internable::Number(_)] => {
            let value = Internable::to_number(param);
            match current {
                &mut AggregateEntry::Counted {ref mut count, ref mut sum, ref mut result } => {
                    *sum += value;
                    *count += 1.0;
                    *result = *sum / *count;
                }
                _ => { *current = AggregateEntry::Counted { count:1.0, sum: value, result:value }; }
            }
        }
        _ => {}
    };
}

pub fn aggregate_avg_remove(current: &mut AggregateEntry, params: &Vec<Internable>) {
    match params.as_slice() {
        &[ref param @ Internable::Number(_)] => {
            let value = Internable::to_number(param);
            match current {
                &mut AggregateEntry::Counted {ref mut count, ref mut sum, ref mut result, } => {
                    *sum -= value;
                    *count -= 1.0;
                    if *count > 0.0 {
                        *result = *sum / *count;
                    } else {
                        *result = 0.0;
                    }
                }
                _ => { *current = AggregateEntry::Counted { count:0.0, sum: 0.0, result:0.0 }; }
            }
        }
        _ => {}
    };
}

//-------------------------------------------------------------------------
// Sort Aggregates
//-------------------------------------------------------------------------

fn is_aggregate_in_round(&(_, v): &(&Vec<Internable>, &Vec<Count>), round:Round) -> bool {
    let sum:Count = v.iter().filter(|cur| cur.abs() <= round as i32).map(|&cur| if cur < 0 { -1 } else { 1 }).sum();
    sum > 0
}

pub fn aggregate_top_add(current: &mut AggregateEntry, params: &Vec<Internable>) {
    if let &mut AggregateEntry::Sorted { ref mut items, current_round, ref current_params, ref mut changes, ..} = current {
        if let &Some(ref limit_params) = current_params {
            let limit_param = limit_params.get(0);
            if let Some(interned_limit @ &Internable::Number(_)) = limit_param {
                let limit = Internable::to_number(interned_limit) as usize;
                let mut iter = items.iter().rev().filter(|entry| {
                    entry.0.last() == limit_param &&
                    is_aggregate_in_round(entry, current_round)
                }).skip(limit - 1);
                let mut local_params = params.clone();
                local_params.push(interned_limit.clone());
                match iter.next() {
                    Some((v, _)) => {
                        if &local_params > v {
                            // remove v
                            changes.push((v.clone(), current_round, -1));
                            // insert params
                            changes.push((local_params, current_round, 1));
                        }
                    }
                    _ => {
                        // insert params
                        changes.push((local_params, current_round, 1));
                    }
                }
            }
        }
    }
}

pub fn aggregate_top_remove(current: &mut AggregateEntry, params: &Vec<Internable>) {
    if let &mut AggregateEntry::Sorted { ref mut items, current_round, ref current_params, ref mut changes, ..} = current {
        if let &Some(ref limit_params) = current_params {
            let limit_param = limit_params.get(0);
            if let Some(interned_limit @ &Internable::Number(_)) = limit_param {
                let limit = Internable::to_number(interned_limit) as usize;
                let mut iter = items.iter().rev().filter(|entry| {
                    entry.0.last() == limit_param &&
                    is_aggregate_in_round(entry, current_round)
                }).skip(limit - 1);
                let mut local_params = params.clone();
                local_params.push(interned_limit.clone());
                match iter.next() {
                    Some((v, _)) => {
                        if &local_params >= v {
                            // remove v
                            changes.push((local_params, current_round, -1));
                            // insert params
                            if let Some((neue_max, _)) = iter.next() {
                                let neue = neue_max.clone();
                                changes.push((neue, current_round, 1));
                            }
                        }
                    }
                    _ => {
                        // remove params
                        changes.push((local_params, current_round, -1));
                    }
                }
            }
        }
    }
}

pub fn aggregate_bottom_add(current: &mut AggregateEntry, params: &Vec<Internable>) {
    if let &mut AggregateEntry::Sorted { ref mut items, current_round, ref current_params, ref mut changes, ..} = current {
        if let &Some(ref limit_params) = current_params {
            let limit_param = limit_params.get(0);
            if let Some(interned_limit @ &Internable::Number(_)) = limit_param {
                let limit = Internable::to_number(interned_limit) as usize;
                let mut iter = items.iter().filter(|entry| {
                    entry.0.last() == limit_param &&
                    is_aggregate_in_round(entry, current_round)
                }).skip(limit - 1);
                let mut local_params = params.clone();
                local_params.push(interned_limit.clone());
                match iter.next() {
                    Some((v, _)) => {
                        if &local_params < v {
                            // remove v
                            changes.push((v.clone(), current_round, -1));
                            // insert params
                            changes.push((local_params, current_round, 1));
                        }
                    }
                    _ => {
                        // insert params
                        changes.push((local_params, current_round, 1));
                    }
                }
            }
        }
    }
}

pub fn aggregate_bottom_remove(current: &mut AggregateEntry, params: &Vec<Internable>) {
    if let &mut AggregateEntry::Sorted { ref mut items, current_round, ref current_params, ref mut changes, ..} = current {
        if let &Some(ref limit_params) = current_params {
            let limit_param = limit_params.get(0);
            if let Some(interned_limit @ &Internable::Number(_)) = limit_param {
                let limit = Internable::to_number(interned_limit) as usize;
                let mut iter = items.iter().filter(|entry| {
                    entry.0.last() == limit_param &&
                    is_aggregate_in_round(entry, current_round)
                }).skip(limit - 1);
                let mut local_params = params.clone();
                local_params.push(interned_limit.clone());
                match iter.next() {
                    Some((v, _)) => {
                        if &local_params <= v {
                            // remove v
                            changes.push((local_params, current_round, -1));
                            // insert params
                            if let Some((neue_max, _)) = iter.next() {
                                let neue = neue_max.clone();
                                changes.push((neue, current_round, 1));
                            }
                        }
                    }
                    _ => {
                        // remove params
                        changes.push((local_params, current_round, -1));
                    }
                }
            }
        }
    }
}

pub fn aggregate_next_add(current: &mut AggregateEntry, params: &Vec<Internable>) {
    if let &mut AggregateEntry::Sorted { ref mut items, current_round, ref current_params, ref mut changes, ..} = current {
        if let &Some(_) = current_params {
            let prev = items.range::<Vec<Internable>, _>((Bound::Unbounded, Bound::Excluded(params)))
                .rev()
                .filter(|entry| is_aggregate_in_round(entry, current_round))
                .next();
            let next = items.range::<Vec<Internable>, _>((Bound::Excluded(params), Bound::Unbounded))
                .filter(|entry| is_aggregate_in_round(entry, current_round))
                .next();
            match prev {
                Some((v, _)) => {
                    let mut neue = v.clone();
                    neue.extend(params.iter().cloned());
                    changes.push((neue, current_round, 1));
                    if let Some((prev_next, _)) = next {
                        let mut neue = v.clone();
                        neue.extend(prev_next.iter().cloned());
                        changes.push((neue, current_round, -1));
                    }
                }
                _ => { }
            }
            if let Some((params_next, _)) = next {
                let mut neue = params.clone();
                neue.extend(params_next.iter().cloned());
                changes.push((neue, current_round, 1));
            }
        }
    }
}

pub fn aggregate_next_remove(current: &mut AggregateEntry, params: &Vec<Internable>) {
    if let &mut AggregateEntry::Sorted { ref mut items, current_round, ref current_params, ref mut changes, ..} = current {
        if let &Some(_) = current_params {
            let prev = items.range::<Vec<Internable>, _>((Bound::Unbounded, Bound::Excluded(params)))
                .rev()
                .filter(|entry| is_aggregate_in_round(entry, current_round))
                .next();
            let next = items.range::<Vec<Internable>, _>((Bound::Excluded(params), Bound::Unbounded))
                .filter(|entry| is_aggregate_in_round(entry, current_round))
                .next();
            match prev {
                Some((v, _)) => {
                    let mut neue = v.clone();
                    neue.extend(params.iter().cloned());
                    changes.push((neue, current_round, -1));
                    if let Some((prev_next, _)) = next {
                        let mut neue = v.clone();
                        neue.extend(prev_next.iter().cloned());
                        changes.push((neue, current_round, 1));
                    }
                }
                _ => { }
            }
            if let Some((params_next, _)) = next {
                let mut neue = params.clone();
                neue.extend(params_next.iter().cloned());
                changes.push((neue, current_round, -1));
            }
        }
    }
}

pub fn aggregate_prev_add(current: &mut AggregateEntry, params: &Vec<Internable>) {
    if let &mut AggregateEntry::Sorted { ref mut items, current_round, ref current_params, ref mut changes, ..} = current {
        if let &Some(_) = current_params {
            let next = items.range::<Vec<Internable>, _>((Bound::Unbounded, Bound::Excluded(params)))
                .rev()
                .filter(|entry| is_aggregate_in_round(entry, current_round))
                .next();
            let prev = items.range::<Vec<Internable>, _>((Bound::Excluded(params), Bound::Unbounded))
                .filter(|entry| is_aggregate_in_round(entry, current_round))
                .next();
            match prev {
                Some((v, _)) => {
                    let mut neue = v.clone();
                    neue.extend(params.iter().cloned());
                    changes.push((neue, current_round, 1));
                    if let Some((prev_next, _)) = next {
                        let mut neue = v.clone();
                        neue.extend(prev_next.iter().cloned());
                        changes.push((neue, current_round, -1));
                    }
                }
                _ => { }
            }
            if let Some((params_next, _)) = next {
                let mut neue = params.clone();
                neue.extend(params_next.iter().cloned());
                changes.push((neue, current_round, 1));
            }
        }
    }
}

pub fn aggregate_prev_remove(current: &mut AggregateEntry, params: &Vec<Internable>) {
    if let &mut AggregateEntry::Sorted { ref mut items, current_round, ref current_params, ref mut changes, ..} = current {
        if let &Some(_) = current_params {
            let next = items.range::<Vec<Internable>, _>((Bound::Unbounded, Bound::Excluded(params)))
                .rev()
                .filter(|entry| is_aggregate_in_round(entry, current_round))
                .next();
            let prev = items.range::<Vec<Internable>, _>((Bound::Excluded(params), Bound::Unbounded))
                .filter(|entry| is_aggregate_in_round(entry, current_round))
                .next();
            match prev {
                Some((v, _)) => {
                    let mut neue = v.clone();
                    neue.extend(params.iter().cloned());
                    changes.push((neue, current_round, -1));
                    if let Some((prev_next, _)) = next {
                        let mut neue = v.clone();
                        neue.extend(prev_next.iter().cloned());
                        changes.push((neue, current_round, 1));
                    }
                }
                _ => { }
            }
            if let Some((params_next, _)) = next {
                let mut neue = params.clone();
                neue.extend(params_next.iter().cloned());
                changes.push((neue, current_round, -1));
            }
        }
    }
}

//-------------------------------------------------------------------------
// Bit helpers
//-------------------------------------------------------------------------

pub fn check_bits(solved:u64, checking:u64) -> bool {
    solved & checking == checking
}

pub fn has_any_bits(solved:u64, checking:u64) -> bool {
    solved & checking != 0
}

pub fn set_bit(solved:u64, bit:usize) -> u64 {
    solved | (1 << bit)
}

pub fn clear_bit(solved:u64, bit:usize) -> u64 {
    solved & !(1 << bit)
}

pub fn check_bit(solved:u64, bit:usize) -> bool {
   solved & (1 << bit) != 0
}

//-------------------------------------------------------------------------
// Round holder
//-------------------------------------------------------------------------

type RoundCount = (Round, Count);

fn collapse_rounds(results:&Vec<RoundCount>, collapsed: &mut Vec<RoundCount>) {
    collapsed.clear();
    let mut prev = (0,0);
    for &(round, count) in results {
        if round == prev.0 {
            prev.1 += count;
        } else {
            if prev.1 != 0 { collapsed.push(prev); }
            prev = (round, count);
        }
    }
    if prev.1 != 0 { collapsed.push(prev); }
}

pub struct OutputRounds {
    pub output_rounds: Vec<RoundCount>,
    prev_output_rounds: Vec<RoundCount>,
    temp_results: Vec<RoundCount>,
}

impl OutputRounds {
    pub fn new() -> OutputRounds {
        OutputRounds { output_rounds:vec![], prev_output_rounds:vec![], temp_results:vec![] }
    }

    pub fn get_output_rounds(&self) -> &Vec<RoundCount> {
        match (self.output_rounds.len(), self.prev_output_rounds.len()) {
            (0, _) => &self.prev_output_rounds,
            (_, 0) => &self.output_rounds,
            (_, _) => panic!("neither round array is empty"),
        }
    }

    fn fetch_neue_current(&mut self) -> (&mut Vec<RoundCount>, &mut Vec<RoundCount>, &mut Vec<RoundCount>) {
        match (self.output_rounds.len(), self.prev_output_rounds.len()) {
            (0, _) => (&mut self.output_rounds, &mut self.prev_output_rounds, &mut self.temp_results),
            (_, 0) => (&mut self.prev_output_rounds, &mut self.output_rounds, &mut self.temp_results),
            (_, _) => panic!("neither round array is empty"),
        }
    }

    pub fn compute_anti_output_rounds(&mut self, right_iter: DistinctIter) {
        let (neue, current, result) = self.fetch_neue_current();
        result.clear();

        for x in current.drain(..) {
            for y in right_iter.clone() {
                let round = cmp::max(x.0, y.0);
                let count = x.1 * y.1 * -1;
                result.push((round, count))
            }
            result.push(x);
        }
        result.sort();

        collapse_rounds(&result, neue);
    }

    pub fn compute_output_rounds(&mut self, right_iter: DistinctIter) {
        let (neue, current, result) = self.fetch_neue_current();
        result.clear();

        for x in current.drain(..) {
            for y in right_iter.clone() {
                let round = cmp::max(x.0, y.0);
                let count = x.1 * y.1;
                result.push((round, count))
            }
        }
        result.sort();
        collapse_rounds(&result, neue);
    }

    pub fn clear(&mut self) {
        self.output_rounds.clear();
        self.prev_output_rounds.clear();
    }
}

pub struct RoundHolder {
    rounds: Vec<HashMap<(Interned,Interned,Interned), Change>>,
    commits: HashMap<(Interned, Interned, Interned, Interned), (ChangeType, Change)>,
    staged_commit_keys: Vec<(Interned, Interned, Interned, Interned)>,
    collapsed_commits: CollapsedChanges,
    pub max_round: usize,
}


impl RoundHolder {
    pub fn new() -> RoundHolder {
        let mut rounds = vec![];
        for _ in 0..100 {
            rounds.push(HashMap::new());
        }
        RoundHolder { rounds, commits:HashMap::new(), staged_commit_keys:vec![], collapsed_commits:CollapsedChanges::new(), max_round: 0 }
    }

    pub fn insert(&mut self, change:Change) {
        let key = (change.e, change.a, change.v);
        let round = change.round as usize;
        self.max_round = cmp::max(round, self.max_round);
        match self.rounds[round].entry(key) {
            Entry::Occupied(mut o) => {
                o.get_mut().count += change.count;
            }
            Entry::Vacant(o) => {
                o.insert(change);
            }
        };
    }

    pub fn commit(&mut self, change:Change, change_type:ChangeType) {
        let key = (change.n, change.e, change.a, change.v);
        if change.a == 0 || change.v == 0 {
            self.staged_commit_keys.push(key);
        }
        match self.commits.entry(key) {
            Entry::Occupied(mut o) => {
                o.get_mut().1.count += change.count;
            }
            Entry::Vacant(o) => {
                o.insert((change_type, change));
            }
        };
    }

    pub fn prepare_commits(&mut self, index:&mut HashIndex, distinct_index:&mut DistinctIndex) -> bool {
        for key in self.staged_commit_keys.iter() {
            match self.commits.get(key) {
                Some(&(ChangeType::Remove, Change {count, e, a, v, n, transaction, round})) => {
                    if count < 0 {
                        // do the index lookups and commit the changes
                        match (a, v) {
                            (0, 0) => {
                                if let Some(attrs) = index.get(e, 0, 0) {
                                    for attr in attrs {
                                        if let Some(vals) = index.get(e, attr, 0) {
                                            for val in vals {
                                                match distinct_index.get(e,attr,val) {
                                                    Some(entry) => {
                                                        if entry.rounds[0] > 0 {
                                                            let cloned = Change {e, a:attr, v:val, n, count, transaction, round};
                                                            self.collapsed_commits.insert(cloned);
                                                        }
                                                    }
                                                    _ => {}
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            (_, 0) => {
                                if let Some(vals) = index.get(e, a, 0) {
                                    for val in vals {
                                        match distinct_index.get(e,a,val) {
                                            Some(entry) => {
                                                if entry.rounds[0] > 0 {
                                                    let cloned = Change {e, a, v:val, n, count, transaction, round};
                                                    self.collapsed_commits.insert(cloned);
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            },
                            _ => { panic!("Staged remove that is completely filled in"); }
                        }
                    }
                    self.commits.remove(key);
                },
                None => {},
                _ => { panic!("Invalid staged commit"); }
            }
        }
        self.staged_commit_keys.clear();
        for info in self.commits.values() {
            match info {
                &(ChangeType::Insert, Change {count, ..}) => {
                    if count > 0 { self.collapsed_commits.insert(info.1); }

                }
                &(ChangeType::Remove, Change {count, ..}) => {
                    if count < 0 { self.collapsed_commits.insert(info.1); }
                }
            }
        }
        self.commits.clear();
        let mut has_changes = false;
        // @FIXME: There should be some way for us to not have to allocate a vec here
        let drained = { self.collapsed_commits.drain().collect::<Vec<Change>>() };
        for change in drained {
            has_changes = true;
            // apply it
            distinct_index.distinct(&change, self);
        }
        has_changes
    }

    pub fn clear(&mut self) {
        for ix in 0..self.max_round {
            self.rounds[ix].clear();
        }
        self.max_round = 0;
    }

    pub fn iter(&self) -> RoundHolderIter {
        RoundHolderIter::new()
    }
}

pub struct RoundHolderIter {
    round_ix: usize,
    change_ix: usize,
    cur_changes: Vec<Change>,
}

impl RoundHolderIter {
    pub fn new() -> RoundHolderIter {
        RoundHolderIter { round_ix: 0, change_ix: 0, cur_changes: vec![] }
    }

    pub fn next(&mut self, holder: &mut RoundHolder) -> Option<Change> {
        if self.change_ix >= self.cur_changes.len() {
            self.next_round(holder);
        }
        let cur = self.change_ix;
        self.change_ix = cur + 1;
        match self.cur_changes.get(cur) {
            None => None,
            Some(&change) => Some(change.clone()),
        }
    }

    pub fn next_round(&mut self, holder: &mut RoundHolder) -> &Vec<Change> {
        let mut round_ix = self.round_ix;
        let max_round = holder.max_round;
        {
            let ref mut cur_changes = self.cur_changes;
            cur_changes.clear();
            self.change_ix = 0;
            while round_ix <= max_round + 1 && cur_changes.len() == 0 {
                for (_, change) in holder.rounds[round_ix].drain().filter(|v| v.1.count != 0) {
                    cur_changes.push(change);
                }
                round_ix += 1;
            }
        }
        self.round_ix = round_ix;
        &self.cur_changes
    }

    pub fn get_round(&mut self, holder: &mut RoundHolder, round: Round) -> &Vec<Change> {
        {
            let ref mut cur_changes = self.cur_changes;
            cur_changes.clear();
            self.change_ix = 0;
            for (_, change) in holder.rounds[round as usize].drain().filter(|v| v.1.count != 0) {
                cur_changes.push(change);
            }
        }
        self.round_ix = (round as usize) + 1;
        &self.cur_changes
    }

}

//-------------------------------------------------------------------------
// Program
//-------------------------------------------------------------------------

pub struct RuntimeState {
    pub debug: bool,
    pub rounds: RoundHolder,
    pub output_rounds: OutputRounds,
    pub index: HashIndex,
    pub distinct_index: DistinctIndex,
    pub remote_index: RemoteIndex,
    pub interner: Interner,
    pub watch_indexes: HashMap<String, WatchIndex>,
    pub intermediates: IntermediateIndex,
}

pub struct BlockInfo {
    pub pipe_lookup: HashMap<(Interned,Interned,Interned), Vec<Solver>>,
    pub intermediate_pipe_lookup: HashMap<Interned, Vec<Solver>>,
    pub remote_pipe_lookup: HashMap<Interned, Vec<Solver>>,
    pub block_names: HashMap<String, usize>,
    pub blocks: Vec<Block>,
}

impl BlockInfo {
    pub fn get_block(&self, name:&str) -> &Block {
        let ix = self.block_names.get(name).unwrap();
        &self.blocks[*ix]
    }

}

pub enum RunLoopMessage {
    Stop,
    Transaction(Vec<RawChange>),
    RemoteTransaction(Vec<RawRemoteChange>),
    CodeTransaction(Vec<Block>, Vec<String>),
    RemoteCodeTransaction(Vec<PortableBlock>, Vec<String>)
}

pub struct Program {
    pub name: String,
    pub state: RuntimeState,
    pub block_info: BlockInfo,
    watchers: HashMap<String, Box<Watcher + Send>>,
    pub incoming: Receiver<RunLoopMessage>,
    pub outgoing: Sender<RunLoopMessage>,
}

impl Program {
    pub fn new(name:&str) -> Program {
        let index = HashIndex::new();
        let distinct_index = DistinctIndex::new();
        let remote_index = RemoteIndex::new();
        let intermediates = IntermediateIndex::new();
        let interner = Interner::new();
        let rounds = RoundHolder::new();
        let output_rounds = OutputRounds::new();
        let block_names = HashMap::new();
        let watch_indexes = HashMap::new();
        let watchers = HashMap::new();
        let pipe_lookup = HashMap::new();
        let intermediate_pipe_lookup = HashMap::new();
        let remote_pipe_lookup = HashMap::new();
        let blocks = vec![];
        let (outgoing, incoming) = mpsc::channel();
        let state = RuntimeState { debug:false, rounds, remote_index, output_rounds, index, distinct_index, interner, watch_indexes, intermediates };
        let block_info = BlockInfo { pipe_lookup, remote_pipe_lookup, intermediate_pipe_lookup, block_names, blocks };
        Program { name: name.to_owned(), state, block_info, watchers, incoming, outgoing }
    }

    pub fn clear(&mut self) {
        self.state.index = HashIndex::new();
    }

    #[allow(dead_code)]
    pub fn exec_query(&mut self, name:&str) -> Vec<Interned> {
        let mut frame = Frame::new();
        let mut iter_pool = EstimateIterPool::new();
        // let start_ns = time::precise_time_ns();
        self.block_info.get_block(name).run(&mut self.state, &mut iter_pool, &mut frame);
        // frame.counters.total_ns += time::precise_time_ns() - start_ns;
        // println!("counters: {:?}", frame.counters);
        return frame.results;
    }

    #[allow(dead_code)]
    pub fn raw_insert(&mut self, e:Interned, a:Interned, v:Interned, round:Round, count:Count) {
        self.state.distinct_index.raw_insert(e,a,v,round,count);
        if count > 0 {
            self.state.distinct_index.insert_active(e,a,v,round);
            self.state.index.insert(e,a,v);
        } else {
            self.state.distinct_index.remove_active(e,a,v,round);
            self.state.index.remove(e,a,v);
        }
    }

    pub fn register_block(&mut self, mut block:Block) {
        let ix = self.block_info.blocks.len();
        let mut pipes = block.gen_pipes(&mut self.state.interner);
        for (pipe, shapes) in pipes.drain(..).zip(block.shapes.iter()) {
            for shape in shapes {
                match shape {
                    &PipeShape::Scan(e,a,v) => {
                        let cur = self.block_info.pipe_lookup.entry((e,a,v)).or_insert_with(|| vec![]);
                        cur.push(pipe.clone());
                    }
                    &PipeShape::Intermediate(id) => {
                        let cur = self.block_info.intermediate_pipe_lookup.entry(id).or_insert_with(|| vec![]);
                        cur.push(pipe.clone());
                    }
                    &PipeShape::Remote(id) => {
                        let cur = self.block_info.remote_pipe_lookup.entry(id).or_insert_with(|| vec![]);
                        cur.push(pipe.clone());
                    }
                }
            }
        }
        self.block_info.block_names.insert(block.name.to_string(), ix);
        self.block_info.blocks.push(block);
    }

    pub fn unregister_block(&mut self, name:String) {
        if let Some(block_ix) = self.block_info.block_names.remove(&name) {
            let block = self.block_info.blocks.swap_remove(block_ix);
            if let Some(neue) = self.block_info.blocks.get(block_ix) {
                self.block_info.block_names.insert(neue.name.to_owned(), block_ix);
            }
            for shape_set in block.shapes.iter() {
                for shape in shape_set.iter() {
                    match shape {
                        &PipeShape::Scan(e, a, v) => {
                            self.block_info.pipe_lookup.get_mut(&(e, a, v)).unwrap().retain(|x| x.block != block.block_id);
                        },
                        &PipeShape::Intermediate(id) => {
                            self.block_info.intermediate_pipe_lookup.get_mut(&id).unwrap().retain(|x| x.block != block.block_id);
                        }
                        &PipeShape::Remote(id) => {
                            self.block_info.remote_pipe_lookup.get_mut(&id).unwrap().retain(|x| x.block != block.block_id);
                        }
                    }
                }
            }
        }
    }

    pub fn insert_block(&mut self, name:&str, code:&str) {
        let bs = make_block(&mut self.state.interner, name, code);
        for b in bs {
            self.register_block(b);
        }
    }

    pub fn block(&mut self, name:&str, code:&str) -> CodeTransaction {
        let bs = make_block(&mut self.state.interner, name, code);
        let mut txn = CodeTransaction::new();
        txn.exec(self, bs, vec![]);
        txn
    }

    pub fn raw_block(&mut self, block:Block) {
        self.register_block(block);
    }

    pub fn attach(&mut self, watcher:Box<Watcher + Send>) {
        let name = watcher.get_name();
        println!("[{}] {} {}", &self.name, BrightCyan.paint("Loaded Watcher:"), name);
        self.watchers.insert(name, watcher);
    }

    pub fn get_pipes<'a>(&self, block_info:&'a BlockInfo, input: &Change, pipes: &mut HashSet<&'a Solver>) {
        let ref pipe_lookup = block_info.pipe_lookup;
        let mut tuple = (0,0,0);
        // look for (0,0,0), (e, 0, 0), (0, a, 0) and (0, a, v) pipes
        match pipe_lookup.get(&tuple) {
            Some(found) => {
                for pipe in found.iter() {
                    pipes.insert(pipe);
                }
            },
            None => {},
        }
        tuple.0 = input.e;
        match pipe_lookup.get(&tuple) {
            Some(found) => {
                for pipe in found.iter() {
                    pipes.insert(pipe);
                }
            },
            None => {},
        }
        tuple.0 = 0;
        tuple.1 = input.a;
        match pipe_lookup.get(&tuple) {
            Some(found) => {
                for pipe in found.iter() {
                    pipes.insert(pipe);
                }
            },
            None => {},
        }
        tuple.2 = input.v;
        match pipe_lookup.get(&tuple) {
            Some(found) => {
                for pipe in found.iter() {
                    pipes.insert(pipe);
                }
            },
            None => {},
        }
        // lookup the tags for this e
        //  for each tag, lookup (e, 0, 0), (e, a, 0) and (e, a, v)
        if let Some(tags) = self.state.index.get(input.e, TAG_INTERNED_ID, 0) {
            for tag in tags {
                tuple.0 = tag;
                tuple.1 = 0;
                tuple.2 = 0;
                match pipe_lookup.get(&tuple) {
                    Some(found) => {
                        for pipe in found.iter() {
                            pipes.insert(pipe);
                        }
                    },
                    None => {},
                }
                tuple.1 = input.a;
                match pipe_lookup.get(&tuple) {
                    Some(found) => {
                        for pipe in found.iter() {
                            pipes.insert(pipe);
                        }
                    },
                    None => {},
                }
                tuple.2 = input.v;
                match pipe_lookup.get(&tuple) {
                    Some(found) => {
                        for pipe in found.iter() {
                            pipes.insert(pipe);
                        }
                    },
                    None => {},
                }
            }
        }
    }
}

//-------------------------------------------------------------------------
// Transaction
//-------------------------------------------------------------------------

fn intermediate_flow(frame: &mut Frame, state: &mut RuntimeState, block_info: &BlockInfo, iter_pool:&mut EstimateIterPool, current_round:Round, max_round:&mut Round) {
    let mut intermediate_max = state.intermediates.consume_round();
    *max_round = cmp::max(*max_round, intermediate_max);
    if let Some(_) = state.intermediates.rounds.get(&current_round) {
        let mut remaining:Vec<(Vec<Interned>, IntermediateChange)> = state.intermediates.rounds.get_mut(&current_round).unwrap().drain().collect();
        while remaining.len() > 0 {
            for (_, cur) in remaining {
                if cur.count == 0 { continue; }
                // println!("Int: {:?} {}:{}  neg?:{}", cur.key, cur.round, cur.count, cur.negate);
                state.intermediates.update_active_rounds(&cur);
                if let Some(ref actives) = block_info.intermediate_pipe_lookup.get(&cur.key[0]) {
                    frame.reset();
                    frame.intermediate = Some(cur);
                    for pipe in actives.iter() {
                        // print_pipe(pipe, block_info, state);
                        frame.row.reset();
                        pipe.run_intermediate(state, iter_pool, frame);
                        // if state.debug {
                        //     state.debug = false;
                        //     println!("\n---------------------------------\n");
                        // }
                    }
                }
            }
            intermediate_max = state.intermediates.consume_round();
            *max_round = cmp::max(*max_round, intermediate_max);
            remaining = state.intermediates.rounds.get_mut(&current_round).unwrap().drain().collect();
        }
    }
}

fn transaction_flow(commits: &mut Vec<Change>, frame: &mut Frame, iter_pool:&mut EstimateIterPool, program: &mut Program) {
    {
        let mut pipes = HashSet::new();
        let mut next_frame = true;

        while next_frame {
            let mut current_round = 0;
            let mut max_round:Round = program.state.rounds.max_round as Round;
            let mut items = program.state.rounds.iter();
            while current_round <= max_round {
                let round = items.get_round(&mut program.state.rounds, current_round);
                for change in round.iter() {
                    // println!("{}", change.print(&program.state.interner));
                    // If this is an add, we want to do it *before* we start running pipes.
                    // This ensures that if there are two constraints in a single block that
                    // would both match the given input, they both have a chance to see this
                    // new triple at the same time. Doing so, means we don't have to go through
                    // every possible combination of the inputs, e.g. A, B, and AB. Instead we
                    // do AB and BA. To make sure that removes correctly cancel out, we don't
                    // want to do a real remove until *after* the pipes have run. Hence, the
                    // separation of insert and remove.
                    if change.count > 0 {
                        if program.state.distinct_index.insert_active(change.e, change.a, change.v, change.round) {
                            program.state.index.insert(change.e, change.a, change.v);
                        }
                    }
                    pipes.clear();
                    program.get_pipes(&program.block_info, change, &mut pipes);
                    frame.reset();
                    frame.input = Some(*change);
                    for pipe in pipes.iter() {
                        // println!("  PIPE: {:?} - {:?}", pipe.block, pipe.id);
                        frame.row.reset();
                        pipe.run(&mut program.state, iter_pool, frame);
                    }
                    // as stated above, we want to do removes after so that when we look
                    // for AB and BA, they find the same values as when they were added.
                    if change.count < 0 {
                        if program.state.distinct_index.remove_active(change.e, change.a, change.v, change.round) {
                            program.state.index.remove(change.e, change.a, change.v);
                        }
                    }
                    if current_round == 0 { commits.push(change.clone()); }
                }
                intermediate_flow(frame, &mut program.state, &program.block_info, iter_pool, current_round, &mut max_round);
                max_round = cmp::max(max_round, program.state.rounds.max_round as Round);
                current_round += 1;
            }
            next_frame = program.state.rounds.prepare_commits(&mut program.state.index, &mut program.state.distinct_index);
        }
    }

    for (name, index) in program.state.watch_indexes.iter_mut() {
        if index.dirty() {
            let diff = index.reconcile();
            if let Some(watcher) = program.watchers.get_mut(name) {
                watcher.on_diff(&mut program.state.interner, diff);
            }
        }
    }
}

pub struct Transaction<'a> {
    changes: Vec<Change>,
    commits: Vec<Change>,
    iter_pool: &'a mut EstimateIterPool,
    collapsed_commits: CollapsedChanges,
    frame: Frame,
}

impl<'a> Transaction<'a> {
    pub fn new(iter_pool:&mut EstimateIterPool) -> Transaction {
        let frame = Frame::new();
        Transaction { changes: vec![], commits: vec![], collapsed_commits:CollapsedChanges::new(), frame, iter_pool}
    }

    pub fn input(&mut self, e:Interned, a:Interned, v:Interned, count: Count) {
        let change = Change { e,a,v,n: 0, transaction:0, round:0, count };
        self.changes.push(change);
    }

    pub fn input_change(&mut self, change: Change) {
        self.changes.push(change);
    }

    pub fn exec(&mut self, program: &mut Program, persistence_channel: &mut Option<Sender<PersisterMessage>>) {
        for change in self.changes.iter() {
            program.state.distinct_index.distinct(&change, &mut program.state.rounds);
        }
        transaction_flow(&mut self.commits, &mut self.frame, self.iter_pool, program);
        if let &mut Some(ref channel) = persistence_channel {
            self.collapsed_commits.clear();
            let mut to_persist = vec![];
            for commit in self.commits.drain(..) {
                self.collapsed_commits.insert(commit);
            }
            for commit in self.collapsed_commits.drain() {
                to_persist.push(commit.to_raw(&program.state.interner));
            }
            channel.send(PersisterMessage::Write(to_persist)).unwrap();
        } else {
            self.commits.clear();
        }
    }

    pub fn clear(&mut self) {
        self.changes.clear();
        self.commits.clear();
    }
}

//-------------------------------------------------------------------------
// Remote Transaction
//-------------------------------------------------------------------------

pub struct RemoteTransaction<'a> {
    changes: Vec<RemoteChange>,
    commits: Vec<Change>,
    iter_pool: &'a mut EstimateIterPool,
    collapsed_commits: CollapsedChanges,
    frame: Frame,
}

impl<'a> RemoteTransaction<'a> {
    pub fn new(iter_pool:&mut EstimateIterPool) -> RemoteTransaction {
        let frame = Frame::new();
        RemoteTransaction { changes: vec![], commits: vec![], collapsed_commits:CollapsedChanges::new(), frame, iter_pool}
    }

    pub fn input_change(&mut self, change: RemoteChange) {
        self.changes.push(change);
    }

    pub fn exec(&mut self, program: &mut Program, persistence_channel: &mut Option<Sender<PersisterMessage>>) {
        let ref mut frame = self.frame;
        let ref mut iter_pool = self.iter_pool;

        for change in self.changes.drain(..) {
            if let Some(ref pipes) = program.block_info.remote_pipe_lookup.get(&0) {
                frame.reset();
                frame.remote = Some(change.clone());
                for pipe in pipes.iter() {
                    frame.row.reset();
                    pipe.run_remote(&mut program.state, iter_pool, frame);
                }
            }
            if let Some(ref pipes) = program.block_info.remote_pipe_lookup.get(&change._for) {
                frame.reset();
                frame.remote = Some(change.clone());
                for pipe in pipes.iter() {
                    frame.row.reset();
                    pipe.run_remote(&mut program.state, iter_pool, frame);
                }
            }
            program.state.remote_index.insert(change);
        }

        transaction_flow(&mut self.commits, frame, iter_pool, program);
        program.state.remote_index.clear();

        if let &mut Some(ref channel) = persistence_channel {
            self.collapsed_commits.clear();
            let mut to_persist = vec![];
            for commit in self.commits.drain(..) {
                self.collapsed_commits.insert(commit);
            }
            for commit in self.collapsed_commits.drain() {
                to_persist.push(commit.to_raw(&program.state.interner));
            }
            channel.send(PersisterMessage::Write(to_persist)).unwrap();
        } else {
            self.commits.clear();
        }
    }

    pub fn clear(&mut self) {
        self.changes.clear();
        self.commits.clear();
    }
}

//-------------------------------------------------------------------------
// Code Transaction
//-------------------------------------------------------------------------

pub struct CodeTransaction {
    changes: Vec<Change>,
    commits: Vec<Change>,
    iter_pool: EstimateIterPool,
    frame: Frame,
}

impl CodeTransaction {
    pub fn new() -> CodeTransaction {
        let frame = Frame::new();
        let iter_pool = EstimateIterPool::new();
        CodeTransaction { changes: vec![], commits:vec![], frame, iter_pool}
    }

    pub fn input_change(&mut self, change: Change) {
        self.changes.push(change);
    }

    pub fn exec(&mut self, program: &mut Program, to_add:Vec<Block>, to_remove:Vec<String>) {
        let ref mut frame = self.frame;
        let ref mut iter_pool = self.iter_pool;

        for name in to_remove {
            {
                let block_ix = match program.block_info.block_names.get(&name) {
                    Some(v) => *v,
                    _ => panic!("Unable to find block to remove: '{}'", name)
                };

                let remove = &program.block_info.blocks[block_ix];
                frame.reset();
                frame.input = Some(Change { e:0,a:0,v:0,n: 0, transaction:0, round:0, count:-1 });
                remove.run(&mut program.state, iter_pool, frame);
            }
            program.unregister_block(name);
        }

        for add in to_add {
            frame.reset();
            frame.input = Some(Change { e:0,a:0,v:0,n: 0, transaction:0, round:0, count:1 });
            program.register_block(add);
            program.block_info.blocks.last().unwrap().run(&mut program.state, iter_pool, frame);
        }

        let mut max_round = 0;
        intermediate_flow(frame, &mut program.state, &program.block_info, iter_pool, 0, &mut max_round);

        for change in self.changes.iter() {
            program.state.distinct_index.distinct(&change, &mut program.state.rounds);
        }

        transaction_flow(&mut self.commits, frame, iter_pool, program);
    }
}

//-------------------------------------------------------------------------
// Portable Code Transaction
//-------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum PortableField {
    Register(usize),
    Value(Internable)
}
impl PortableField {
    pub fn intern(&self, interner:&mut Interner) -> Field {
        match self {
            &PortableField::Register(ix) => Field::Register(ix),
            &PortableField::Value(ref internable) => Field::Value(interner.internable_to_id(internable.clone()))
        }
    }
}
impl Field {
    pub fn to_portable(&self, interner:&Interner) -> PortableField {
        match self {
            &Field::Register(ix) => PortableField::Register(ix),
            &Field::Value(interned) => PortableField::Value(interner.get_value(interned).clone())
        }
    }
}

pub enum PortableConstraint {
    Scan(PortableField, PortableField, PortableField),
    Output(PortableField, PortableField, PortableField, bool),
    Watch(String, Vec<PortableField>),
    Function(String, PortableField, Vec<PortableField>),
    Variadic(String, PortableField, Vec<PortableField>),
    GenId(PortableField, HashMap<String, PortableField>),
}

impl PortableConstraint {
    pub fn intern(&self, interner:&mut Interner) -> Constraint {
        match self {
            &PortableConstraint::Scan(ref e, ref a, ref v) => make_scan(e.intern(interner), a.intern(interner), v.intern(interner)),
            &PortableConstraint::Output(ref e, ref a, ref v, commit) => Constraint::Insert{e: e.intern(interner), a: a.intern(interner), v: v.intern(interner), commit},
            &PortableConstraint::Watch(ref name, ref registers) => {
                Constraint::Watch {name: name.to_owned(), registers: registers.iter().map(|v| v.intern(interner)).collect()}
            },
            &PortableConstraint::Function(ref name, ref output, ref args) => {
                let params = args.iter().map(|v| v.intern(interner)).collect();
                make_function(name, params, output.intern(interner))
            },

            _ => unimplemented!()
        }
    }
}

impl Constraint {
    pub fn to_portable(&self, i:&Interner) -> PortableConstraint {
        match self {
            &Constraint::Scan{ref e, ref a, ref v, ..} => PortableConstraint::Scan(e.to_portable(i), a.to_portable(i), v.to_portable(i)),
            &Constraint::Insert{ref e, ref a, ref v, commit} => PortableConstraint::Output(e.to_portable(i), a.to_portable(i), v.to_portable(i), commit),
            &Constraint::Watch{ref name, ref registers} => {
                PortableConstraint::Watch(name.to_owned(), registers.iter().map(|v| v.to_portable(i)).collect())
            },
            &Constraint::Function{ref op, ref output, ref params, ..} => {
                PortableConstraint::Function(op.to_owned(), output.to_portable(i), params.iter().map(|v| v.to_portable(i)).collect())
            },

            _ => unimplemented!()
        }
    }
}

pub struct PortableBlock {
    pub name: String,
    pub block_id: Internable,
    pub constraints: Vec<PortableConstraint>
}

impl PortableBlock {
    pub fn intern(&self, interner:&mut Interner) -> Block {
        let constraints = self.constraints.iter().map(|c| c.intern(interner)).collect();
        let block_id = interner.internable_to_id(self.block_id.clone());
        Block::new(interner, &self.name, block_id, constraints)
    }
}

impl Block {
    pub fn to_portable(&self, interner:&Interner) -> PortableBlock {
        let constraints = self.constraints.iter().map(|c| c.to_portable(interner)).collect();
        PortableBlock{name: self.name.clone(), block_id: interner.get_value(self.block_id).clone(), constraints}
    }
}

//-------------------------------------------------------------------------
// Persister
//-------------------------------------------------------------------------

pub enum PersisterMessage {
    Stop,
    Write(Vec<RawChange>),
}

pub struct Persister {
    thread: JoinHandle<()>,
    outgoing: Sender<PersisterMessage>,
    loaded: Vec<RawChange>,
}

impl Persister {
    pub fn new(path_ref:&str) -> Persister {
        let (outgoing, incoming) = mpsc::channel();
        let path = path_ref.to_string();
        let thread = thread::spawn(move || {
            let file = OpenOptions::new().append(true).create(true).open(&path).unwrap();
            let mut writer = BufWriter::new(file);
            loop {
                match incoming.recv().unwrap() {
                    PersisterMessage::Stop => { break; }
                    PersisterMessage::Write(items) => {
                        println!("Let's persist some stuff!");
                        for item in items {
                            let result = bincode::serialize(&item, bincode::Infinite).unwrap();
                            match writer.write_all(&result) {
                                Err(e) => {panic!("Can't persist! {:?}", e); }
                                Ok(_) => { }
                            }
                        }
                        writer.flush().unwrap();
                    }
                }
            }
        });
        Persister { outgoing, thread, loaded: vec![] }
    }

    pub fn load(&mut self, path:&str) {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(_) => {
                println!("Unable to load db: {}", path);
                return;
            }
        };
        let mut reader = BufReader::new(file);
        loop {
            let result:Result<RawChange, _> = bincode::deserialize_from(&mut reader, bincode::Infinite);
            match result {
                Ok(c) => {
                    println!("{:?}", c);
                    self.loaded.push(c);
                },
                Err(info) => {
                    println!("ran out {:?}", info);
                    break;
                }
            }
        }
    }

    pub fn send(&self, changes:Vec<RawChange>) {
       self.outgoing.send(PersisterMessage::Write(changes)).unwrap();
    }

    pub fn wait(self) {
        self.thread.join().unwrap();
    }

    pub fn get_channel(&self) -> Sender<PersisterMessage> {
        self.outgoing.clone()
    }

    pub fn get_commits(&mut self) -> Vec<RawChange> {
        mem::replace(&mut self.loaded, vec![])
    }

    pub fn close(&self) {
       self.outgoing.send(PersisterMessage::Stop).unwrap();
    }
}

//-------------------------------------------------------------------------
// Program Runner
//-------------------------------------------------------------------------

pub struct RunLoop {
    thread: JoinHandle<()>,
    outgoing: Sender<RunLoopMessage>,
}

impl RunLoop {
    pub fn wait(self) {
        self.thread.join().unwrap();
    }

    pub fn close(&self) {
        self.outgoing.send(RunLoopMessage::Stop).unwrap();
    }

    pub fn send(&self, msg: RunLoopMessage) {
        self.outgoing.send(msg).unwrap();
    }

    pub fn channel(&self) -> Sender<RunLoopMessage> {
        self.outgoing.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DebugMode {
    Compile
}

pub struct ProgramRunner {
    pub program: Program,
    pub name: String,
    paths: Vec<String>,
    initial_commits: Vec<RawChange>,
    persistence_channel: Option<Sender<PersisterMessage>>,
    debug_modes: HashSet<DebugMode>
}

impl ProgramRunner {
    pub fn new(name:&str) -> ProgramRunner {
        ProgramRunner {name: name.to_owned(), paths: vec![], program: Program::new(name), persistence_channel:None, initial_commits: vec![], debug_modes: HashSet::new() }
    }

    pub fn load(&mut self, path:&str) {
        self.paths.push(path.to_owned());
    }

    pub fn persist(&mut self, persister:&mut Persister) {
        self.persistence_channel = Some(persister.get_channel());
        self.initial_commits = persister.get_commits();
    }

    pub fn debug(&mut self, mode:DebugMode) {
        self.debug_modes.insert(mode);
    }

    pub fn run(self) -> RunLoop {
        let outgoing = self.program.outgoing.clone();
        let mut program = self.program;
        let paths = self.paths;
        let mut persistence_channel = self.persistence_channel;
        let initial_commits = self.initial_commits;
        let debug_compile = self.debug_modes.contains(&DebugMode::Compile);

        let thread = thread::Builder::new().name(program.name.to_owned()).spawn(move || {
            let mut blocks = vec![];
            let mut start_ns = time::precise_time_ns();
            for path in paths {
                blocks.extend(parse_file(&mut program.state.interner, &path, true, debug_compile));
            }
            let mut end_ns = time::precise_time_ns();
            println!("[{}] Compile took {:?}", &program.name, (end_ns - start_ns) as f64 / 1_000_000.0);

            start_ns = time::precise_time_ns();
            let mut txn = CodeTransaction::new();
            for initial in initial_commits {
                txn.input_change(initial.to_change(&mut program.state.interner));
            }
            txn.exec(&mut program, blocks, vec![]);
            end_ns = time::precise_time_ns();
            println!("[{}] Load took {:?}", &program.name, (end_ns - start_ns) as f64 / 1_000_000.0);

            let mut iter_pool = EstimateIterPool::new();
            println!("[{}] Starting run loop.", &program.name);

            'outer: loop {
                match program.incoming.recv() {
                    Ok(RunLoopMessage::Transaction(v)) => {
                        println!("[{}] Txn started", &program.name);
                        let start_ns = time::precise_time_ns();
                        let mut txn = Transaction::new(&mut iter_pool);
                        for cur in v {
                            // println!("  -> {:?}", cur);
                            txn.input_change(cur.to_change(&mut program.state.interner));
                        };
                        txn.exec(&mut program, &mut persistence_channel);
                        let end_ns = time::precise_time_ns();
                        let time = (end_ns - start_ns) as f64;
                        println!("[{}] Txn took {:?} - {:?} insts ({:?} ns) - {:?} inserts ({:?} ns)", &program.name, time / 1_000_000.0, txn.frame.counters.instructions, (time / (txn.frame.counters.instructions as f64)).floor(), txn.frame.counters.inserts, (time / (txn.frame.counters.inserts as f64)).floor());
                    }
                    Ok(RunLoopMessage::RemoteTransaction(v)) => {
                        let start_ns = time::precise_time_ns();
                        println!("[{}] Remote txn started", &program.name);
                        let mut txn = RemoteTransaction::new(&mut iter_pool);
                        for cur in v {
                            txn.input_change(cur.to_change(&mut program.state.interner));
                        };
                        txn.exec(&mut program, &mut persistence_channel);
                        let end_ns = time::precise_time_ns();
                        let time = (end_ns - start_ns) as f64;
                        println!("[{}] Txn took {:?} - {:?} insts ({:?} ns) - {:?} inserts ({:?} ns)", &program.name, time / 1_000_000.0, txn.frame.counters.instructions, (time / (txn.frame.counters.instructions as f64)).floor(), txn.frame.counters.inserts, (time / (txn.frame.counters.inserts as f64)).floor());
                    }
                    Ok(RunLoopMessage::Stop) => {
                        break 'outer;
                    }
                    Ok(RunLoopMessage::CodeTransaction(adds, removes)) => {
                        let start_ns = time::precise_time_ns();
                        let mut tx = CodeTransaction::new();
                        println!("[{}] Code Txn started", &program.name);
                        if adds.len() > 0 {
                            println!("  ADDS:");
                            for block in adds.iter() {
                                print_block_constraints(&block);
                            }
                        }
                        if removes.len() > 0 {
                            println!("  REMOVES:");
                            for block in removes.iter() {
                                println!("    - {:?}", block);
                            }
                        }
                        tx.exec(&mut program, adds, removes);
                        let end_ns = time::precise_time_ns();
                        let time = (end_ns - start_ns) as f64;
                        println!("[{}] Txn took {:?}", &program.name, time / 1_000_000.0);
                    }
                    Ok(RunLoopMessage::RemoteCodeTransaction(adds, removes)) => {
                        let start_ns = time::precise_time_ns();
                        let mut tx = CodeTransaction::new();
                        println!("[{}] Remote Code Txn started", &program.name);
                        let added_blocks:Vec<Block> = adds.iter().map(|b| b.intern(&mut program.state.interner)).collect();

                        if adds.len() > 0 {
                            println!("  ADDS:");
                            for block in added_blocks.iter() {
                                print_block_constraints(&block);
                            }
                        }
                        if removes.len() > 0 {
                            println!("  REMOVES:");
                            for block in removes.iter() {
                                println!("    - {:?}", block);
                            }
                        }

                        tx.exec(&mut program, added_blocks, removes);
                        let end_ns = time::precise_time_ns();
                        let time = (end_ns - start_ns) as f64;
                        println!("[{}] Txn took {:?}", &program.name, time / 1_000_000.0);

                    }
                    Err(_) => { break; }
                }
            }
            if let Some(channel) = persistence_channel {
                channel.send(PersisterMessage::Stop).unwrap();
            }
            println!("Closing run loop.");
        }).unwrap();

        RunLoop { thread, outgoing }
    }

}
