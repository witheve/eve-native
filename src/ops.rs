//-------------------------------------------------------------------------
// Ops
//-------------------------------------------------------------------------

extern crate time;

use indexes::{HashIndex, DistinctIter, HashIndexIter, WatchIndex, IntermediateIndex, MyHasher, RoundEntry, AggregateEntry, CollapsedChanges};
use compiler::{make_block, parse_file};
use hash::map::{DangerousKeys};
use std::collections::HashMap;
use std::mem::transmute;
use std::collections::hash_map::Entry;
use std::cmp;
use std::hash::{Hash, Hasher};
use std::cmp::Eq;
use std::collections::HashSet;
use std::iter::Iterator;
use std::fmt;
use watcher::{Watcher};
use std::sync::mpsc::{SyncSender, Receiver};
use std::sync::mpsc;
use serde::ser::{Serialize, Serializer};
use serde::de::{Deserialize, Deserializer, Visitor};
use std::error::Error;
use std::thread::{self, JoinHandle};

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

pub fn print_pipe(pipe: &Pipe, block_info:&BlockInfo, state:&mut RuntimeState) {
    let block_id = if let Instruction::StartBlock { block } = pipe[0] {
       block
    } else { unreachable!() };

    let block = &block_info.blocks[block_id];
    let name = "";
    if block.name != name { return; }

    state.debug = true;

    println!("\n\n-------------- Pipe ----------------\n");
    for inst in pipe.iter() {
        println!("   {:?}", inst);
    }
    println!("");
}

//-------------------------------------------------------------------------
// Change
//-------------------------------------------------------------------------

#[derive(Debug, Copy, Clone)]
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
    pub fn print(&self, prog:&Program) -> String {
        let a = prog.state.interner.get_value(self.a).print();
        let mut v = prog.state.interner.get_value(self.v).print();
        v = if v.contains("|") { format!("<{}>", self.v) } else { v };
        format!("Change (<{}>, {:?}, {})  {}:{}:{}", self.e, a, v, self.transaction, self.round, self.count)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

pub type Pipe = Vec<Instruction>;

#[derive(Debug)]
pub enum PipeShape {
    Scan(Interned, Interned, Interned),
    Intermediate(Interned),
}

#[derive(Debug)]
pub struct Block {
    pub name: String,
    pub constraints: Vec<Constraint>,
    pub pipes: Vec<Pipe>,
    pub shapes: Vec<Vec<PipeShape>>
}

impl Block {

    pub fn new(name:&str, constraints:Vec<Constraint>) -> Block {
       let mut me = Block { name:name.to_string(), constraints, pipes:vec![], shapes:vec![] };
       me.gen_pipes();
       me
    }

    pub fn gen_pipes(&mut self) {
        const NO_INPUTS_PIPE:u32 = 1000000;
        let mut moves:HashMap<u32, Vec<Instruction>> = HashMap::new();
        let mut scans = vec![NO_INPUTS_PIPE];
        let mut get_iters = vec![];
        let mut accepts = vec![];
        let mut get_rounds = vec![];
        let mut outputs = vec![];
        let mut project_constraints = vec![];
        let mut watch_constraints = vec![];
        let mut registers = 0;
        for (ix_usize, constraint) in self.constraints.iter().enumerate() {
            let ix = ix_usize as u32;
            match constraint {
                &Constraint::Scan {ref e, ref a, ref v, .. } => {
                    scans.push(ix);
                    get_iters.push(Instruction::GetIterator { bail: 0, constraint: ix, iterator: 0});
                    accepts.push(Instruction::Accept { bail: 0, constraint: ix, iterator: 0});
                    get_rounds.push(Instruction::GetRounds { bail: 0, constraint: ix });

                    let mut scan_moves = vec![];
                    if let &Field::Register(offset) = e {
                        scan_moves.push(Instruction::MoveInputField { from:0, to:offset as u32 });
                        registers = cmp::max(registers, offset + 1);
                    }
                    if let &Field::Register(offset) = a {
                        scan_moves.push(Instruction::MoveInputField { from:1, to:offset as u32 });
                        registers = cmp::max(registers, offset + 1);
                    }
                    if let &Field::Register(offset) = v {
                        scan_moves.push(Instruction::MoveInputField { from:2, to:offset as u32 });
                        registers = cmp::max(registers, offset + 1);
                    }
                    moves.insert(ix, scan_moves);
                },
                &Constraint::AntiScan {ref key, ..} => {
                    scans.push(ix);
                    let mut intermediate_moves = vec![];
                    for (field_ix, field) in key.iter().enumerate() {
                        if let &Field::Register(offset) = field {
                            intermediate_moves.push(Instruction::MoveIntermediateField { from:field_ix as u32, to:offset as u32 });
                            registers = cmp::max(registers, offset + 1);
                        } else if field_ix > 0 {
                            if let &Field::Value(value) = field {
                                intermediate_moves.push(Instruction::AcceptIntermediateField { from:field_ix as u32, value, bail: 0 });
                            }
                        }
                    }
                    moves.insert(ix, intermediate_moves);
                    get_rounds.push(Instruction::GetIntermediateRounds { bail: 0, constraint: ix });
                }
                &Constraint::IntermediateScan {ref full_key, ..} => {
                    scans.push(ix);
                    get_iters.push(Instruction::GetIterator { bail: 0, constraint: ix, iterator: 0});
                    accepts.push(Instruction::Accept { bail: 0, constraint: ix, iterator: 0});
                    get_rounds.push(Instruction::GetIntermediateRounds { bail: 0, constraint: ix });

                    let mut intermediate_moves = vec![];
                    for (field_ix, field) in full_key.iter().enumerate() {
                        if let &Field::Register(offset) = field {
                            intermediate_moves.push(Instruction::MoveIntermediateField { from:field_ix as u32, to:offset as u32 });
                            registers = cmp::max(registers, offset + 1);
                        } else if field_ix > 0 {
                            if let &Field::Value(value) = field {
                                intermediate_moves.push(Instruction::AcceptIntermediateField { from:field_ix as u32, value, bail: 0 });
                            }
                        }
                    }
                    moves.insert(ix, intermediate_moves);
                }
                &Constraint::Function {ref output, ..} => {
                    // @TODO: ensure that all inputs are accounted for
                    // count the registers in the functions
                    if let &Field::Register(offset) = output {
                        registers = cmp::max(registers, offset + 1);
                    }
                    get_iters.push(Instruction::GetIterator { bail: 0, constraint: ix, iterator: 0 });
                    accepts.push(Instruction::Accept { bail: 0, constraint: ix, iterator: 0 });
                },
                &Constraint::MultiFunction {ref outputs, ..} => {
                    // @TODO: ensure that all inputs are accounted for
                    // count the registers in the functions
                    for output in outputs.iter() {
                        if let &Field::Register(offset) = output {
                            registers = cmp::max(registers, offset + 1);
                        }
                    }
                    get_iters.push(Instruction::GetIterator { bail: 0, constraint: ix, iterator: 0 });
                    accepts.push(Instruction::Accept { bail: 0, constraint: ix, iterator: 0 });
                },
                &Constraint::Aggregate {..} => {
                    outputs.push(Instruction::InsertIntermediate { next: 1, constraint: ix });
                },
                &Constraint::Filter {..} => {
                    accepts.push(Instruction::Accept { bail: 0, constraint: ix, iterator: 0, });
                },
                &Constraint::Insert {ref commit, ..} => {
                    if *commit {
                        outputs.push(Instruction::Commit { next: 1, constraint: ix });
                    } else {
                        outputs.push(Instruction::Bind { next: 1, constraint: ix });
                    }
                },
                &Constraint::InsertIntermediate {..} => {
                    outputs.push(Instruction::InsertIntermediate { next: 1, constraint: ix });
                }
                &Constraint::Remove {..} => {
                    outputs.push(Instruction::Commit { next: 1, constraint: ix });
                },
                &Constraint::RemoveAttribute {..} => {
                    outputs.push(Instruction::Commit { next: 1, constraint: ix });
                },
                &Constraint::RemoveEntity {..} => {
                    outputs.push(Instruction::Commit { next: 1, constraint: ix });
                },
                &Constraint::Project {..} => {
                    project_constraints.push(constraint);
                }
                &Constraint::Watch {..} => {
                    watch_constraints.push((ix, constraint));
                }
            }
        };

        // println!("registers: {:?}", registers);

        let mut pipes = vec![];
        const PIPE_FINISHED:i32 = 1000000;
        let outputs_len = outputs.len();
        for scan_ix in &scans {
            let mut to_solve = registers;
            let mut pipe = vec![Instruction::StartBlock {block:0}];
            let mut seen = HashSet::new();
            if *scan_ix != NO_INPUTS_PIPE {
                for move_inst in &moves[scan_ix] {
                    match move_inst {
                        &Instruction::MoveInputField {to, ..} |
                        &Instruction::MoveIntermediateField {to, ..} => {
                            pipe.push(move_inst.clone());
                            if !seen.contains(&to) {
                                to_solve -= 1;
                                seen.insert(to);
                            }
                        },
                        &Instruction::AcceptIntermediateField {..} => {
                            let mut neue = move_inst.clone();
                            if let Instruction::AcceptIntermediateField { ref mut bail, .. } = neue {
                                *bail = PIPE_FINISHED;
                            }
                            pipe.push(neue);
                        }
                        _ => { panic!("invalid move instruction: {:?}", move_inst); }
                    }
                }
                for accept in accepts.iter() {
                    if let &Instruction::Accept { constraint, .. } = accept {
                        if constraint != *scan_ix {
                            let mut neue = accept.clone();
                            if let Instruction::Accept { ref mut bail, .. } = neue {
                                *bail = PIPE_FINISHED;
                            }
                            pipe.push(neue);
                        }
                    }
                }
            }
            let mut last_iter_next = 0;
            for ix in 0..to_solve {
                for get_iter in get_iters.iter() {
                    if let &Instruction::GetIterator { constraint, .. } = get_iter {
                        if constraint != *scan_ix {
                            last_iter_next -= 1;
                            let mut neue = get_iter.clone();
                            if let Instruction::GetIterator { ref mut bail, ref mut iterator, .. } = neue {
                                *iterator = ix as u32;
                                if ix == 0 {
                                    *bail = PIPE_FINISHED;
                                } else {
                                    *bail = last_iter_next;
                                }
                            }
                            pipe.push(neue);
                        }
                    }
                }

                last_iter_next -= 1;
                let iter_bail = if ix == 0 { PIPE_FINISHED } else { last_iter_next };
                pipe.push(Instruction::IteratorNext { bail: iter_bail, iterator: ix as u32, finished_mask: (2u64.pow(registers as u32) - 1) });
                last_iter_next = 0;

                for accept in accepts.iter() {
                    if let &Instruction::Accept { constraint, ..} = accept {
                        if constraint != *scan_ix {
                            last_iter_next -= 1;
                            let mut neue = accept.clone();
                            if let Instruction::Accept { ref mut bail, ref mut iterator, .. } = neue {
                                *iterator = (ix + 1) as u32;
                                *bail = last_iter_next;
                            }
                            pipe.push(neue);
                        }
                    }
                }
            }

            pipe.push(Instruction::ClearRounds);
            last_iter_next -= 1;

            if outputs_len > 0 || watch_constraints.len() > 0 {
                for inst in get_rounds.iter() {
                    match inst {
                        &Instruction::GetRounds { constraint, .. } |
                        &Instruction::GetIntermediateRounds { constraint, .. } => {
                            if constraint != *scan_ix {
                                last_iter_next -= 1;
                                let mut neue = inst.clone();
                                match neue {
                                    Instruction::GetRounds { ref mut bail, .. } |
                                    Instruction::GetIntermediateRounds { ref mut bail, .. } => {
                                        *bail = if to_solve > 0 {
                                            last_iter_next
                                        } else {
                                            PIPE_FINISHED
                                        }
                                    }
                                    _ => panic!()
                                }
                                pipe.push(neue);
                            }
                        }
                        _ => { panic!("Invalid instruction in rounds: {:?}", inst) }
                    }
                }
            }

            for (ix, output) in outputs.iter().enumerate() {
                last_iter_next -= 1;
                if ix < outputs_len - 1 {
                    pipe.push(output.clone());
                } else {
                    let mut neue = output.clone();
                    match neue {
                        Instruction::Bind {ref mut next, ..} |
                        Instruction::Commit { ref mut next, ..} |
                        Instruction::InsertIntermediate { ref mut next, ..} => {
                            *next = if to_solve > 0 {
                                last_iter_next
                            } else {
                                PIPE_FINISHED
                            }
                        }
                        _ => { panic!("Invalid output instruction"); }
                    };
                    pipe.push(neue);
                }
            }

            for constraint in project_constraints.iter() {
                if let &&Constraint::Project {ref registers} = constraint {
                    let registers_len = registers.len();
                    for (ix, reg) in registers.iter().enumerate() {
                        last_iter_next -= 1;
                        if ix < registers_len - 1 {
                            pipe.push(Instruction::Project { next:1, from: *reg as u32 });
                        } else {
                            let mut neue = Instruction::Project {next: 1, from: *reg as u32 };
                            if let Instruction::Project {ref mut next, ..} = neue {
                                *next = if to_solve > 0 {
                                    last_iter_next
                                } else {
                                    PIPE_FINISHED
                                }
                            }
                            pipe.push(neue);
                        }
                    }
                }
            }

            for (watch_ix, &(ix, constraint)) in watch_constraints.iter().enumerate() {
                if let &Constraint::Watch {ref name, ..} = constraint {
                    last_iter_next -= 1;
                    let next = if watch_ix as usize != watch_constraints.len() - 1 {
                        1
                    } else if to_solve > 0 {
                        last_iter_next
                    } else {
                        PIPE_FINISHED
                    };
                    pipe.push(Instruction::Watch {next, name:name.to_string(), constraint:ix as usize});
                }
            }

            pipes.push(pipe);
        };

        for pipe in pipes.iter() {
            self.pipes.push(pipe.clone());
            // println!("\npipe: [");
            // for inst in pipe {
            //     println!("  {:?}", inst);
            // }
            // println!("]");
        }

        let shapes_per_pipe = self.to_shapes(scans.iter().skip(1).map(|scan_ix| &self.constraints[*scan_ix as usize]).collect::<Vec<&Constraint>>());
        self.shapes.push(vec![]);
        for shape in shapes_per_pipe {
            self.shapes.push(shape);
        }
    }

    pub fn to_shapes(&self, scans: Vec<&Constraint>) -> Vec<Vec<PipeShape>> {
        let mut shapes = vec![];
        let tag = TAG_INTERNED_ID;
        let mut tag_mappings:HashMap<Field, Vec<Interned>> = HashMap::new();
        // find all the e -> tag mappings
        for scan in scans.iter() {
            if let &&Constraint::Scan {ref e, ref a, ref v, ..} = scan {
                let actual_a = if let &Field::Value(val) = a { val } else { 0 };
                let actual_v = if let &Field::Value(val) = v { val } else { 0 };
                if actual_a == tag && actual_v != 0 {
                    let mut tags = tag_mappings.entry(e.clone()).or_insert_with(|| vec![]);
                    tags.push(actual_v);
                }
            }
        }
        // go through each scan and create tag, a, v pairs where 0 is wildcard
        for scan in scans.iter() {
            let mut scan_shapes = vec![];
            match scan {
                &&Constraint::Scan {ref e, ref a, ref v, ..} => {
                    let actual_e = if let &Field::Value(val) = e { val } else { 0 };
                    let actual_a = if let &Field::Value(val) = a { val } else { 0 };
                    let actual_v = if let &Field::Value(val) = v { val } else { 0 };
                    if actual_a == tag {
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
    fields: Vec<Interned>,
    solved_fields: u64,
    solving_for:u64,
    solved_stack: Vec<u64>,
}

impl Row {
    pub fn new(size:usize) -> Row {
        Row { fields: vec![0; size], solved_fields: 0, solving_for: 0, solved_stack:vec![0; size] }
    }

    pub fn put_solved(&mut self, ix:u32) {
        self.solved_stack[ix as usize] = self.solved_fields;
    }

    pub fn clear_solved(&mut self, ix:u32) {
        self.solved_stack[ix as usize] = 0;
    }

    pub fn get_solved(&self, ix:i32) -> u64 {
        if ix >= 0 {
            self.solved_stack[ix as usize]
        } else {
            0
        }
    }

    pub fn check(&self, field_index:u32, value:Interned) -> bool {
        let cur = self.fields[field_index as usize];
        cur == 0 || cur == value
    }

    pub fn set(&mut self, field_index:u32, value:Interned) {
        self.fields[field_index as usize] = value;
        self.solving_for = set_bit(0, field_index);
        self.solved_fields = set_bit(self.solved_fields, field_index);
    }

    pub fn set_multi(&mut self, field_index:u32, value:Interned) {
        self.fields[field_index as usize] = value;
        self.solving_for = set_bit(self.solving_for, field_index);
        self.solved_fields = set_bit(self.solved_fields, field_index);
    }

    pub fn clear_solving_for(&mut self) {
        self.solving_for = 0;
    }

    pub fn clear(&mut self, field_index:u32) {
        self.fields[field_index as usize] = 0;
        self.solving_for = 0;
        self.solved_fields = clear_bit(self.solved_fields, field_index);
    }

    pub fn reset(&mut self) {
        let size = 64;
        self.solved_fields = 0;
        self.solving_for = 0;
        for field_index in 0..size {
            self.fields[field_index as usize] = 0;
        }
    }
}

//-------------------------------------------------------------------------
// Estimate Iter
//-------------------------------------------------------------------------

pub struct EstimateIterPool {
    available: Vec<EstimateIter>,
    available_funcs: Vec<EstimateIter>,
    available_multi_funcs: Vec<EstimateIter>,
    available_intermedaites: Vec<EstimateIter>,
    iters: Vec<Option<EstimateIter>>,
}

impl EstimateIterPool {
    pub fn new() -> EstimateIterPool {
        EstimateIterPool { available: vec![], available_funcs: vec![], available_multi_funcs: vec![], available_intermedaites: vec![], iters:vec![None; 64] }
    }

    pub fn release(&mut self, mut estimate_iter:EstimateIter) {
        match estimate_iter {
            EstimateIter::Scan {ref mut estimate, ref mut iter, ref mut output, ref mut constraint} => {
                *estimate = 0;
                *iter = HashIndexIter::Empty;
                *output = 0;
                *constraint = 0;
            },
            EstimateIter::Function { ref mut estimate, ref mut result, ref mut output, ref mut returned, ref mut constraint } => {
                *estimate = 0;
                *result = 0;
                *output = 0;
                *constraint = 0;
                *returned = false;
            },
            EstimateIter::MultiFunction { ref mut estimate, ref mut results, ref mut outputs, ref mut ix, ref mut constraint } => {
                *estimate = 0;
                *results = None;
                *outputs = None;
                *constraint = 0;
                *ix = 0;
            },
            EstimateIter::Intermediate {ref mut estimate, ref mut iter, ref mut output, ref mut constraint} => {
                *estimate = 0;
                *iter = None;
                *output = None;
                *constraint = 0;
            },
            EstimateIter::PassThrough => {}
        }
        match estimate_iter {
            EstimateIter::Scan {..} => {
                self.available.push(estimate_iter);
            },
            EstimateIter::Function {..} => {
                self.available_funcs.push(estimate_iter);
            },
            EstimateIter::MultiFunction {..} => {
                self.available_multi_funcs.push(estimate_iter);
            },
            EstimateIter::Intermediate {..} => {
                self.available_intermedaites.push(estimate_iter);
            },
            EstimateIter::PassThrough => {}
        }
    }

    pub fn get(&mut self) -> EstimateIter {
        match self.available.pop() {
            Some(iter) => iter,
            None => EstimateIter::Scan { estimate:0, iter:HashIndexIter::Empty, output:0, constraint: 0 },
        }
    }

    pub fn get_func(&mut self) -> EstimateIter {
        match self.available_funcs.pop() {
            Some(iter) => iter,
            None => EstimateIter::Function { estimate:0, result:0, output:0, returned: false, constraint:0 },
        }
    }

    pub fn get_multi_func(&mut self) -> EstimateIter {
        match self.available_funcs.pop() {
            Some(iter) => iter,
            None => EstimateIter::MultiFunction { estimate:0, results:None, outputs:None, ix: 0, constraint:0 },
        }
    }

    pub fn get_intermediate(&mut self) -> EstimateIter {
        match self.available_intermedaites.pop() {
            Some(iter) => iter,
            None => EstimateIter::Intermediate { estimate:0, iter:None, output:None, constraint:0 },
        }
    }

    pub fn check_iter(&mut self, iter_ix:u32, iter: EstimateIter) {
        // @FIXME: it seems like there should be a better way to pull a value
        // out of a vector and potentially replace it
        let ix = iter_ix as usize;
        let mut cur = self.iters[ix].take();
        let cur_estimate = if let Some(ref cur_iter) = cur {
            cur_iter.estimate()
        } else {
            1_000_000_000
        };
        // println!("{:?}  estimate {:?} less than? {:?}", iter_ix, iter.estimate(), cur_estimate);

        let neue = match cur {
            None => {
                Some(iter)
            },
            Some(_) if cur_estimate > iter.estimate() => {
                self.release(cur.take().unwrap());
                Some(iter)
            },
            old => old,
        };
        match neue {
            Some(_) => { self.iters[ix] = neue; },
            None => {},
        }
    }

}


#[derive(Clone)]
pub enum EstimateIter {
    PassThrough,
    Scan {estimate: u32, iter: HashIndexIter, output: u32, constraint: u32},
    Function {estimate: u32, output: u32, result: Interned, returned: bool, constraint: u32},
    MultiFunction {estimate: u32, outputs: Option<Vec<u32>>, ix:usize, results: Option<Vec<Vec<Interned>>>, constraint: u32},
    Intermediate {estimate: u32, iter: Option<DangerousKeys<Vec<Interned>, RoundEntry>>, output: Option<Vec<u32>>, constraint: u32},
}

impl EstimateIter {
    pub fn estimate(&self) -> u32 {
        match self {
            &EstimateIter::Scan {ref estimate, .. } |
            &EstimateIter::Function {ref estimate, .. } |
            &EstimateIter::Intermediate {ref estimate, .. } |
            &EstimateIter::MultiFunction {ref estimate, .. } => {
                *estimate
            },
            &EstimateIter::PassThrough => 1
        }
    }

    pub fn next(&mut self, row:&mut Row, iterator: u32) -> bool {
        match self {
            &mut EstimateIter::Scan {ref mut iter, ref output, .. } => {
                if let Some(v) = iter.next() {
                    row.set(*output, v);
                    true
                } else {
                    false
                }
            },
            &mut EstimateIter::Function { result, ref output, ref mut returned, .. } => {
                if !*returned && result > 0 {
                    *returned = true;
                    row.set(*output, result);
                    true
                } else {
                    false
                }
            },
            &mut EstimateIter::MultiFunction {ref results, ref mut ix, outputs: Some(ref outputs), .. } => {
                if let &Some(ref rows) = results {
                    loop {
                        if *ix < rows.len() {
                            let prev_solved = row.get_solved(iterator as i32 - 1);
                            let mut valid = true;
                            row.clear_solving_for();
                            for (out, v) in outputs.iter().zip(rows[*ix].iter()) {
                                if check_bit(prev_solved, *out) {
                                    if !row.check(*out, *v) {
                                        valid = false;
                                        break;
                                    }
                                } else {
                                    row.set_multi(*out, *v);
                                }
                            }
                            *ix += 1;
                            if valid {
                                return true;
                            }
                        } else {
                            return false
                        }
                    }
                } else {
                    false
                }
            },
            &mut EstimateIter::Intermediate {ref mut iter, output: Some(ref outputs), .. } => {
                if let &mut Some(ref mut keys) = iter {
                    loop {
                        if let Some(key) = keys.next() {
                            let prev_solved = row.get_solved(iterator as i32 - 1);
                            let mut valid = true;
                            row.clear_solving_for();
                            for (out, v) in outputs.iter().zip(key) {
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
                        } else {
                            return false;
                        }
                    }
                } else {
                    false
                }
            },
            &mut EstimateIter::PassThrough => { false }
            _ => panic!("Implement me"),
        }
    }

    pub fn clear(&self, row:&mut Row, iterator: u32) {
        match self {
            &EstimateIter::Scan {ref output, .. } => {
                row.clear(*output);
            },
            &EstimateIter::Function { ref output, .. } => {
                row.clear(*output);
            },
            &EstimateIter::MultiFunction { outputs: Some(ref outputs), .. } => {
                let prev_solved = row.get_solved(iterator as i32 - 1);
                for output in outputs.iter() {
                    if !check_bit(prev_solved, *output) {
                        row.clear(*output);
                    }
                }
            },
            &EstimateIter::Intermediate { output: Some(ref outputs), .. } => {
                let prev_solved = row.get_solved(iterator as i32 - 1);
                for output in outputs.iter() {
                    if !check_bit(prev_solved, *output) {
                        row.clear(*output);
                    }
                }
            },
            &EstimateIter::PassThrough => { }
            _ => panic!("Implement me"),
        }
    }
}

//-------------------------------------------------------------------------
// Frame
//-------------------------------------------------------------------------

pub struct Counters {
    total_ns: u64,
    instructions: u64,
    iter_next: u64,
    accept: u64,
    accept_bail: u64,
    accept_ns: u64,
    considered: u64,
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
    input: Option<Change>,
    intermediate: Option<IntermediateChange>,
    row: Row,
    block_ix: usize,
    results: Vec<Interned>,
    #[allow(dead_code)]
    counters: Counters,
}

impl Frame {
    pub fn new() -> Frame {
        Frame {row: Row::new(64), block_ix:0, input: None, intermediate: None, results: vec![], counters: Counters {iter_next: 0, accept: 0, accept_bail: 0, instructions: 0, accept_ns: 0, total_ns: 0, considered: 0}}
    }

    pub fn get_register(&self, register:u32) -> Interned {
        self.row.fields[register as usize]
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
// Instruction
//-------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum Instruction {
    StartBlock { block: usize },
    GetIterator {iterator: u32, bail: i32, constraint: u32},
    IteratorNext {iterator: u32, bail: i32, finished_mask: u64},
    Accept {bail: i32, constraint:u32, iterator:u32},
    MoveInputField { from:u32, to:u32, },
    MoveIntermediateField { from:u32, to:u32, },
    AcceptIntermediateField { from:u32, value:Interned, bail:i32 },
    ClearRounds,
    GetRounds {bail: i32, constraint: u32},
    GetIntermediateRounds {bail: i32, constraint: u32},
    Bind {next: i32, constraint:u32},
    Commit {next: i32, constraint:u32},
    InsertIntermediate {next: i32, constraint:u32},
    Project {next: i32, from:u32},
    Watch { next:i32, name:String, constraint:usize}
}

#[inline(never)]
pub fn start_block(_: &mut RuntimeState, frame: &mut Frame, block:usize) -> i32 {
    // println!("STARTING! {:?}", block);
    frame.block_ix = block;
    1
}

#[inline(never)]
pub fn move_input_field(_: &mut RuntimeState, frame: &mut Frame, from:u32, to:u32) -> i32 {
    // println!("STARTING! {:?}", block);
    if let Some(change) = frame.input {
        match from {
            0 => { frame.row.set_multi(to, change.e); }
            1 => { frame.row.set_multi(to, change.a); }
            2 => { frame.row.set_multi(to, change.v); }
            _ => { panic!("Unknown move: {:?}", from); },
        }
    }
    1
}

#[inline(never)]
pub fn move_intermediate_field(_: &mut RuntimeState, frame: &mut Frame, from:u32, to:u32) -> i32 {
    // println!("STARTING! {:?}", block);
    if let Some(ref intermediate) = frame.intermediate {
        frame.row.set_multi(to, intermediate.key[from as usize]);
        1
    } else {
        panic!("move_input_field without an intermediate in the frame?");
    }
}

#[inline(never)]
pub fn accept_intermediate_field(_: &mut RuntimeState, frame: &mut Frame, from:u32, value:Interned, bail:i32) -> i32 {
    // println!("STARTING! {:?}", block);
    if let Some(ref intermediate) = frame.intermediate {
        if intermediate.key[from as usize] == value { 1 } else { bail }
    } else {
        panic!("move_input_field without an intermediate in the frame?");
    }
}

#[inline(never)]
pub fn get_iterator(program: &mut RuntimeState, block_info: &BlockInfo, iter_pool:&mut EstimateIterPool, frame: &mut Frame, iter_ix:u32, cur_constraint:u32, bail:i32) -> i32 {
    let cur = &block_info.blocks[frame.block_ix].constraints[cur_constraint as usize];
    match cur {
        &Constraint::Scan {ref e, ref a, ref v, ref register_mask} => {
            // if we have already solved all of this scan's vars, we just move on
            if check_bits(frame.row.solved_fields, *register_mask) {
                return 1;
            }

            let resolved_e = frame.resolve(e);
            let resolved_a = frame.resolve(a);
            let resolved_v = frame.resolve(v);

            // println!("Getting proposal for {:?} {:?} {:?}", resolved_e, resolved_a, resolved_v);
            let mut iter = iter_pool.get();
            program.index.propose(&mut iter, resolved_e, resolved_a, resolved_v);
            match iter {
                EstimateIter::Scan {ref mut output, ref mut constraint, ..} => {
                    *constraint = cur_constraint;
                    *output = match (*output, e, a, v) {
                        (0, &Field::Register(reg), _, _) => reg as u32,
                        (1, _, &Field::Register(reg), _) => reg as u32,
                        (2, _, _, &Field::Register(reg)) => reg as u32,
                        _ => panic!("bad scan output {:?} {:?} {:?} {:?}", output,e,a,v),
                    };
                }
                _ => panic!("Implement me"),
            }

            // if program.debug { println!("get iter: {:?} -> estimate {:?}", cur_constraint, iter.estimate()); }
            iter_pool.check_iter(iter_ix, iter);
            1
        },
        &Constraint::Function {ref func, ref output, ref params, param_mask, output_mask, ..} => {
            let solved = frame.row.solved_fields;
            let jump = if check_bits(solved, param_mask) && !check_bits(solved, output_mask) {
                let result = {
                    let mut resolved = vec![];
                    for param in params {
                        resolved.push(program.interner.get_value(frame.resolve(param)));
                    }
                    func(resolved)
                };
                let mut iter = iter_pool.get_func();
                match result {
                    Some(v) => {
                        let id = program.interner.internable_to_id(v);
                        let reg = if let &Field::Register(reg) = output {
                            reg as u32
                        } else {
                            panic!("Function output is not a register");
                        };
                        if let EstimateIter::Function {ref mut estimate, ref mut output, ref mut result, ..} = iter {
                            *estimate = 1;
                            *result = id;
                            *output = reg;
                        }
                        iter_pool.check_iter(iter_ix, iter);
                        1
                    }
                    _ => bail,
                }
            } else {
                1
            };
            // if program.debug { println!("get func iter: {:?} -> jump: {:?}", cur_constraint, jump); }
            jump
        },
        &Constraint::MultiFunction {ref func, outputs:ref output_fields, ref params, param_mask, output_mask, ..} => {
            let solved = frame.row.solved_fields;
            if check_bits(solved, param_mask) && !check_bits(solved, output_mask) {
                let result = {
                    let mut resolved = vec![];
                    for param in params {
                        resolved.push(program.interner.get_value(frame.resolve(param)));
                    }
                    func(resolved)
                };
                let mut iter = iter_pool.get_multi_func();
                match result {
                    Some(mut result_values) => {
                        if let EstimateIter::MultiFunction {ref mut estimate, ref mut outputs, ref mut results, ..} = iter {
                            *estimate = 1;
                            *results = Some(result_values.drain(..).map(|mut row| {
                                row.drain(..).map(|field| program.interner.internable_to_id(field)).collect()
                            }).collect());
                            // @TODO this could be precomputed
                            *outputs = Some(output_fields.iter().map(|x| {
                                if let &Field::Register(reg) = x {
                                    reg as u32
                                } else {
                                    panic!("Non-register multi-function output")
                                }
                            }).collect());
                        }
                        iter_pool.check_iter(iter_ix, iter);
                        1
                    }
                    _ => bail,
                }
            } else {
                1
            }
            // println!("get function iterator {:?}", cur);
        },
        &Constraint::IntermediateScan {ref key, ref value, ref register_mask, ref output_mask, ..} => {
            // if we have already solved all of this scan's outputs or we don't have all of our
            // inputs, we just move on
            if !check_bits(frame.row.solved_fields, *register_mask) ||
               check_bits(frame.row.solved_fields, *output_mask) {
                return 1;
            }

            let resolved = key.iter().map(|param| frame.resolve(param)).collect();

            // println!("Getting proposal for {:?} {:?} {:?}", resolved_e, resolved_a, resolved_v);
            let mut iter = iter_pool.get_intermediate();
            program.intermediates.propose(&mut iter, resolved);
            match iter {
                EstimateIter::Intermediate {ref mut output, ref mut constraint, ..} => {
                    *constraint = cur_constraint;
                    // @TODO this could be precomputed
                    *output = Some(value.iter().map(|x| {
                        if let &Field::Register(reg) = x {
                            reg as u32
                        } else {
                            panic!("Non-register intermediate scan output")
                        }
                    }).collect());
                }
                _ => panic!("Non-intermediate iterator for IntermediateScan"),
            }

            // println!("get iter: {:?}", cur_constraint);
            iter_pool.check_iter(iter_ix, iter);
            1
        },
        _ => { 1 }
    }
}

#[inline(never)]
pub fn iterator_next(_: &mut RuntimeState, iter_pool:&mut EstimateIterPool, frame: &mut Frame, iterator:u32, bail:i32, finished_mask:u64) -> i32 {
    let mut passthrough = false;
    let go = {
        let mut iter = iter_pool.iters[iterator as usize].as_mut();
        // println!("Iter Next: {:?}", iter);
        match iter {
            Some(ref mut cur) => {
                match cur.next(&mut frame.row, iterator) {
                    false => {
                        if cur.estimate() != 0 {
                            frame.row.clear_solved(iterator);
                            cur.clear(&mut frame.row, iterator);
                        }
                        bail
                    },
                    true => {
                        // frame.counters.iter_next += 1;
                        frame.row.put_solved(iterator);
                        1
                    },
                }
            },
            None => {
                if frame.row.get_solved(iterator as i32 - 1) == finished_mask {
                    // if we were solved when we came into here, and there were no
                    // iterators set, that means we've completely solved for all the variables
                    // and we just need to passthrough to the end, by setting the current iter
                    // to the PassThrough iterator, when we come back into this instruction,
                    // we'll go through the other branch and bail out appropriately. Effectively
                    // setting the passthrough iterator allows you to proceed through the pipe
                    // exactly once without needing to iterate normally. This is necessary because
                    // some instructions can solve for multiple registers at once, but it's not
                    // guaranteed that they'll run before some other provider that might do each
                    // register one by one, so the number of iterations necessary may vary.
                    passthrough = true;
                    1
                } else {
                    bail
                }
            },
        }
    };
    if passthrough {
        iter_pool.iters[iterator as usize] = Some(EstimateIter::PassThrough);
    } else if go == bail {
        let old = iter_pool.iters[iterator as usize].take();
        if let Some(cur) = old {
            let est = cur.estimate();
            frame.counters.considered += est as u64;
            iter_pool.release(cur);
        }
    }
    // println!("Row: {:?}", &frame.row.fields[0..3]);
    go
}

#[inline(never)]
pub fn accept(program: &mut RuntimeState, block_info:&BlockInfo, iter_pool:&mut EstimateIterPool, frame: &mut Frame, cur_constraint:u32, cur_iterator:u32, bail:i32) -> i32 {
    frame.counters.accept += 1;
    let cur = &block_info.blocks[frame.block_ix].constraints[cur_constraint as usize];
    if cur_iterator > 0 {
        let iter = &iter_pool.iters[(cur_iterator - 1) as usize];
        match iter {
            &Some(EstimateIter::Scan { constraint, .. }) => {
                if constraint == cur_constraint { return 1; }
            }
            &Some(EstimateIter::PassThrough) => { return 1; }
            _ => {}
        }
    }
    match cur {
        &Constraint::Scan {ref e, ref a, ref v, ref register_mask} => {
            // if we aren't solving for something this scan cares about, then we
            // automatically accept it.
            if !has_any_bits(*register_mask, frame.row.solving_for) {
                // println!("auto accept {:?} {:?}", cur, frame.row.solving_for);
               return 1;
            }
            let resolved_e = frame.resolve(e);
            let resolved_a = frame.resolve(a);
            let resolved_v = frame.resolve(v);
            let checked = program.index.check(resolved_e, resolved_a, resolved_v);
            // if program.debug { println!("scan accept {:?} {:?}", cur_constraint, checked); }
            if checked { 1 } else { bail }
        },
        &Constraint::Function {ref func, ref output, ref params, ref param_mask, ref output_mask, .. } => {
            let solved = frame.row.solved_fields;
            if !check_bits(solved, *param_mask) || !has_any_bits(frame.row.solving_for, *output_mask) {
                return 1
            }

            let result = {
                let mut resolved = vec![];
                for param in params {
                    resolved.push(program.interner.get_value(frame.resolve(param)));
                }
                func(resolved)
            };
            match result {
                Some(v) => {
                    let id = program.interner.internable_to_id(v);
                    if id == frame.resolve(output) { 1 } else { bail }
                }
                _ => bail,
            }
        },
        &Constraint::Filter {ref left, ref right, ref func, ref param_mask, .. } => {
            if !has_any_bits(*param_mask, frame.row.solving_for) {
               return 1;
            }
            if check_bits(frame.row.solved_fields, *param_mask) {
                let resolved_left = program.interner.get_value(frame.resolve(left));
                let resolved_right = program.interner.get_value(frame.resolve(right));
                if func(resolved_left, resolved_right) {
                    1
                } else {
                    bail
                }
            } else {
                1
            }
        },
        &Constraint::IntermediateScan {ref key, ref value, ref register_mask, ref output_mask, ..} => {
            // if we haven't solved all our inputs and outputs, just skip us
            if !check_bits(frame.row.solved_fields, *register_mask) ||
               !check_bits(frame.row.solved_fields, *output_mask) {
                return 1;
            }

            let resolved = key.iter().map(|param| frame.resolve(param)).collect();
            let resolved_value = value.iter().map(|param| frame.resolve(param)).collect();

            if program.intermediates.check(&resolved, &resolved_value) { 1 } else { bail }
        },
        _ => { 1 }
    }
}

#[inline(never)]
pub fn clear_rounds(program: &mut RuntimeState, frame: &mut Frame) -> i32 {
    program.rounds.clear_output_rounds();
    if let Some(ref change) = frame.input {
        program.rounds.output_rounds.push((change.round, change.count));
    } else if let Some(ref change) = frame.intermediate {
        let count = if change.negate { change.count * -1 } else { change.count };
        program.rounds.output_rounds.push((change.round, count));
    }
    1
}

#[inline(never)]
pub fn get_rounds(program: &mut RuntimeState, block_info:&BlockInfo, frame: &mut Frame, constraint:u32, bail:i32) -> i32 {
    // println!("get rounds!");
    let cur = &block_info.blocks[frame.block_ix].constraints[constraint as usize];
    match cur {
        &Constraint::Scan {ref e, ref a, ref v, .. } => {
            let resolved_e = frame.resolve(e);
            let resolved_a = frame.resolve(a);
            let resolved_v = frame.resolve(v);
            // println!("getting rounds for {:?} {:?} {:?}", e, a, v);
            program.rounds.compute_output_rounds(program.index.distinct_iter(resolved_e, resolved_a, resolved_v));
            // if program.debug { println!("get rounds: ({}, {}, {}) -> {:?}", resolved_e, resolved_a, resolved_v, program.rounds.get_output_rounds()); }
            if program.rounds.get_output_rounds().len() > 0 {
                1
            } else {
                bail
            }
        },
        _ => { panic!("Get rounds on non-scan") }
    }
}

#[inline(never)]
pub fn get_intermediate_rounds(program: &mut RuntimeState, block_info:&BlockInfo, frame: &mut Frame, constraint:u32, bail:i32) -> i32 {
    // println!("get rounds!");
    let cur = &block_info.blocks[frame.block_ix].constraints[constraint as usize];
    match cur {
        &Constraint::AntiScan {ref key, .. } => {
            let resolved:Vec<Interned> = key.iter().map(|v| frame.resolve(v)).collect();
            program.rounds.compute_anti_output_rounds(program.intermediates.distinct_iter(&resolved, &vec![]));
        },
        &Constraint::IntermediateScan {ref key, ref value, .. } => {
            let resolved:Vec<Interned> = key.iter().map(|v| frame.resolve(v)).collect();
            let resolved_value:Vec<Interned> = value.iter().map(|v| frame.resolve(v)).collect();
            program.rounds.compute_output_rounds(program.intermediates.distinct_iter(&resolved, &resolved_value));
        },
        _ => { panic!("Get rounds on non-scan") }
    };
    if program.rounds.get_output_rounds().len() > 0 {
        1
    } else {
        bail
    }
}

#[inline(never)]
pub fn bind(program: &mut RuntimeState, block_info:&BlockInfo, frame: &mut Frame, constraint:u32, next:i32) -> i32 {
    let cur = &block_info.blocks[frame.block_ix].constraints[constraint as usize];
    match cur {
        &Constraint::Insert {ref e, ref a, ref v, ..} => {
            let c = Change { e: frame.resolve(e), a: frame.resolve(a), v:frame.resolve(v), n: 0, round:0, transaction: 0, count:0, };
            let ref mut rounds = program.rounds;
            // println!("rounds {:?}", rounds.output_rounds);
            // @FIXME this clone is completely unnecessary, but borrows are a bit sad here
            for &(round, count) in rounds.get_output_rounds().clone().iter() {
                let output = &c.with_round_count(round + 1, count);
                program.index.distinct(output, rounds);
            }
        },
        _ => {}
    };
    next
}

#[inline(never)]
pub fn commit(program: &mut RuntimeState, block_info:&BlockInfo, frame: &mut Frame, constraint:u32, next:i32) -> i32 {
    let cur = &block_info.blocks[frame.block_ix].constraints[constraint as usize];
    match cur {
        &Constraint::Insert {ref e, ref a, ref v, ..} => {
            let n = (frame.block_ix as u32) * 10000 + constraint;
            let c = Change { e: frame.resolve(e), a: frame.resolve(a), v:frame.resolve(v), n, round:0, transaction: 0, count:0, };
            let ref mut rounds = program.rounds;
            // @FIXME this clone is completely unnecessary, but borrows are a bit sad here
            for &(_, count) in rounds.get_output_rounds().clone().iter() {
                let output = c.with_round_count(0, count);
                // if program.debug { println!("     -> Commit {:?}", output); }
                rounds.commit(output, ChangeType::Insert)
            }
        },
        &Constraint::Remove {ref e, ref a, ref v } => {
            let n = (frame.block_ix as u32) * 10000 + constraint;
            let c = Change { e: frame.resolve(e), a: frame.resolve(a), v:frame.resolve(v), n, round:0, transaction: 0, count:0, };
            let ref mut rounds = program.rounds;
            // @FIXME this clone is completely unnecessary, but borrows are a bit sad here
            for &(_, count) in rounds.get_output_rounds().clone().iter() {
                let output = c.with_round_count(0, count * -1);
                rounds.commit(output, ChangeType::Remove)
            }
        },
        &Constraint::RemoveAttribute {ref e, ref a } => {
            let n = (frame.block_ix as u32) * 10000 + constraint;
            let c = Change { e: frame.resolve(e), a: frame.resolve(a), v:0, n, round:0, transaction: 0, count:0, };
            let ref mut rounds = program.rounds;
            // @FIXME this clone is completely unnecessary, but borrows are a bit sad here
            for &(_, count) in rounds.get_output_rounds().clone().iter() {
                let output = c.with_round_count(0, count * -1);
                rounds.commit(output, ChangeType::Remove)
            }
        },
        &Constraint::RemoveEntity {ref e } => {
            let n = (frame.block_ix as u32) * 10000 + constraint;
            let c = Change { e: frame.resolve(e), a: 0, v:0, n, round:0, transaction: 0, count:0, };
            let ref mut rounds = program.rounds;
            // @FIXME this clone is completely unnecessary, but borrows are a bit sad here
            for &(_, count) in rounds.get_output_rounds().clone().iter() {
                let output = c.with_round_count(0, count * -1);
                rounds.commit(output, ChangeType::Remove)
            }
        },
        _ => {}
    };
    next
}

#[inline(never)]
pub fn insert_intermediate(program: &mut RuntimeState, block_info:&BlockInfo, frame: &mut Frame, constraint:u32, next:i32) -> i32 {
    let cur = &block_info.blocks[frame.block_ix].constraints[constraint as usize];
    match cur {
        &Constraint::InsertIntermediate {ref key, ref value, negate} => {
            let resolved:Vec<Interned> = key.iter().map(|v| frame.resolve(v)).collect();
            let resolved_value:Vec<Interned> = value.iter().map(|v| frame.resolve(v)).collect();
            let mut full_key = resolved.clone();
            full_key.extend(resolved_value.iter());
            for &(round, count) in program.rounds.get_output_rounds().iter() {
                program.intermediates.buffer(full_key.clone(), resolved.clone(), resolved_value.clone(), round, count, negate);
            }
        },
        &Constraint::Aggregate {ref group, ref params, ref output_key, ref add, ref remove, ..} => {
            let ref mut interner = program.interner;
            let resolved_group:Vec<Interned> = group.iter().map(|v| frame.resolve(v)).collect();
            let resolved_params:Vec<Internable> = { params.iter().map(|v| interner.get_value(frame.resolve(v)).clone()).collect() };
            let resolved_output:Vec<Interned> = output_key.iter().map(|v| frame.resolve(v)).collect();
            for &(round, count) in program.rounds.get_output_rounds().iter() {
                // @TODO: do aggregates need to be buffered as well?
                let action = if count < 0 { remove } else { add };
                program.intermediates.aggregate(interner, resolved_group.clone(), resolved_params.clone(), round, *action, resolved_output.clone());
            }
        },
        _ => {}
    };
    next
}


#[inline(never)]
pub fn project(_: &mut RuntimeState, frame: &mut Frame, from:u32, next:i32) -> i32 {
    let value = frame.get_register(from);
    frame.results.push(value);
    next
}

#[inline(never)]
pub fn watch(program: &mut RuntimeState, block_info:&BlockInfo, frame: &mut Frame, name:&str, next:i32, constraint:usize) -> i32 {
    let cur = &block_info.blocks[frame.block_ix].constraints[constraint as usize];
    match cur {
        &Constraint::Watch { ref registers, ..} => {
            let resolved = registers.iter().map(|x| frame.resolve(x)).collect();
            let mut total = 0;
            for &(_, count) in program.rounds.get_output_rounds().iter() {
                total += count;
            }
            program.watch(name, resolved, total);
        },
        _ => unreachable!()
    }
    next
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

//-------------------------------------------------------------------------
// Interner
//-------------------------------------------------------------------------

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Internable {
    String(String),
    Number(u32),
    Null,
}

impl Internable {
    pub fn to_number(intern: &Internable) -> f32 {
        match intern {
            &Internable::Number(num) => unsafe { transmute::<u32, f32>(num) },
            _ => { panic!("to_number on non-number") }
        }
    }

    pub fn from_number(num: f32) -> Internable {
        let value = unsafe { transmute::<f32, u32>(num) };
        Internable::Number(value)
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
}

impl Serialize for Internable {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        match self {
            &Internable::String(ref s) => serializer.serialize_str(s),
            &Internable::Number(_) => serializer.serialize_f32(Internable::to_number(self)),
            _ => serializer.serialize_unit(),
        }
    }
}

impl<'de> Deserialize<'de> for Internable {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        struct InternableVisitor;

        impl<'de> Visitor<'de> for InternableVisitor {
            type Value = Internable;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("Internable")
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
                where E: Error
            {
                Ok(Internable::from_number(v as f32))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
                where E: Error
            {
                Ok(Internable::from_number(v as f32))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
                where E: Error
            {
                Ok(Internable::from_number(v as f32))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where E: Error
            {
                Ok(Internable::String(v.to_owned()))
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
}

//-------------------------------------------------------------------------
// Constraint
//-------------------------------------------------------------------------

type FilterFunction = fn(&Internable, &Internable) -> bool;
type Function = fn(Vec<&Internable>) -> Option<Internable>;
type MultiFunction = fn(Vec<&Internable>) -> Option<Vec<Vec<Internable>>>;
pub type AggregateFunction = fn(&mut AggregateEntry, Vec<Internable>);

pub enum Constraint {
    Scan {e: Field, a: Field, v: Field, register_mask: u64},
    AntiScan {key: Vec<Field>, register_mask: u64},
    IntermediateScan {full_key:Vec<Field>, key: Vec<Field>, value: Vec<Field>, register_mask: u64, output_mask: u64},
    Function {op: String, output: Field, func: Function, params: Vec<Field>, param_mask: u64, output_mask: u64},
    MultiFunction {op: String, outputs: Vec<Field>, func: MultiFunction, params: Vec<Field>, param_mask: u64, output_mask: u64},
    Aggregate {op: String, output: Field, add: AggregateFunction, remove:AggregateFunction, group:Vec<Field>, projection:Vec<Field>, params: Vec<Field>, param_mask: u64, output_mask: u64, output_key:Vec<Field>},
    Filter {op: String, func: FilterFunction, left: Field, right: Field, param_mask: u64},
    Insert {e: Field, a: Field, v:Field, commit:bool},
    InsertIntermediate {key:Vec<Field>, value:Vec<Field>, negate:bool},
    Remove {e: Field, a: Field, v:Field},
    RemoveAttribute {e: Field, a: Field},
    RemoveEntity {e: Field },
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
            &Constraint::Project {ref registers} => { registers.iter().map(|v| Field::Register(*v)).collect() },
            &Constraint::Watch {ref registers, ..} => { filter_registers(&registers.iter().collect()) },
        }
    }

    pub fn get_output_registers(&self) -> Vec<Field> {
        match self {
            &Constraint::Scan { ref e, ref a, ref v, ..} => { filter_registers(&vec![e,a,v]) }
            &Constraint::Function {ref output, ..} => { filter_registers(&vec![output]) }
            &Constraint::MultiFunction {ref outputs, ..} => { filter_registers(&outputs.iter().collect()) }
            &Constraint::Aggregate {ref output, ..} => { filter_registers(&vec![output]) }
            &Constraint::IntermediateScan {ref value, ..} => { filter_registers(&value.iter().collect()) }
            _ => { vec![] }
        }
    }

    pub fn get_filtering_registers(&self) -> Vec<Field> {
        match self {
            &Constraint::Scan { ref e, ref a, ref v, ..} => { filter_registers(&vec![e,a,v]) }
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
            &Constraint::Aggregate {ref op, ref output, ref add, ref remove, ref group, ref projection, ref params, ref param_mask, ref output_mask, ref output_key} => {
                Constraint::Aggregate { op:op.clone(), output:output.clone(), add:*add, remove:*remove, group:group.clone(), projection:projection.clone(), params:params.clone(), param_mask:*param_mask, output_mask:*output_mask, output_key:output_key.clone() }
            }
            &Constraint::Filter {ref op, ref func, ref left, ref right, ref param_mask} => {
                Constraint::Filter{ op:op.clone(), func:*func, left:left.clone(), right:right.clone(), param_mask:*param_mask }
            }
            &Constraint::Insert { e,a,v,commit } => { Constraint::Insert { e,a,v,commit } },
            &Constraint::InsertIntermediate { ref key, ref value, negate } => { Constraint::InsertIntermediate {key:key.clone(), value:value.clone(), negate} }
            &Constraint::Remove { e,a,v } => { Constraint::Remove { e,a,v } },
            &Constraint::RemoveAttribute { e,a } => { Constraint::RemoveAttribute { e,a } },
            &Constraint::RemoveEntity { e } => { Constraint::RemoveEntity { e } },
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
            &Constraint::Project { ref registers } => { registers.hash(state); },
            &Constraint::Watch { ref name, ref registers } => { name.hash(state); registers.hash(state); },
        }
    }
}



impl fmt::Debug for Constraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Constraint::Scan { e, a, v, .. } => { write!(f, "Scan ( {:?}, {:?}, {:?} )", e, a, v) }
            &Constraint::AntiScan { ref key, .. } => { write!(f, "AntiScan ({:?})", key) }
            &Constraint::IntermediateScan { ref key, ref value, .. } => { write!(f, "IntermediateScan ( {:?}, {:?} )", key, value) }
            &Constraint::Insert { e, a, v, .. } => { write!(f, "Insert ( {:?}, {:?}, {:?} )", e, a, v) }
            &Constraint::InsertIntermediate { ref key, ref value, negate } => { write!(f, "InsertIntermediate ({:?}, {:?}, negate? {:?})", key, value, negate) }
            &Constraint::Function { ref op, ref params, ref output, .. } => { write!(f, "{:?} = {}({:?})", output, op, params) }
            &Constraint::MultiFunction { ref op, ref params, ref outputs, .. } => { write!(f, "{:?} = {}({:?})", outputs, op, params) }
            &Constraint::Aggregate { ref op, ref group, ref projection, ref params, ref output_key, .. } => { write!(f, "{:?} = {}(per: {:?}, for: {:?}, {:?})", output_key, op, group, projection, params) }
            &Constraint::Filter { ref op, ref left, ref right, .. } => { write!(f, "Filter ( {:?} {} {:?} )", left, op, right) }
            &Constraint::Project { ref registers } => { write!(f, "Project {:?}", registers) }
            &Constraint::Watch { ref name, ref registers } => { write!(f, "Watch {}{:?}", name, registers) }
            _ => { write!(f, "Constraint ...") }
        }
    }
}


pub fn make_register_mask(fields: Vec<&Field>) -> u64 {
    let mut mask = 0;
    for field in fields {
        match field {
            &Field::Register(r) => mask = set_bit(mask, (r % 64) as u32),
            _ => {},
        }
    }
    mask
}

pub fn make_scan(e:Field, a:Field, v:Field) -> Constraint {
    let register_mask = make_register_mask(vec![&e,&a,&v]);
    Constraint::Scan{e, a, v, register_mask }
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
        "string/replace" => string_replace,
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
        "string/split" => string_split,
        _ => panic!("Unknown multi function: {:?}", op)
    };
    Constraint::MultiFunction {op: op.to_string(), func, params, outputs, param_mask, output_mask }
}

pub fn make_aggregate(op: &str, group: Vec<Field>, projection:Vec<Field>, params: Vec<Field>, output: Field) -> Constraint {
    let param_mask = make_register_mask(params.iter().collect::<Vec<&Field>>());
    let output_mask = make_register_mask(vec![&output]);
    let (add, remove):(AggregateFunction, AggregateFunction) = match op {
        "gather/sum" => (aggregate_sum_add, aggregate_sum_remove),
        "gather/count" => (aggregate_count_add, aggregate_count_remove),
        "gather/average" => (aggregate_avg_add, aggregate_avg_remove),
        _ => panic!("Unknown function: {:?}", op)
    };
    Constraint::Aggregate {op: op.to_string(), add, remove, group, projection, params, output, param_mask, output_mask, output_key:vec![], }
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
        "contains" => string_contains,
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

pub fn string_contains(haystack:&Internable, needle:&Internable) -> bool {
    match (haystack, needle) {
        (&Internable::String(ref a), &Internable::String(ref b)) => {
            a.contains(b)
        },
        _ => { false }
    }
}

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
            Some(Internable::from_number(a.sin()))
        },
        _ => { None }
    }
}

pub fn math_cos(params: Vec<&Internable>) -> Option<Internable> {
    match params.as_slice() {
        &[&Internable::Number(_)] => {
            let a = Internable::to_number(params[0]);
            Some(Internable::from_number(a.cos()))
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

pub fn aggregate_sum_add(current: &mut AggregateEntry, params: Vec<Internable>) {
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

pub fn aggregate_sum_remove(current: &mut AggregateEntry, params: Vec<Internable>) {
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

pub fn aggregate_count_add(current: &mut AggregateEntry, _: Vec<Internable>) {
    match current {
        &mut AggregateEntry::Result(ref mut res) => { *res = *res + 1.0; }
        _ => { *current = AggregateEntry::Result(1.0); }
    }
}

pub fn aggregate_count_remove(current: &mut AggregateEntry, _: Vec<Internable>) {
    match current {
        &mut AggregateEntry::Result(ref mut res) => { *res = *res - 1.0; }
        _ => { *current = AggregateEntry::Result(-1.0); }
    }
}

pub fn aggregate_avg_add(current: &mut AggregateEntry, params: Vec<Internable>) {
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

pub fn aggregate_avg_remove(current: &mut AggregateEntry, params: Vec<Internable>) {
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
// Bit helpers
//-------------------------------------------------------------------------

pub fn check_bits(solved:u64, checking:u64) -> bool {
    solved & checking == checking
}

pub fn has_any_bits(solved:u64, checking:u64) -> bool {
    solved & checking != 0
}

pub fn set_bit(solved:u64, bit:u32) -> u64 {
    solved | (1 << bit)
}

pub fn clear_bit(solved:u64, bit:u32) -> u64 {
    solved & !(1 << bit)
}

pub fn check_bit(solved:u64, bit:u32) -> bool {
   solved & (1 << bit) != 0
}

//-------------------------------------------------------------------------
// Interpret
//-------------------------------------------------------------------------

#[inline(never)]
pub fn interpret(program: &mut RuntimeState, block_info: &BlockInfo, frame:&mut Frame, pipe:&Vec<Instruction>) {
    let mut iter_pool = EstimateIterPool::new();
    // println!("Doing work");
    let mut pointer:i32 = 0;
    let len = pipe.len() as i32;
    while pointer < len {
        frame.counters.instructions += 1;
        let inst = &pipe[pointer as usize];
        pointer += match *inst {
            Instruction::StartBlock {block} => {
                start_block(program, frame, block)
            },
            Instruction::MoveInputField { from, to } => {
                move_input_field(program, frame, from, to)
            },
            Instruction::MoveIntermediateField { from, to } => {
                move_intermediate_field(program, frame, from, to)
            },
            Instruction::AcceptIntermediateField { from, value, bail } => {
                accept_intermediate_field(program, frame, from, value, bail)
            },
            Instruction::GetIterator { iterator, constraint, bail } => {
                get_iterator(program, block_info, &mut iter_pool, frame, iterator, constraint, bail)
            },
            Instruction::IteratorNext { iterator, bail, finished_mask } => {
                iterator_next(program, &mut iter_pool, frame, iterator, bail, finished_mask)
            },
            Instruction::Accept { constraint, bail, iterator } => {
                // let start_ns = time::precise_time_ns();
                let next = accept(program, block_info, &mut iter_pool, frame, constraint, iterator, bail);
                // frame.counters.accept_ns += time::precise_time_ns() - start_ns;
                next
            },
            Instruction::ClearRounds => {
                clear_rounds(program, frame)
            },
            Instruction::GetRounds { constraint, bail } => {
                get_rounds(program, block_info, frame, constraint, bail)
            },
            Instruction::GetIntermediateRounds { constraint, bail } => {
                get_intermediate_rounds(program, block_info, frame, constraint, bail)
            },
            Instruction::Bind { constraint, next } => {
                bind(program, block_info, frame, constraint, next)
            },
            Instruction::Commit { constraint, next } => {
                commit(program, block_info, frame, constraint, next)
            },
            Instruction::InsertIntermediate { constraint, next } => {
                insert_intermediate(program, block_info, frame, constraint, next)
            },
            Instruction::Project { from, next } => {
                project(program, frame, from, next)
            },
            Instruction::Watch { ref name, next, constraint } => {
                watch(program, block_info, frame, name, next, constraint)
            },
        }
    };
}

//-------------------------------------------------------------------------
// Round holder
//-------------------------------------------------------------------------

#[derive(Debug)]
pub enum RoundState {
    Equal,
    Left,
    Right
}

pub struct RoundHolder {
    pub output_rounds: Vec<(Round, Count)>,
    prev_output_rounds: Vec<(Round, Count)>,
    rounds: Vec<HashMap<(Interned,Interned,Interned), Change>>,
    commits: HashMap<(Interned, Interned, Interned, Interned), (ChangeType, Change)>,
    staged_commit_keys: Vec<(Interned, Interned, Interned, Interned)>,
    collapsed_commits: CollapsedChanges,
    pub max_round: usize,
}

pub fn move_output_round(info:&Option<(Round, Count)>, round:&mut Round, count:&mut Count) {
    if let &Some((r, c)) = info {
        *round = r;
        *count += c;
    }
}

impl RoundHolder {
    pub fn new() -> RoundHolder {
        let mut rounds = vec![];
        for _ in 0..100 {
            rounds.push(HashMap::new());
        }
        RoundHolder { rounds, output_rounds:vec![], prev_output_rounds:vec![], commits:HashMap::new(), staged_commit_keys:vec![], collapsed_commits:CollapsedChanges::new(), max_round: 0 }
    }

    pub fn get_output_rounds(&self) -> &Vec<(Round, Count)> {
        match (self.output_rounds.len(), self.prev_output_rounds.len()) {
            (0, _) => &self.prev_output_rounds,
            (_, 0) => &self.output_rounds,
            (_, _) => panic!("neither round array is empty"),
        }
    }


    fn _compute_output_rounds<F>(&mut self, mut right_iter: DistinctIter, mut action:F)
        where F: FnMut(RoundState, &mut Vec<(Round, Count)>, Option<(Round, Count)>, Round, Count, Option<(Round, Count)>, Round, Count)
    {
        let (neue, current) = match (self.output_rounds.len(), self.prev_output_rounds.len()) {
            (0, _) => (&mut self.output_rounds, &mut self.prev_output_rounds),
            (_, 0) => (&mut self.prev_output_rounds, &mut self.output_rounds),
            (_, _) => panic!("neither round array is empty"),
        };
        {
            // let len = self.output_rounds.len();
            let mut left_iter = current.drain(..);
            let mut left_round = 0;
            let mut left_count = 0;
            let mut right_round = 0;
            let mut right_count = 0;
            let mut left = None;
            let mut right = None;
            let mut next_left = left_iter.next();
            let mut next_right = right_iter.next();
            let mut keep_running = true;
            // move_output_round(&left, &mut left_round, &mut left_count);
            // move_output_round(&right, &mut right_round, &mut right_count);
            while keep_running {
                // println!("left: {:?}, right {:?}", left, right);
                let state = if left_round == right_round {
                    RoundState::Equal
                } else if left_round > right_round {
                    while next_right != None && next_right.unwrap().0 < left_round {
                        right = next_right;
                        next_right = right_iter.next();
                        move_output_round(&right, &mut right_round, &mut right_count);
                    }
                    RoundState::Right
                } else {
                    while next_left != None && next_left.unwrap().0 < right_round {
                        left = next_left;
                        next_left = left_iter.next();
                        move_output_round(&left, &mut left_round, &mut left_count);
                    }
                    RoundState::Left
                };
                action(state, neue, left, left_round, left_count, right, right_round, right_count);
                match (next_left, next_right) {
                    (None, None) => { break; },
                    (None, Some(_)) => {
                        right = next_right;
                        next_right = right_iter.next();
                        move_output_round(&right, &mut right_round, &mut right_count);
                    },
                    (Some(_), None) => {
                        left = next_left;
                        next_left = left_iter.next();
                        move_output_round(&left, &mut left_round, &mut left_count);
                    },
                    (Some((next_left_round, _)), Some((next_right_round, _))) => {
                        if next_left_round < next_right_round {
                            left = next_left;
                            next_left = left_iter.next();
                            move_output_round(&left, &mut left_round, &mut left_count);
                        } else if next_left_round == next_right_round {
                            left = next_left;
                            next_left = left_iter.next();
                            move_output_round(&left, &mut left_round, &mut left_count);
                            right = next_right;
                            next_right = right_iter.next();
                            move_output_round(&right, &mut right_round, &mut right_count);
                        } else {
                            right = next_right;
                            next_right = right_iter.next();
                            move_output_round(&right, &mut right_round, &mut right_count);
                        }
                    }
                }
                keep_running = left != None || right != None;
            }
        }
    }

    pub fn compute_anti_output_rounds(&mut self, right_iter: DistinctIter) {
        let action = |state, neue:&mut Vec<(Round,Count)>, left, left_round, left_count, right, right_round, right_count| {
            match state {
                RoundState::Equal => {
                    if let Some((_, count)) = left {
                        if right_count == 0 && count != 0 {
                            neue.push((left_round, count));
                        }
                    }
                },
                RoundState::Right => {
                    if let Some((_, count)) = left {
                        if right_count == 0 {
                            neue.push((left_round, count));
                        }
                    }
                },
                RoundState::Left => {
                    if let Some((_, count)) = right {
                        let total = (count * -1) * left_count;
                        if total != 0 {
                            neue.push((right_round, total));
                        }
                    }
                }
            }
        };
        self._compute_output_rounds(right_iter, action);
    }

    pub fn compute_output_rounds(&mut self, right_iter: DistinctIter) {
        let action = |state, neue:&mut Vec<(Round,Count)>, left, left_round, left_count, right, right_round, right_count| {
            match state {
                RoundState::Equal => {
                    if let Some((_, count)) = left {
                        let total = count * right_count;
                        if total != 0 {
                            neue.push((left_round, total));
                        } else if left_count == 0 && right_count == 0 {
                            neue.push((left_round, count));
                        }
                    }

                },
                RoundState::Right => {
                    if let Some((_, count)) = left {
                        let total = count * right_count;
                        if total != 0 {
                            neue.push((left_round, total));
                        }
                    }
                },
                RoundState::Left => {
                    if let Some((_, count)) = right {
                        let total = count * left_count;
                        if total != 0 {
                            neue.push((right_round, total));
                        }
                    }
                }
            }
        };
        self._compute_output_rounds(right_iter, action);
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

    pub fn prepare_commits(&mut self, index:&mut HashIndex) -> bool {
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
                                                let cloned = Change {e, a:attr, v:val, n, count, transaction, round};
                                                self.collapsed_commits.insert(cloned);
                                            }
                                        }
                                    }
                                }
                            },
                            (_, 0) => {
                                if let Some(vals) = index.get(e, a, 0) {
                                    for val in vals {
                                        let cloned = Change {e, a, v:val, n, count, transaction, round};
                                        self.collapsed_commits.insert(cloned);
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
            index.distinct(&change, self);
        }
        has_changes
    }

    pub fn clear(&mut self) {
        for ix in 0..self.max_round {
            self.rounds[ix].clear();
        }
        self.clear_output_rounds();
        self.max_round = 0;
    }

    pub fn clear_output_rounds(&mut self) {
        self.output_rounds.clear();
        self.prev_output_rounds.clear();
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

impl<'a> RoundHolderIter {
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
    pub index: HashIndex,
    pub interner: Interner,
    pub watch_indexes: HashMap<String, WatchIndex>,
    pub intermediates: IntermediateIndex,
}

impl RuntimeState {
    pub fn watch(&mut self, name:&str, resolved:Vec<Interned>, count:Count) {
        let index = self.watch_indexes.entry(name.to_string()).or_insert_with(|| WatchIndex::new());
        index.insert(resolved, count);
    }
}


pub struct BlockInfo {
    pub pipe_lookup: HashMap<(Interned,Interned,Interned), Vec<Vec<Instruction>>>,
    pub intermediate_pipe_lookup: HashMap<Interned, Vec<Vec<Instruction>>>,
    pub block_names: HashMap<String, usize>,
    pub blocks: Vec<Block>,
}

impl BlockInfo {
    pub fn get_block(&self, name:&str) -> &Block {
        let ix = self.block_names.get(name).unwrap();
        &self.blocks[*ix]
    }

}

pub struct Program {
    pub state: RuntimeState,
    pub block_info: BlockInfo,
    watchers: HashMap<String, Box<Watcher + Send>>,
    pub incoming: Receiver<Vec<RawChange>>,
    pub outgoing: SyncSender<Vec<RawChange>>,
}

impl Program {
    pub fn new() -> Program {
        let index = HashIndex::new();
        let intermediates = IntermediateIndex::new();
        let interner = Interner::new();
        let rounds = RoundHolder::new();
        let block_names = HashMap::new();
        let watch_indexes = HashMap::new();
        let watchers = HashMap::new();
        let pipe_lookup = HashMap::new();
        let intermediate_pipe_lookup = HashMap::new();
        let blocks = vec![];
        let (outgoing, incoming) = mpsc::sync_channel(1);
        let state = RuntimeState { debug:false, rounds, index, interner, watch_indexes, intermediates };
        let block_info = BlockInfo { pipe_lookup, intermediate_pipe_lookup, block_names, blocks };
        Program { state, block_info, watchers, incoming, outgoing }
    }

    pub fn clear(&mut self) {
        self.state.index = HashIndex::new();
    }

    #[allow(dead_code)]
    pub fn exec_query(&mut self, name:&str) -> Vec<Interned> {
        let mut frame = Frame::new();
        // let start_ns = time::precise_time_ns();
        let pipe = self.block_info.get_block(name).pipes[0].clone();
        interpret(&mut self.state, &mut self.block_info, &mut frame, &pipe);
        // frame.counters.total_ns += time::precise_time_ns() - start_ns;
        // println!("counters: {:?}", frame.counters);
        return frame.results;
    }

    #[allow(dead_code)]
    pub fn raw_insert(&mut self, e:Interned, a:Interned, v:Interned, round:Round, count:Count) {
        self.state.index.insert_distinct(e,a,v,round,count);
    }

    pub fn register_block(&mut self, mut block:Block) {
        let ix = self.block_info.blocks.len();
        for (pipe_ix, ref mut pipe) in block.pipes.iter_mut().enumerate() {
            if let Some(&mut Instruction::StartBlock {ref mut block}) = pipe.get_mut(0) {
                *block = ix;
            } else { panic!("Block where the first instruction is not a start block.") }
            for shape in block.shapes[pipe_ix].iter() {
                match shape {
                    &PipeShape::Scan(e,a,v) => {
                        let cur = self.block_info.pipe_lookup.entry((e,a,v)).or_insert_with(|| vec![]);
                        cur.push(pipe.clone());
                    }
                    &PipeShape::Intermediate(id) => {
                        let cur = self.block_info.intermediate_pipe_lookup.entry(id).or_insert_with(|| vec![]);
                        cur.push(pipe.clone());
                    }
                }
            }
        }
        self.block_info.block_names.insert(block.name.to_string(), ix);
        self.block_info.blocks.push(block);
    }

    pub fn unregister_block(&mut self, block:&Block) {
        println!("Unregister: {}", block.name);
        unimplemented!();
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

    pub fn attach(&mut self, name:&str, watcher:Box<Watcher + Send>) {
        self.watchers.insert(name.to_string(), watcher);
    }

    pub fn get_pipes<'a>(&self, block_info:&'a BlockInfo, input: &Change, pipes: &mut Vec<&'a Vec<Instruction>>) {
        let ref pipe_lookup = block_info.pipe_lookup;
        let mut tuple = (0,0,0);
        // look for (0,0,0), (0, a, 0) and (0, a, v) pipes
        match pipe_lookup.get(&tuple) {
            Some(found) => {
                for pipe in found.iter() {
                    pipes.push(pipe);
                }
            },
            None => {},
        }
        tuple.1 = input.a;
        match pipe_lookup.get(&tuple) {
            Some(found) => {
                for pipe in found.iter() {
                    pipes.push(pipe);
                }
            },
            None => {},
        }
        tuple.2 = input.v;
        match pipe_lookup.get(&tuple) {
            Some(found) => {
                for pipe in found.iter() {
                    pipes.push(pipe);
                }
            },
            None => {},
        }
        // lookup the tags for this e
        //  for each tag, lookup (e, a, 0) and (e, a, v)
        if let Some(tags) = self.state.index.get(input.e, TAG_INTERNED_ID, 0) {
            for tag in tags {
                tuple.0 = tag;
                tuple.2 = 0;
                match pipe_lookup.get(&tuple) {
                    Some(found) => {
                        for pipe in found.iter() {
                            pipes.push(pipe);
                        }
                    },
                    None => {},
                }
                tuple.2 = input.v;
                match pipe_lookup.get(&tuple) {
                    Some(found) => {
                        for pipe in found.iter() {
                            pipes.push(pipe);
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

fn intermediate_flow(frame: &mut Frame, state: &mut RuntimeState, block_info: &BlockInfo, current_round:Round, max_round:&mut Round) {
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
                        interpret(state, block_info, frame, pipe);
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

fn transaction_flow(commits: &mut Vec<Change>, frame: &mut Frame, program: &mut Program) {
    let mut pipes = vec![];
    let mut next_frame = true;

    while next_frame {
        let mut current_round = 0;
        let mut max_round:Round = program.state.rounds.max_round as Round;
        let mut items = program.state.rounds.iter();
        while current_round <= max_round {
            let round = items.get_round(&mut program.state.rounds, current_round);
            for change in round.iter() {
                // println!("{}", change.print(&program));
                // If this is an add, we want to do it *before* we start running pipes.
                // This ensures that if there are two constraints in a single block that
                // would both match the given input, they both have a chance to see this
                // new triple at the same time. Doing so, means we don't have to go through
                // every possible combination of the inputs, e.g. A, B, and AB. Instead we
                // do AB and BA. To make sure that removes correctly cancel out, we don't
                // want to do a real remove until *after* the pipes have run. Hence, the
                // separation of insert and remove.
                if change.count > 0 {
                    program.state.index.insert(change.e, change.a, change.v, change.round);
                }
                pipes.clear();
                program.get_pipes(&program.block_info, change, &mut pipes);
                frame.reset();
                frame.input = Some(*change);
                for pipe in pipes.iter() {
                    // print_pipe(pipe, &program.block_info, &mut program.state);
                    frame.row.reset();
                    interpret(&mut program.state, &program.block_info, frame, pipe);
                    // if program.state.debug {
                    //     program.state.debug = false;
                    //     println!("\n---------------------------------\n");
                    // }
                }
                // as stated above, we want to do removes after so that when we look
                // for AB and BA, they find the same values as when they were added.
                if change.count < 0 {
                    program.state.index.remove(change.e, change.a, change.v, change.round);
                }
                if current_round == 0 { commits.push(change.clone()); }
            }
            intermediate_flow(frame, &mut program.state, &program.block_info, current_round, &mut max_round);
            max_round = cmp::max(max_round, program.state.rounds.max_round as Round);
            current_round += 1;
        }
        next_frame = program.state.rounds.prepare_commits(&mut program.state.index);
    }

    for (name, index) in program.state.watch_indexes.iter_mut() {
        if index.dirty() {
            let diff = index.reconcile();
            if let Some(watcher) = program.watchers.get(name) {
                watcher.on_diff(&program.state.interner, diff);
            }
        }
    }
}

pub struct Transaction {
    changes: Vec<Change>,
    commits: Vec<Change>,
    frame: Frame,
}

impl Transaction {
    pub fn new() -> Transaction {
        let frame = Frame::new();
        Transaction { changes: vec![], commits: vec![], frame}
    }

    pub fn input(&mut self, e:Interned, a:Interned, v:Interned, count: Count) {
        let change = Change { e,a,v,n: 0, transaction:0, round:0, count };
        self.changes.push(change);
    }

    pub fn input_change(&mut self, change: Change) {
        self.changes.push(change);
    }

    pub fn exec(&mut self, program: &mut Program) {
        for change in self.changes.iter() {
            program.state.index.distinct(&change, &mut program.state.rounds);
        }
        transaction_flow(&mut self.commits, &mut self.frame, program);
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
    frame: Frame,
}

impl CodeTransaction {
    pub fn new() -> CodeTransaction {
        let frame = Frame::new();
        CodeTransaction { changes: vec![], commits:vec![], frame}
    }

    pub fn exec(&mut self, program: &mut Program, to_add:Vec<Block>, to_remove:Vec<&Block>) {
        for change in self.changes.iter() {
            program.state.index.distinct(&change, &mut program.state.rounds);
        }

        let ref mut frame = self.frame;

        for add in to_add {
            frame.reset();
            frame.input = Some(Change { e:0,a:0,v:0,n: 0, transaction:0, round:0, count:1 });
            program.register_block(add);
            interpret(&mut program.state, &program.block_info, frame, &program.block_info.blocks.last().unwrap().pipes[0]);
        }

        for remove in to_remove {
            frame.reset();
            frame.input = Some(Change { e:0,a:0,v:0,n: 0, transaction:0, round:0, count:-1 });
            interpret(&mut program.state, &program.block_info, frame, &remove.pipes[0]);
            program.unregister_block(remove);
        }

        let mut max_round = 0;
        intermediate_flow(frame, &mut program.state, &program.block_info, 0, &mut max_round);

        transaction_flow(&mut self.commits, frame, program);
    }
}

//-------------------------------------------------------------------------
// Program Runner
//-------------------------------------------------------------------------

pub struct RunLoop {
    thread: JoinHandle<()>,
    close_message: RawChange,
    outgoing: SyncSender<Vec<RawChange>>,
}

impl RunLoop {
    pub fn wait(self) {
        self.thread.join().unwrap();
    }

    pub fn close(&self) {
        self.outgoing.send(vec![self.close_message.clone()]).unwrap();
    }

    pub fn send(&self, msg: Vec<RawChange>) {
        self.outgoing.send(msg).unwrap();
    }
}

pub struct ProgramRunner {
    pub program: Program,
    paths: Vec<String>,
    close_message: RawChange,
}

impl ProgramRunner {
    pub fn new() -> ProgramRunner {
        let close_message = RawChange {e:Internable::Null, a:Internable::Null, v:Internable::Null, n:Internable::Null, count:0};
        ProgramRunner {close_message, paths: vec![], program: Program::new()}
    }

    pub fn load(&mut self, path:&str) {
        self.paths.push(path.to_owned());
    }

    pub fn run(self) -> RunLoop {
        let outgoing = self.program.outgoing.clone();
        let close_message = self.close_message.clone();
        let close_message2 = self.close_message.clone();
        let mut program = self.program;
        let paths = self.paths;
        let thread = thread::spawn(move || {
            let mut blocks = vec![];
            for path in paths {
                blocks.extend(parse_file(&mut program, &path));
            }

            let mut txn = CodeTransaction::new();
            txn.exec(&mut program, blocks, vec![]);

            println!("Starting run loop.");
            'outer: loop {
                match program.incoming.recv() {
                    Ok(v) => {
                        let start_ns = time::precise_time_ns();
                        let mut txn = Transaction::new();
                        for cur in v {
                            if cur == close_message2 { break 'outer; }
                            txn.input_change(cur.to_change(&mut program.state.interner));
                        };
                        txn.exec(&mut program);
                        let end_ns = time::precise_time_ns();
                        println!("Txn took {:?}", (end_ns - start_ns) as f64 / 1_000_000.0);
                    }
                    Err(_) => { break; }
                }
            }
            println!("Closing run loop.");
        });

        RunLoop { thread, outgoing, close_message }
    }

}
