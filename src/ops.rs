//-------------------------------------------------------------------------
// Ops
//-------------------------------------------------------------------------

// TODO:
//  - index insert
//  - functions

extern crate time;

use indexes::{HashIndex, DistinctIter, HashIndexIter, WatchIndex};
use parser::{make_block};
use std::collections::HashMap;
use std::mem::transmute;
use std::collections::hash_map::Entry;
use std::cmp;
use std::iter::Iterator;
use std::fmt;

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
        let a = prog.interner.get_value(self.a).print();
        let mut v = prog.interner.get_value(self.v).print();
        v = if v.contains("|") { format!("<{}>", self.v) } else { v };
        format!("Change (<{}>, {:?}, {})  {}:{}:{}", self.e, a, v, self.transaction, self.round, self.count)
    }
}

//-------------------------------------------------------------------------
// Block
//-------------------------------------------------------------------------

pub type Pipe = Vec<Instruction>;
pub type PipeShape = (Interned, Interned, Interned);

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
                &Constraint::Function {ref output, ..} => {
                    // @TODO: ensure that all inputs are accounted for
                    // count the registers in the functions
                    if let &Field::Register(offset) = output {
                        registers = cmp::max(registers, offset + 1);
                    }
                    get_iters.push(Instruction::GetIterator { bail: 0, constraint: ix, iterator: 0 });
                    accepts.push(Instruction::Accept { bail: 0, constraint: ix, iterator: 0 });
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
                    watch_constraints.push(constraint);
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
            if *scan_ix != NO_INPUTS_PIPE {
                for move_inst in &moves[scan_ix] {
                    pipe.push(move_inst.clone());
                    to_solve -= 1;
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
                pipe.push(Instruction::IteratorNext { bail: iter_bail, iterator: ix as u32 });
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

            if outputs.len() > 0 {
                for inst in get_rounds.iter() {
                    if let &Instruction::GetRounds { constraint, .. } = inst {
                        if constraint != *scan_ix {
                            last_iter_next -= 1;
                            let mut neue = inst.clone();
                            if let Instruction::GetRounds { ref mut bail, .. } = neue {
                                *bail = last_iter_next;
                            }
                            pipe.push(neue);
                        }
                    }
                }
            }

            for (ix, output) in outputs.iter().enumerate() {
                last_iter_next -= 1;
                if ix < outputs_len - 1 {
                    pipe.push(output.clone());
                } else {
                    let mut neue = output.clone();
                    if let Instruction::Bind {ref mut next, ..} = neue {
                        *next = if to_solve > 0 {
                            last_iter_next
                        } else {
                            PIPE_FINISHED
                        }
                    } else if let Instruction::Commit { ref mut next, ..} = neue {
                        *next = if to_solve > 0 {
                            last_iter_next
                        } else {
                            PIPE_FINISHED
                        }
                    }
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

            for constraint in watch_constraints.iter() {
                if let &&Constraint::Watch {ref name, ref registers} = constraint {
                    last_iter_next -= 1;
                    let next = if to_solve > 0 {last_iter_next} else { PIPE_FINISHED };
                    pipe.push(Instruction::Watch {next, registers:registers.iter().map(|x| *x as u32).collect(), name:name.to_string()});
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

    pub fn to_shapes(&self, scans: Vec<&Constraint>) -> Vec<Vec<(Interned, Interned, Interned)>> {
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
            if let &&Constraint::Scan {ref e, ref a, ref v, ..} = scan {
                let actual_e = if let &Field::Value(val) = e { val } else { 0 };
                let actual_a = if let &Field::Value(val) = a { val } else { 0 };
                let actual_v = if let &Field::Value(val) = v { val } else { 0 };
                if actual_a == tag {
                    scan_shapes.push((0, actual_a, actual_v));
                } else {
                    match tag_mappings.get(e) {
                        Some(mappings) => {
                            for mapping in mappings {
                                scan_shapes.push((*mapping, actual_a, actual_v))
                            }
                        },
                        None => {
                            scan_shapes.push((actual_e, actual_a, actual_v))
                        }
                    }
                }
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
}

impl Row {
    pub fn new(size:usize) -> Row {
        Row { fields: vec![0; size], solved_fields: 0, solving_for: 0 }
    }

    pub fn set(&mut self, field_index:u32, value:Interned) {
        self.fields[field_index as usize] = value;
        self.solving_for = set_bit(0, field_index);
        self.solved_fields = set_bit(self.solved_fields, field_index);
    }

    pub fn clear(&mut self, field_index:u32) {
        self.fields[field_index as usize] = 0;
        self.solving_for = 0;
        self.solved_fields = clear_bit(self.solved_fields, field_index);
    }

    pub fn reset(&mut self, size:u32) {
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
}

impl EstimateIterPool {
    pub fn new() -> EstimateIterPool {
        EstimateIterPool { available: vec![], available_funcs: vec![] }
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
            _ => panic!("Releasing non-scan"),
        }
        match estimate_iter {
            EstimateIter::Scan {..} => {
                self.available.push(estimate_iter);
            },
            EstimateIter::Function {..} => {
                self.available_funcs.push(estimate_iter);
            },
            _ => panic!("Releasing non-scan"),
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
}


#[derive(Clone, Debug)]
pub enum EstimateIter {
    Scan {estimate: u32, iter: HashIndexIter, output: u32, constraint: u32},
    Function {estimate: u32, output: u32, result: Interned, returned: bool, constraint: u32},
    MultiRowFunction {estimate: u32, output: u32, results: Vec<Interned>, returned: bool, constraint: u32},
}

impl EstimateIter {
    pub fn estimate(&self) -> u32 {
        match self {
            &EstimateIter::Scan {ref estimate, .. } => {
                *estimate
            },
            &EstimateIter::Function {ref estimate, .. } => {
                *estimate
            },
            &EstimateIter::MultiRowFunction {ref estimate, .. } => {
                *estimate
            },
        }
    }

    pub fn next(&mut self, row:&mut Row) -> bool {
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
            _ => panic!("Implement me"),
        }
    }

    pub fn clear(&self, row:&mut Row) {
        match self {
            &EstimateIter::Scan {ref output, .. } => {
                row.clear(*output);
            },
            &EstimateIter::Function { ref output, .. } => {
                row.clear(*output);
            },
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
    row: Row,
    block_ix: usize,
    iters: Vec<Option<EstimateIter>>,
    results: Vec<Interned>,
    #[allow(dead_code)]
    counters: Counters,
}

impl Frame {
    pub fn new() -> Frame {
        Frame {row: Row::new(64), block_ix:0, input: None, iters: vec![None; 64], results: vec![], counters: Counters {iter_next: 0, accept: 0, accept_bail: 0, instructions: 0, accept_ns: 0, total_ns: 0, considered: 0}}
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

    pub fn check_iter(&mut self, iter_pool: &mut EstimateIterPool, iter_ix:u32, iter: EstimateIter) {
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
                iter_pool.release(cur.take().unwrap());
                Some(iter)
            },
            old => old,
        };
        match neue {
            Some(_) => { self.iters[ix] = neue; },
            None => {},
        }
    }

    pub fn reset(&mut self) {
        self.input = None;
        self.results.clear();
        self.row.reset(64);
    }
}



//-------------------------------------------------------------------------
// Instruction
//-------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum Instruction {
    StartBlock { block: usize },
    GetIterator {iterator: u32, bail: i32, constraint: u32},
    IteratorNext {iterator: u32, bail: i32},
    Accept {bail: i32, constraint:u32, iterator:u32},
    MoveInputField { from:u32, to:u32, },
    ClearRounds,
    GetRounds {bail: i32, constraint: u32},
    Bind {next: i32, constraint:u32},
    Commit {next: i32, constraint:u32},
    Project {next: i32, from:u32},
    Watch { next:i32, registers:Vec<u32>, name:String}
}

#[inline(never)]
pub fn start_block(_: &mut Program, frame: &mut Frame, block:usize) -> i32 {
    // println!("STARTING! {:?}", block);
    frame.block_ix = block;
    1
}

#[inline(never)]
pub fn move_input_field(_: &mut Program, frame: &mut Frame, from:u32, to:u32) -> i32 {
    // println!("STARTING! {:?}", block);
    if let Some(change) = frame.input {
        match from {
            0 => { frame.row.set(to, change.e); }
            1 => { frame.row.set(to, change.a); }
            2 => { frame.row.set(to, change.v); }
            _ => { panic!("Unknown move: {:?}", from); },
        }
    }
    1
}

#[inline(never)]
pub fn get_iterator(program: &mut Program, frame: &mut Frame, iter_ix:u32, cur_constraint:u32, bail:i32) -> i32 {
    let cur = &program.blocks[frame.block_ix].constraints[cur_constraint as usize];
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
            let mut iter = program.iter_pool.get();
            program.index.propose(&mut iter, resolved_e, resolved_a, resolved_v);
            match iter {
                EstimateIter::Scan {ref mut output, ref mut constraint, ..} => {
                    *constraint = cur_constraint;
                    *output = match (*output, e, a, v) {
                        (0, &Field::Register(reg), _, _) => reg as u32,
                        (1, _, &Field::Register(reg), _) => reg as u32,
                        (2, _, _, &Field::Register(reg)) => reg as u32,
                        _ => panic!("bad scan output"),
                    };
                }
                _ => panic!("Implement me"),
            }

            // println!("get iter: {:?}", cur_constraint);
            frame.check_iter(&mut program.iter_pool, iter_ix, iter);
            1
        },
        &Constraint::Function {ref func, ref output, ref params, param_mask, output_mask, ..} => {
            let solved = frame.row.solved_fields;
            if check_bits(solved, param_mask) && !check_bits(solved, output_mask) {
                let result = {
                    let mut resolved = vec![];
                    for param in params {
                        resolved.push(program.interner.get_value(frame.resolve(param)));
                    }
                    func(resolved)
                };
                let mut iter = program.iter_pool.get_func();
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
                        frame.check_iter(&mut program.iter_pool, iter_ix, iter);
                        1
                    }
                    _ => bail,
                }
            } else {
                1
            }
            // println!("get function iterator {:?}", cur);
        },
        _ => { 1 }
    }
}

#[inline(never)]
pub fn iterator_next(_: &mut Program, frame: &mut Frame, iterator:u32, bail:i32) -> i32 {
    let go = {
        let mut iter = frame.iters[iterator as usize].as_mut();
        // println!("Iter Next: {:?}", iter);
        match iter {
            Some(ref mut cur) => {
                match cur.next(&mut frame.row) {
                    false => {
                        cur.clear(&mut frame.row);
                        bail
                    },
                    true => {
                        // frame.counters.iter_next += 1;
                        1
                    },
                }
            },
            None => bail,
        }
    };
    if go == bail {

        if let Some(ref cur) = frame.iters[iterator as usize] {
            let est = cur.estimate();
            frame.counters.considered += est as u64;
        }
        frame.iters[iterator as usize] = None;
    }
    // println!("Row: {:?}", &frame.row.fields[0..3]);
    go
}

#[inline(never)]
pub fn accept(program: &mut Program, frame: &mut Frame, cur_constraint:u32, cur_iterator:u32, bail:i32) -> i32 {
    frame.counters.accept += 1;
    let cur = &program.blocks[frame.block_ix].constraints[cur_constraint as usize];
    if cur_iterator > 0 {
        if let Some(EstimateIter::Scan { constraint, .. }) = frame.iters[(cur_iterator - 1) as usize] {
            if constraint == cur_constraint {
                // frame.counters.accept_bail += 1;
                return 1;
            }
        }
    }
    match cur {
        &Constraint::Scan {ref e, ref a, ref v, ref register_mask} => {
            // if we aren't solving for something this scan cares about, then we
            // automatically accept it.
            if !check_bits(*register_mask, frame.row.solving_for) {
                // println!("auto accept {:?} {:?}", cur, frame.row.solving_for);
               return 1;
            }
            let resolved_e = frame.resolve(e);
            let resolved_a = frame.resolve(a);
            let resolved_v = frame.resolve(v);
            let checked = program.index.check(resolved_e, resolved_a, resolved_v);
            // println!("scan accept {:?} {:?}", cur_constraint, checked);
            match checked {
                true => 1,
                false => bail,
            }
        },
        &Constraint::Function {/* ref op, ref outputs, ref params, */ ref param_mask, ref output_mask, .. } => {
            let solved = frame.row.solved_fields;
            if check_bits(solved, *param_mask) && check_bits(solved, *output_mask) {

            }
            1
        },
        &Constraint::Filter {ref left, ref right, ref func, ref param_mask, .. } => {
            if !check_bits(*param_mask, frame.row.solving_for) {
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
        _ => { 1 }
    }
}

#[inline(never)]
pub fn clear_rounds(program: &mut Program, frame: &mut Frame) -> i32 {
    program.rounds.clear_output_rounds();
    if let Some(change) = frame.input {
        program.rounds.output_rounds.push((change.round, change.count));
    }
    1
}

#[inline(never)]
pub fn get_rounds(program: &mut Program, frame: &mut Frame, constraint:u32, bail:i32) -> i32 {
    // println!("get rounds!");
    let cur = &program.blocks[frame.block_ix].constraints[constraint as usize];
    match cur {
        &Constraint::Scan {ref e, ref a, ref v, .. } => {
            let resolved_e = frame.resolve(e);
            let resolved_a = frame.resolve(a);
            let resolved_v = frame.resolve(v);
            // println!("getting rounds for {:?} {:?} {:?}", e, a, v);
            program.rounds.compute_output_rounds(program.index.distinct_iter(resolved_e, resolved_a, resolved_v));
            1
        },
        _ => { panic!("Get rounds on non-scan") }
    }

}

#[inline(never)]
pub fn bind(program: &mut Program, frame: &mut Frame, constraint:u32, next:i32) -> i32 {
    let cur = &program.blocks[frame.block_ix].constraints[constraint as usize];
    match cur {
        &Constraint::Insert {ref e, ref a, ref v, ..} => {
            let c = Change { e: frame.resolve(e), a: frame.resolve(a), v:frame.resolve(v), n: 0, round:0, transaction: 0, count:0, };
            // println!("want to output {:?}", c);
            let ref mut rounds = program.rounds;
            // println!("rounds {:?}", rounds.output_rounds);
            // @FIXME this clone is completely unnecessary, but borrows are a bit sad here
            for &(round, count) in rounds.get_output_rounds().clone().iter() {
                let output = &c.with_round_count(round + 1, count);
                program.index.distinct(output, rounds);
                // println!("insert {:?}", output);
            }
        },
        _ => {}
    };
    next
}

#[inline(never)]
pub fn commit(program: &mut Program, frame: &mut Frame, constraint:u32, next:i32) -> i32 {
    let cur = &program.blocks[frame.block_ix].constraints[constraint as usize];
    match cur {
        &Constraint::Insert {ref e, ref a, ref v, ref commit} => {
            let n = (frame.block_ix as u32) * 10000 + constraint;
            let c = Change { e: frame.resolve(e), a: frame.resolve(a), v:frame.resolve(v), n, round:0, transaction: 0, count:0, };
            // println!("want to output {:?}", c);
            let ref mut rounds = program.rounds;
            // println!("rounds {:?}", rounds.output_rounds);
            // @FIXME this clone is completely unnecessary, but borrows are a bit sad here
            for &(round, count) in rounds.get_output_rounds().clone().iter() {
                let output = c.with_round_count(0, count);
                rounds.commit(output, ChangeType::Insert)
                // println!("insert {:?}", output);
            }
        },
        _ => {}
    };
    next
}

#[inline(never)]
pub fn project(_: &mut Program, frame: &mut Frame, from:u32, next:i32) -> i32 {
    let value = frame.get_register(from);
    frame.results.push(value);
    next
}

#[inline(never)]
pub fn watch(program: &mut Program, frame: &mut Frame, name:&str, registers:&Vec<u32>, next:i32) -> i32 {
    let resolved = registers.iter().map(|x| frame.get_register(*x)).collect();
    let mut total = 0;
    for &(_, count) in program.rounds.get_output_rounds().iter() {
        total += count;
    }
    program.watch(name, resolved, total);
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
    fn to_number(intern: &Internable) -> f32 {
        match intern {
            &Internable::Number(num) => unsafe { transmute::<u32, f32>(num) },
            _ => { panic!("to_number on non-number") }
        }
    }

    fn from_number(num: f32) -> Internable {
        let value = unsafe { transmute::<f32, u32>(num) };
        Internable::Number(value)
    }

    pub fn print(&self) -> String {
        match self {
            &Internable::String(ref s) => {
                s.to_string()
            }
            &Internable::Number(n) => {
                Internable::to_number(self).to_string()
            }
            &Internable::Null => {
                "Null!".to_string()
            }
        }
    }
}

pub struct Interner {
    id_to_value: HashMap<Internable, Interned>,
    value_to_id: Vec<Internable>,
    next_id: Interned,
}

impl Interner {
    pub fn new() -> Interner {
        let mut me = Interner {id_to_value: HashMap::new(), value_to_id:vec![Internable::Null], next_id:1};
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

// #[derive(Clone)]
#[allow(dead_code)]
pub enum Constraint {
    Scan {e: Field, a: Field, v: Field, register_mask: u64},
    Function {op: String, output: Field, func: Function, params: Vec<Field>, param_mask: u64, output_mask: u64},
    Filter {op: String, func: FilterFunction, left: Field, right: Field, param_mask: u64},
    Insert {e: Field, a: Field, v:Field, commit:bool},
    Remove {e: Field, a: Field, v:Field},
    RemoveAttribute {e: Field, a: Field},
    RemoveEntity {e: Field, a: Field},
    Project {registers: Vec<usize>},
    Watch {name: String, registers: Vec<usize>},
}

impl fmt::Debug for Constraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Constraint::Scan { e, a, v, .. } => { write!(f, "Scan ( {:?}, {:?}, {:?} )", e, a, v) }
            &Constraint::Insert { e, a, v, .. } => { write!(f, "Insert ( {:?}, {:?}, {:?} )", e, a, v) }
            &Constraint::Function { ref op, ref params, ref output, .. } => { write!(f, "{:?} = {}({:?})", output, op, params) }
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
            &Field::Register(r) => mask = set_bit(mask, r as u32),
            _ => {},
        }
    }
    mask
}

pub fn make_scan(e:Field, a:Field, v:Field) -> Constraint {
    let register_mask = make_register_mask(vec![&e,&a,&v]);
    Constraint::Scan{e, a, v, register_mask }
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
        "concat" => concat,
        "gen_id" => gen_id,
        _ => panic!("Unknown function: {:?}", op)
    };
    Constraint::Function {op: op.to_string(), func, params, output, param_mask, output_mask }
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
// Bit helpers
//-------------------------------------------------------------------------

pub fn check_bits(solved:u64, checking:u64) -> bool {
    solved & checking == checking
}

pub fn set_bit(solved:u64, bit:u32) -> u64 {
    solved | (1 << bit)
}

pub fn clear_bit(solved:u64, bit:u32) -> u64 {
    solved & !(1 << bit)
}

//-------------------------------------------------------------------------
// Interpret
//-------------------------------------------------------------------------

#[inline(never)]
pub fn interpret(program: &mut Program, frame:&mut Frame, pipe:&Vec<Instruction>) {
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
            Instruction::GetIterator { iterator, constraint, bail } => {
                get_iterator(program, frame, iterator, constraint, bail)
            },
            Instruction::IteratorNext { iterator, bail } => {
                iterator_next(program, frame, iterator, bail)
            },
            Instruction::Accept { constraint, bail, iterator } => {
                // let start_ns = time::precise_time_ns();
                let next = accept(program, frame, constraint, iterator, bail);
                // frame.counters.accept_ns += time::precise_time_ns() - start_ns;
                next
            },
            Instruction::ClearRounds => {
                clear_rounds(program, frame)
            },
            Instruction::GetRounds { constraint, bail } => {
                get_rounds(program, frame, constraint, bail)
            },
            Instruction::Bind { constraint, next } => {
                bind(program, frame, constraint, next)
            },
            Instruction::Commit { constraint, next } => {
                commit(program, frame, constraint, next)
            },
            Instruction::Project { from, next } => {
                project(program, frame, from, next)
            },
            Instruction::Watch { ref name, ref registers, next } => {
                watch(program, frame, name, registers, next)
            },
        }
    };
}

//-------------------------------------------------------------------------
// Round holder
//-------------------------------------------------------------------------

pub struct RoundHolder {
    pub output_rounds: Vec<(Round, Count)>,
    prev_output_rounds: Vec<(Round, Count)>,
    rounds: Vec<HashMap<(Interned,Interned,Interned), Change>>,
    commits: HashMap<(Interned, Interned, Interned, Interned), (ChangeType, Change)>,
    collapsed_commits: HashMap<(Interned, Interned, Interned), Change>,
    pub max_round: usize,
}

pub fn move_output_round(info:&Option<(Round, Count)>, round:&mut Round, count:&mut Count) {
    if let &Some((r, c)) = info {
        *round = r;
        *count += c;
    }
}

pub fn commit_collapsed(collapsed:&mut HashMap<(Interned, Interned, Interned), Change>, change:Change) {
    let key = (change.e, change.a, change.v);
    match collapsed.entry(key) {
        Entry::Occupied(mut o) => {
            o.get_mut().count += change.count;
        }
        Entry::Vacant(o) => {
            o.insert(change);
        }
    };
}

impl RoundHolder {
    pub fn new() -> RoundHolder {
        let mut rounds = vec![];
        for _ in 0..100 {
            rounds.push(HashMap::new());
        }
        RoundHolder { rounds, output_rounds:vec![], prev_output_rounds:vec![], commits:HashMap::new(), collapsed_commits:HashMap::new(), max_round: 0 }
    }

    pub fn get_output_rounds(&self) -> &Vec<(Round, Count)> {
        match (self.output_rounds.len(), self.prev_output_rounds.len()) {
            (0, _) => &self.prev_output_rounds,
            (_, 0) => &self.output_rounds,
            (_, _) => panic!("neither round array is empty"),
        }
    }

    pub fn compute_output_rounds(&mut self, mut right_iter: DistinctIter) {
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
            let mut left = left_iter.next();
            let mut right = right_iter.next();
            let mut next_left = left_iter.next();
            let mut next_right = right_iter.next();
            move_output_round(&left, &mut left_round, &mut left_count);
            move_output_round(&right, &mut right_round, &mut right_count);
            while left != None || right != None {
                // println!("left: {:?}, right {:?}", left, right);
                if left_round == right_round {
                    if let Some((_, count)) = left {
                        let total = count * right_count;
                        if total != 0 {
                            neue.push((left_round, total));
                        }
                    }
                } else if left_round > right_round {
                    while next_right != None && next_right.unwrap().0 < left_round {
                        right = next_right;
                        next_right = right_iter.next();
                        move_output_round(&right, &mut right_round, &mut right_count);
                    }
                    if let Some((_, count)) = left {
                        let total = count * right_count;
                        if total != 0 {
                            neue.push((left_round, total));
                        }
                    }
                } else {
                    while next_left != None && next_left.unwrap().0 < right_round {
                        left = next_left;
                        next_left = left_iter.next();
                        move_output_round(&left, &mut left_round, &mut left_count);
                    }
                    if let Some((_, count)) = right {
                        let total = count * left_count;
                        if total != 0 {
                            neue.push((right_round, total));
                        }
                    }
                }

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
                    (Some((next_left_count, _)), Some((next_right_count, _))) => {
                        if next_left_count <= next_right_count {
                            left = next_left;
                            next_left = left_iter.next();
                            move_output_round(&left, &mut left_round, &mut left_count);
                        } else {
                            right = next_right;
                            next_right = right_iter.next();
                            move_output_round(&right, &mut right_round, &mut right_count);
                        }
                    }
                }

            }
        }
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
        for info in self.commits.values() {
            match info {
                &(ChangeType::Insert, Change {count, ..}) => {
                    if count > 0 { commit_collapsed(&mut self.collapsed_commits, info.1); }
                }
                &(ChangeType::Remove, Change {count, ..}) => {
                    if count < 0 { commit_collapsed(&mut self.collapsed_commits, info.1); }
                }
            }
        }
        self.commits.clear();
        let mut has_changes = false;
        // @FIXME: There should be some way for us to not have to allocate a vec here
        let drained = { self.collapsed_commits.drain().map(|v| v.1).collect::<Vec<Change>>() };
        for change in drained {
            if change.count != 0 {
                has_changes = true;
                // apply it
                index.distinct(&change, self);
            }
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
        let ref mut cur_changes = self.cur_changes;
        let mut round_ix = self.round_ix;
        let mut change_ix = self.change_ix;
        let max_round = holder.max_round;
        if change_ix >= cur_changes.len() {
            cur_changes.clear();
            change_ix = 0;
            while round_ix <= max_round + 1 && cur_changes.len() == 0 {
                for (_, change) in holder.rounds[round_ix].drain() {
                    cur_changes.push(change);
                }
                round_ix += 1;
            }
        }
        self.change_ix = change_ix + 1;
        self.round_ix = round_ix;
        match cur_changes.get(change_ix) {
            None => None,
            Some(&change) => Some(change.clone()),
        }
    }
}

//-------------------------------------------------------------------------
// Program
//-------------------------------------------------------------------------

pub struct Program {
    rounds: RoundHolder,
    pipe_lookup: HashMap<(Interned,Interned,Interned), Vec<Vec<Instruction>>>,
    block_names: HashMap<String, usize>,
    blocks: Vec<Block>,
    watch_indexes: HashMap<String, WatchIndex>,
    pub index: HashIndex,
    pub interner: Interner,
    iter_pool: EstimateIterPool,
}

impl Program {
    pub fn new() -> Program {
        let index = HashIndex::new();
        let iter_pool = EstimateIterPool::new();
        let mut interner = Interner::new();
        let rounds = RoundHolder::new();
        let block_names = HashMap::new();
        let watch_indexes = HashMap::new();
        let blocks = vec![];
        Program { rounds, interner, pipe_lookup: HashMap::new(), blocks, block_names, watch_indexes, index, iter_pool }
    }

    pub fn clear(&mut self) {
        self.index = HashIndex::new();
    }

    #[allow(dead_code)]
    pub fn exec_query(&mut self, name:&str) -> Vec<Interned> {
        let mut frame = Frame::new();
        // let start_ns = time::precise_time_ns();
        let pipe = self.get_block(name).pipes[0].clone();
        interpret(self, &mut frame, &pipe);
        // frame.counters.total_ns += time::precise_time_ns() - start_ns;
        // println!("counters: {:?}", frame.counters);
        return frame.results;
    }

    pub fn get_block(&self, name:&str) -> &Block {
        let ix = self.block_names.get(name).unwrap();
        &self.blocks[*ix]
    }

    #[allow(dead_code)]
    pub fn raw_insert(&mut self, e:Interned, a:Interned, v:Interned, round:Round, count:Count) {
        self.index.insert_distinct(e,a,v,round,count);
    }

    pub fn register_block(&mut self, mut block:Block) {
        let ix = self.blocks.len();
        for (pipe_ix, ref mut pipe) in block.pipes.iter_mut().enumerate() {
            if let Some(&mut Instruction::StartBlock {ref mut block}) = pipe.get_mut(0) {
                *block = ix;
            } else { panic!("Block where the first instruction is not a start block.") }
            for shape in block.shapes[pipe_ix].iter() {
                let cur = self.pipe_lookup.entry(*shape).or_insert_with(|| vec![]);
                cur.push(pipe.clone());
            }
        }
        self.block_names.insert(block.name.to_string(), ix);
        self.blocks.push(block);
    }

    pub fn insert_block(&mut self, name:&str, code:&str) {
        let mut b = make_block(&mut self.interner, name, code);
        self.register_block(b)
    }

    pub fn block(&mut self, name:&str, code:&str) -> CodeTransaction {
        self.insert_block(name, code);
        let mut txn = CodeTransaction::new();
        txn.exec(self, name, true);
        txn
    }

    pub fn raw_block(&mut self, block:Block) -> CodeTransaction {
        let name = &block.name.to_string();
        self.register_block(block);
        let mut txn = CodeTransaction::new();
        txn.exec(self, name, true);
        txn
    }

    pub fn watch(&mut self, name:&str, resolved:Vec<Interned>, count:Count) {
        let index = self.watch_indexes.entry(name.to_string()).or_insert_with(|| WatchIndex::new());
        index.insert(resolved, count);
    }

    pub fn get_pipes(&self, input: Change, pipes: &mut Vec<Vec<Instruction>>) {
        // @TODO @FIXME: the clones here are just a work around for the borrow checker
        // they are not necessary, and I imagine pretty slow :(
        let ref pipe_lookup = self.pipe_lookup;
        let mut tuple = (0,0,0);
        // look for (0,0,0), (0, a, 0) and (0, a, v) pipes
        match pipe_lookup.get(&tuple) {
            Some(found) => {
                for pipe in found.iter() {
                    pipes.push(pipe.clone());
                }
            },
            None => {},
        }
        tuple.1 = input.a;
        match pipe_lookup.get(&tuple) {
            Some(found) => {
                for pipe in found.iter() {
                    pipes.push(pipe.clone());
                }
            },
            None => {},
        }
        tuple.2 = input.v;
        match pipe_lookup.get(&tuple) {
            Some(found) => {
                for pipe in found.iter() {
                    pipes.push(pipe.clone());
                }
            },
            None => {},
        }
        // lookup the tags for this e
        //  for each tag, lookup (e, a, 0) and (e, a, v)
        if let Some(tags) = self.index.get(input.e, TAG_INTERNED_ID, 0) {
            for tag in tags {
                tuple.0 = tag;
                tuple.2 = 0;
                match pipe_lookup.get(&tuple) {
                    Some(found) => {
                        for pipe in found.iter() {
                            pipes.push(pipe.clone());
                        }
                    },
                    None => {},
                }
                tuple.2 = input.v;
                match pipe_lookup.get(&tuple) {
                    Some(found) => {
                        for pipe in found.iter() {
                            pipes.push(pipe.clone());
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

pub struct Transaction {
    changes: Vec<Change>,
    frame: Frame,
}

impl Transaction {
    pub fn new() -> Transaction {
        let frame = Frame::new();
        Transaction { changes: vec![], frame}
    }

    pub fn input(&mut self, e:Interned, a:Interned, v:Interned, count: Count) {
        let change = Change { e,a,v,n: 0, transaction:0, round:0, count };
        self.changes.push(change);
    }

    pub fn exec(&mut self, program: &mut Program) {
        {
            let ref mut rounds = program.rounds;

            for change in self.changes.iter() {
                program.index.distinct(&change, rounds);
            }
        }

        let ref mut frame = self.frame;
        let mut pipes = vec![];
        let mut next_frame = true;

        while next_frame {
            let mut items = program.rounds.iter();
            while let Some(change) = items.next(&mut program.rounds) {
                // println!("Change {:?}", change);
                pipes.clear();
                program.get_pipes(change, &mut pipes);
                frame.reset();
                frame.input = Some(change);
                for pipe in pipes.iter() {
                    interpret(program, frame, pipe);
                }
                if change.count > 0 {
                    program.index.insert(change.e, change.a, change.v);
                } else {
                    program.index.remove(change.e, change.a, change.v);
                }
            }

            next_frame = program.rounds.prepare_commits(&mut program.index);
        }

        for (name, index) in program.watch_indexes.iter_mut() {
            let diff = index.reconcile();
            println!("DIFF {} {:?}", name, diff);
        }
    }

    pub fn clear(&mut self) {
        self.changes.clear();
    }
}

//-------------------------------------------------------------------------
// Code Transaction
//-------------------------------------------------------------------------

pub struct CodeTransaction {
    changes: Vec<Change>,
    frame: Frame,
}

impl CodeTransaction {
    pub fn new() -> CodeTransaction {
        let frame = Frame::new();
        CodeTransaction { changes: vec![], frame}
    }

    pub fn exec(&mut self, program: &mut Program, block_name: &str, insert:bool) {
        {
            let ref mut rounds = program.rounds;

            for change in self.changes.iter() {
                program.index.distinct(&change, rounds);
            }
        }

        let ref mut frame = self.frame;

        {
            let pipe = program.get_block(block_name).pipes[0].clone();
            // run the block
            frame.input = Some(Change { e:0,a:0,v:0,n: 0, transaction:0, round:0, count: if insert { 1 } else { -1 } });
            interpret(program, frame, &pipe);
        }

        let mut pipes = vec![];
        let mut items = program.rounds.iter();
        let mut next_frame = true;

        while next_frame {
            let mut items = program.rounds.iter();
            while let Some(change) = items.next(&mut program.rounds) {
                println!("{}", change.print(program));
                pipes.clear();
                program.get_pipes(change, &mut pipes);
                frame.reset();
                frame.input = Some(change);
                for pipe in pipes.iter() {
                    interpret(program, frame, pipe);
                }
                if change.count > 0 {
                    program.index.insert(change.e, change.a, change.v);
                } else {
                    program.index.remove(change.e, change.a, change.v);
                }
            }

            next_frame = program.rounds.prepare_commits(&mut program.index);
        }

        for (name, index) in program.watch_indexes.iter_mut() {
            if index.dirty() {
                let diff = index.reconcile();
                println!("DIFF {} {:?}", name, diff);
            }
        }
    }
}
