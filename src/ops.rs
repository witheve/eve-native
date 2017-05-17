//-------------------------------------------------------------------------
// Ops
//-------------------------------------------------------------------------

// TODO:
//  - index insert
//  - functions

extern crate time;

use indexes::{HashIndex, DistinctIter, HashIndexIter};
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

//-------------------------------------------------------------------------
// Change
//-------------------------------------------------------------------------

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
}

//-------------------------------------------------------------------------
// Block
//-------------------------------------------------------------------------

pub struct Block {
    pub name: String,
    pub constraints: Vec<Constraint>,
    pub pipes: Vec<Vec<Instruction>>,
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

#[derive(Debug, Copy, Clone)]
pub enum Instruction {
    StartBlock { block: usize },
    GetIterator {iterator: u32, bail: i32, constraint: u32},
    IteratorNext {iterator: u32, bail: i32},
    Accept {bail: i32, constraint:u32, iterator:u32},
    MoveInputField { from:u32, to:u32, },
    ClearRounds,
    GetRounds {bail: i32, constraint: u32},
    Output {next: i32, constraint:u32},
    Project {next: i32, from:u32},
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
                println!("DOING FUNC {:?}", result);
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
pub fn output(program: &mut Program, frame: &mut Frame, constraint:u32, next:i32) -> i32 {
    let cur = &program.blocks[frame.block_ix].constraints[constraint as usize];
    match cur {
        &Constraint::Insert {ref e, ref a, ref v} => {
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
pub fn project(_: &mut Program, frame: &mut Frame, from:u32, next:i32) -> i32 {
    let value = frame.get_register(from);
    frame.results.push(value);
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
}

pub struct Interner {
    id_to_value: HashMap<Internable, Interned>,
    value_to_id: Vec<Internable>,
    next_id: Interned,
}

impl Interner {
    pub fn new() -> Interner {
        Interner {id_to_value: HashMap::new(), value_to_id:vec![Internable::Null], next_id:1}
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
    Insert {e: Field, a: Field, v:Field},
    Remove {e: Field, a: Field, v:Field},
    RemoveAttribute {e: Field, a: Field},
    RemoveEntity {e: Field, a: Field},
    Project {registers: Vec<usize>},
}

impl fmt::Debug for Constraint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Constraint::Scan { e, a, v, .. } => { write!(f, "Scan ( {:?}, {:?}, {:?} )", e, a, v) }
            &Constraint::Insert { e, a, v, .. } => { write!(f, "Insert ( {:?}, {:?}, {:?} )", e, a, v) }
            &Constraint::Function { ref op, ref params, ref output, .. } => { write!(f, "{:?} = {}({:?})", output, op, params) }
            &Constraint::Project { ref registers } => { write!(f, "Project {:?}", registers) }
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
            Instruction::Output { constraint, next } => {
                output(program, frame, constraint, next)
            },
            Instruction::Project { from, next } => {
                project(program, frame, from, next)
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
        RoundHolder { rounds, output_rounds:vec![], prev_output_rounds:vec![], max_round: 0 }
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
    pub blocks: Vec<Block>,
    pub index: HashIndex,
    pub interner: Interner,
    iter_pool: EstimateIterPool,
    tag_id: Interned,
}

impl Program {
    pub fn new() -> Program {
        let index = HashIndex::new();
        let iter_pool = EstimateIterPool::new();
        let mut interner = Interner::new();
        let tag_id = interner.string_id("tag");
        let rounds = RoundHolder::new();
        let blocks = vec![];
        Program { rounds, interner, pipe_lookup: HashMap::new(), blocks, index, tag_id, iter_pool }
    }

    #[allow(dead_code)]
    pub fn exec_query(&mut self) -> Vec<Interned> {
        let mut frame = Frame::new();
        // let start_ns = time::precise_time_ns();
        let pipe = self.blocks[0].pipes[0].clone();
        interpret(self, &mut frame, &pipe);
        // frame.counters.total_ns += time::precise_time_ns() - start_ns;
        println!("counters: {:?}", frame.counters);
        return frame.results;
    }

    #[allow(dead_code)]
    pub fn raw_insert(&mut self, e:Interned, a:Interned, v:Interned, round:Round, count:Count) {
        self.index.insert_distinct(e,a,v,round,count);
    }

    pub fn register_block(&mut self, mut block:Block) {
        let ix = self.blocks.len();
        self.gen_pipes(&mut block, ix);
        self.blocks.push(block);
    }

    pub fn block(&mut self, name:&str, code:&str) {
        let mut b = make_block(&mut self.interner, name, code);
        self.register_block(b);
    }

    pub fn gen_pipes(&mut self, block: &mut Block, block_ix: usize) {

        // for each scan we need a new pipe
        //   a block instruction
        //   move_input_fields instructions
        //   for each scan / function that is not the root of this pipe, an accept
        //   for each variable not solved by the input,
        //     for each scan in the pipe, we need a get_iter
        //     for each function in the pipe we need a get_iter
        //     an iter_next
        //     for each scan in the pipe, we need an accept
        //     for each function in the pipe we need an accept
        //   a clear_rounds
        //   for each scan in the pipe, we need a get_rounds
        //   for each insert in the pipe, we need an output
        //
        //

        const NO_INPUTS_PIPE:u32 = 1000000;
        let mut moves:HashMap<u32, Vec<Instruction>> = HashMap::new();
        let mut scans = vec![NO_INPUTS_PIPE];
        let mut get_iters = vec![];
        let mut accepts = vec![];
        let mut get_rounds = vec![];
        let mut outputs = vec![];
        let mut project_constraints = vec![];
        let mut registers = 0;
        for (ix_usize, constraint) in block.constraints.iter().enumerate() {
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
                &Constraint::Insert {..} => {
                    outputs.push(Instruction::Output { next: 1, constraint: ix });
                },
                &Constraint::Remove {..} => {
                    outputs.push(Instruction::Output { next: 1, constraint: ix });
                },
                &Constraint::RemoveAttribute {..} => {
                    outputs.push(Instruction::Output { next: 1, constraint: ix });
                },
                &Constraint::RemoveEntity {..} => {
                    outputs.push(Instruction::Output { next: 1, constraint: ix });
                },
                &Constraint::Project {..} => {
                    project_constraints.push(constraint);
                }
            }
        };

        // println!("registers: {:?}", registers);

        let mut pipes = vec![];
        const PIPE_FINISHED:i32 = 1000000;
        let outputs_len = outputs.len();
        for scan_ix in &scans {
            let mut to_solve = registers;
            let mut pipe = vec![Instruction::StartBlock { block: block_ix }];
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
                    if let Instruction::Output {ref mut next, ..} = neue {
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

            pipes.push(pipe);
        };

        for pipe in pipes.iter() {
            block.pipes.push(pipe.clone());
            // println!("\npipe: [");
            // for inst in pipe {
            //     println!("  {:?}", inst);
            // }
            // println!("]");
        }

        let shapes_per_pipe = self.to_shapes(scans.iter().skip(1).map(|scan_ix| &block.constraints[*scan_ix as usize]).collect::<Vec<&Constraint>>());
        let pipe_iter = pipes.iter().skip(1);
        for (shapes, pipe) in shapes_per_pipe.iter().zip(pipe_iter) {
            for shape in shapes {
                let cur = self.pipe_lookup.entry(*shape).or_insert_with(|| vec![]);
                cur.push(pipe.clone());
            }
        }
        // println!("shapes: {:?}", shapes_per_pipe);
    }

    pub fn to_shapes(&mut self, scans: Vec<&Constraint>) -> Vec<Vec<(Interned, Interned, Interned)>> {
        let mut shapes = vec![];
        let tag = self.tag_id;
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
        if let Some(tags) = self.index.get(input.e, self.tag_id, 0) {
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
        let mut items = program.rounds.iter();
        while let Some(change) = items.next(&mut program.rounds) {
            println!("Change {:?}", change);
            pipes.clear();
            program.get_pipes(change, &mut pipes);
            frame.reset();
            frame.input = Some(change);
            for pipe in pipes.iter() {
                interpret(program, frame, pipe);
            }
            program.index.insert(change.e, change.a, change.v);
        }
    }

    pub fn clear(&mut self) {
        self.changes.clear();
    }
}

//-------------------------------------------------------------------------
// Tests
//-------------------------------------------------------------------------

pub fn doit() {
    // prog.block("simple block", ({find, record, lib}) => {
    //  let person = find("person");
    //  let text = `name: ${person.name}`;
    //  return [
    //    record("html/div", {person, text})
    //  ]
    // });
    //
    let mut program = Program::new();
    let constraints = vec![
        make_scan(register(0), program.interner.string("tag"), program.interner.string("person")),
        make_scan(register(0), program.interner.string("name"), register(1)),
        make_function("concat", vec![program.interner.string("name: "), register(1)], register(2)),
        make_function("gen_id", vec![register(0), register(2)], register(3)),
        Constraint::Insert {e: register(3), a: program.interner.string("tag"), v: program.interner.string("html/div")},
        Constraint::Insert {e: register(3), a: program.interner.string("person"), v: register(0)},
        Constraint::Insert {e: register(3), a: program.interner.string("text"), v: register(2)},
        // Constraint::Insert {e: program.interner.string("foo"), a: program.interner.string("tag"), v: program.interner.string("html/div")},
        // Constraint::Insert {e: program.interner.string("foo"), a: program.interner.string("person"), v: register(0)},
        // Constraint::Insert {e: program.interner.string("foo"), a: program.interner.string("text"), v: register(1)},
    ];
    program.register_block(Block { name: "simple block".to_string(), constraints, pipes: vec![] });
    let start = time::precise_time_ns();
    let mut txn = Transaction::new();
    for ix in 0..100000 {
        txn.clear();
        txn.input(program.interner.number_id(ix as f32), program.interner.string_id("tag"), program.interner.string_id("person"), 1);
        txn.input(program.interner.number_id(ix as f32), program.interner.string_id("name"), program.interner.number_id(ix as f32), 1);
        txn.exec(&mut program);
    }
    let end = time::precise_time_ns();
    println!("TOOK {:?}", (end - start) as f64 / 1_000_000.0);
}


pub fn doit_blah() {
    let mut program = Program::new();
    let constraints = vec![
        make_scan(register(0), program.interner.string("tag"), program.interner.string("person")),
        make_scan(register(0), program.interner.string("age"), register(1)),
        make_filter(">", register(1), program.interner.number(60.0)),
        make_function("+", vec![register(1), program.interner.number(10.0)], register(2)),
        Constraint::Insert {e: register(0), a: program.interner.string("adjsted-age"), v: register(2)},
    ];
    program.register_block(Block { name: "simple block".to_string(), constraints, pipes: vec![] });
    let start = time::precise_time_ns();
    let mut txn = Transaction::new();
    for ix in 0..1 {
        txn.clear();
        txn.input(program.interner.number_id(ix as f32), program.interner.string_id("tag"), program.interner.string_id("person"), 1);
        txn.input(program.interner.number_id(ix as f32), program.interner.string_id("age"), program.interner.number_id((ix % 100) as f32), 1);
        txn.exec(&mut program);
    }
    let end = time::precise_time_ns();
    println!("TOOK {:?}", (end - start) as f64 / 1_000_000.0);
}


