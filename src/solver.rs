use ops::*;
use compiler::{FunctionKind};
use indexes::{WatchIndex, RemoteChangeField};
use std::collections::{HashSet};
use std::hash::{Hash, Hasher};
use std::usize;
use std::iter;
use std::sync::Arc;
use std::fmt;

pub type OutputFunc = fn(&Solver, &mut RuntimeState, &mut Frame);
pub type AcceptFunc = Fn(&mut RuntimeState, &mut Frame, usize) -> bool;
pub type GetIteratorFunc = Fn(&mut EstimateIter, &mut RuntimeState, &mut Frame) -> bool;
pub type GetRoundsFunc = Fn(&mut RuntimeState, &mut Frame);

//-------------------------------------------------------------------------
// Input Fields
//-------------------------------------------------------------------------

#[derive(Eq, Hash, PartialEq, Copy, Clone)]
pub enum InputField {
    Transaction,
    Round,
    Type,
    Count,
}

//-------------------------------------------------------------------------
// OutputFuncs
//-------------------------------------------------------------------------

#[derive(Eq, Hash, PartialEq, Copy, Clone)]
pub enum OutputFuncs {
    Bind,
    Commit,
    DynamicCommit,
    Aggregate,
    Intermediate,
    Project,
    Watch,
}

//-------------------------------------------------------------------------
// Solve Variable
//-------------------------------------------------------------------------

pub struct Solver {
    pub block: Interned,
    pub id: usize,
    outputs: Vec<OutputFunc>,
    get_iters: Vec<Arc<GetIteratorFunc>>,
    accepts: Vec<Arc<AcceptFunc>>,
    get_rounds: Vec<Arc<GetRoundsFunc>>,
    finished_mask: u64,
    moves: Vec<(usize, usize)>,
    input_checks: Vec<(InputField, Interned)>,
    commits: Vec<(Field, Field, Field, ChangeType)>,
    dynamic_commits: Vec<(Field, Field, Field, Field)>,
    binds: Vec<(Field, Field, Field)>,
    watch_registers: Vec<(String, Vec<Field>)>,
    project_fields: Vec<usize>,
    intermediates: Vec<(Vec<Field>, Vec<Field>, bool)>,
    intermediate_accepts: Vec<(usize, Interned)>,
    aggregates: Vec<(Vec<Field>, Vec<Field>, Vec<Field>, Vec<Field>, AggregateFunction, AggregateFunction, FunctionKind)>,
    interned_remove: Interned,
}

impl Hash for Solver {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.block.hash(state);
        self.id.hash(state);
    }
}

impl PartialEq for Solver {
    fn eq(&self, other:&Self) -> bool {
        self.block == other.block && self.id == other.id
    }
}

impl Eq for Solver {}

unsafe impl Send for Solver {}
impl Clone for Solver {
    fn clone(&self) -> Self {
        Solver {
            block: self.block,
            id: self.id,
            moves: self.moves.clone(),
            input_checks: self.input_checks.clone(),
            get_iters: self.get_iters.iter().cloned().collect(),
            accepts: self.accepts.iter().cloned().collect(),
            get_rounds: self.get_rounds.iter().cloned().collect(),
            commits: self.commits.clone(),
            dynamic_commits: self.dynamic_commits.clone(),
            binds: self.binds.clone(),
            intermediates: self.intermediates.clone(),
            intermediate_accepts: self.intermediate_accepts.clone(),
            outputs: self.outputs.iter().map(|x| *x).collect(),
            watch_registers: self.watch_registers.clone(),
            project_fields: self.project_fields.clone(),
            aggregates: self.aggregates.iter().map(|&(ref a, ref b, ref c, ref d,e,f,g)| (a.clone(), b.clone(), c.clone(), d.clone(), e, f, g)).collect(),
            finished_mask: self.finished_mask,
            interned_remove: self.interned_remove,
        }
    }
}

impl fmt::Debug for Solver {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Solver")
    }
}

fn move_or_accept(field:&Field, ix:usize, moves:&mut Vec<(usize, usize)>, accepts:&mut Vec<(usize, Interned)>) {
    if let &Field::Register(reg) = field {
        moves.push((ix, reg));
    } else if let &Field::Value(val) = field {
        accepts.push((ix, val));
    }
}

impl Solver {

    pub fn new(interner:&mut Interner, block:Interned, id:usize, active_scan:Option<&Constraint>, constraints:&Vec<Constraint>) -> Solver {
        let mut moves = vec![];
        let mut input_checks = vec![];
        let mut get_iters = vec![];
        let mut accepts = vec![];
        let mut get_rounds = vec![];
        let mut commits = vec![];
        let mut dynamic_commits = vec![];
        let mut binds = vec![];
        let mut watch_registers = vec![];
        let mut project_fields:Vec<usize> = vec![];
        let mut intermediates = vec![];
        let mut aggregates = vec![];
        let mut intermediate_accepts = vec![];

        let mut output_funcs = HashSet::new();
        let mut to_solve = HashSet::new();

        match active_scan {
            Some(&Constraint::Scan { e, a, v, .. }) => {
                to_solve.extend(active_scan.unwrap().get_registers());
                if let Field::Register(ix) = e { moves.push((0, ix)); }
                if let Field::Register(ix) = a { moves.push((1, ix)); }
                if let Field::Register(ix) = v { moves.push((2, ix)); }
            },
            Some(&Constraint::LookupCommit { e, a, v, .. }) => {
                to_solve.extend(active_scan.unwrap().get_registers());
                if let Field::Register(ix) = e { moves.push((0, ix)); }
                if let Field::Register(ix) = a { moves.push((1, ix)); }
                if let Field::Register(ix) = v { moves.push((2, ix)); }
                input_checks.push((InputField::Round, 0));
                get_rounds.push(make_commit_lookup_get_rounds(active_scan.unwrap()));
            },
            Some(&Constraint::LookupRemote { e, a, v, _for, _type, from, to, .. }) => {
                to_solve.extend(active_scan.unwrap().get_registers());
                move_or_accept(&e, 0, &mut moves, &mut intermediate_accepts);
                move_or_accept(&a, 1, &mut moves, &mut intermediate_accepts);
                move_or_accept(&v, 2, &mut moves, &mut intermediate_accepts);
                move_or_accept(&_for, 3, &mut moves, &mut intermediate_accepts);
                move_or_accept(&_type, 4, &mut moves, &mut intermediate_accepts);
                move_or_accept(&from, 5, &mut moves, &mut intermediate_accepts);
                move_or_accept(&to, 6, &mut moves, &mut intermediate_accepts);
            },
            Some(&Constraint::IntermediateScan { ref full_key, .. }) |
            Some(&Constraint::AntiScan { key: ref full_key, .. }) => {
                to_solve.extend(active_scan.unwrap().get_registers());
                for (field_ix, field) in full_key.iter().enumerate() {
                    if let &Field::Register(offset) = field {
                        moves.push((field_ix, offset));
                    } else if field_ix > 0 {
                        if let &Field::Value(value) = field {
                            intermediate_accepts.push((field_ix, value));
                        }
                    }
                }
            },
            _ => {}
        }

        for (ix, constraint) in constraints.iter().enumerate() {
            to_solve.extend(constraint.get_registers());
            if active_scan.map_or(false, |x| x == constraint) { continue; }

            match constraint {
                &Constraint::Scan {..} => {
                    get_iters.push(make_scan_get_iterator(constraint, ix));
                    accepts.push(make_scan_accept(constraint, ix));
                    get_rounds.push(make_scan_get_rounds(constraint));
                },
                &Constraint::LookupCommit {..} => {
                    get_iters.push(make_scan_get_iterator(constraint, ix));
                    accepts.push(make_scan_accept(constraint, ix));
                    get_rounds.push(make_commit_lookup_get_rounds(constraint));
                },
                &Constraint::LookupRemote {..} => {
                    get_iters.push(make_lookup_remote_get_iterator(constraint, ix));
                },
                &Constraint::AntiScan {..}  => {
                    get_rounds.push(make_anti_get_rounds(constraint));
                }
                &Constraint::IntermediateScan {..} => {
                    get_iters.push(make_intermediate_get_iterator(constraint, ix));
                    accepts.push(make_intermediate_accept(constraint, ix));
                    get_rounds.push(make_intermediate_get_rounds(constraint));
                }
                &Constraint::Function {..} => {
                    get_iters.push(make_function_get_iterator(constraint, ix));
                    accepts.push(make_function_accept(constraint, ix));
                }
                &Constraint::MultiFunction {..} => {
                    get_iters.push(make_multi_get_iterator(constraint, ix));
                }
                &Constraint::Aggregate {ref output_key, ref group, ref projection, ref params, add, remove, kind, ..} => {
                    aggregates.push((group.clone(), projection.clone(), params.clone(), output_key.clone(), add, remove, kind));
                    output_funcs.insert(OutputFuncs::Aggregate);
                }
                &Constraint::Filter {..} => {
                    accepts.push(make_filter_accept(constraint, ix));
                }
                &Constraint::Insert { e,a,v,commit } => {
                    if commit {
                        commits.push((e,a,v, ChangeType::Insert));
                        output_funcs.insert(OutputFuncs::Commit);
                    } else {
                        binds.push((e,a,v));
                        output_funcs.insert(OutputFuncs::Bind);
                    }
                },
                &Constraint::InsertIntermediate { ref key, ref value, negate } => {
                    intermediates.push((key.clone(), value.clone(), negate));
                    output_funcs.insert(OutputFuncs::Intermediate);
                }
                &Constraint::Remove { e,a,v } => {
                    commits.push((e,a,v, ChangeType::Remove));
                    output_funcs.insert(OutputFuncs::Commit);
                },
                &Constraint::RemoveAttribute { e,a } => {
                    commits.push((e,a, Field::Value(0), ChangeType::Remove));
                    output_funcs.insert(OutputFuncs::Commit);
                },
                &Constraint::RemoveEntity { e } => {
                    commits.push((e, Field::Value(0), Field::Value(0), ChangeType::Remove));
                    output_funcs.insert(OutputFuncs::Commit);
                },
                &Constraint::DynamicCommit { e,a,v,_type } => {
                    dynamic_commits.push((e,a,v,_type));
                    output_funcs.insert(OutputFuncs::DynamicCommit);
                },
                &Constraint::Project { ref registers } => {
                    project_fields.extend(registers.iter());
                    output_funcs.insert(OutputFuncs::Project);
                },
                &Constraint::Watch { ref name, ref registers } => {
                    watch_registers.push((name.to_owned(), registers.clone()));
                    output_funcs.insert(OutputFuncs::Watch);
                },
            }
        }

        let finished_mask = 2u64.pow(to_solve.len() as u32) - 1;
        let outputs = output_funcs.iter().map(|&x| {
            match x {
                OutputFuncs::Bind => do_bind as OutputFunc,
                OutputFuncs::Commit => do_commit as OutputFunc,
                OutputFuncs::DynamicCommit => do_dynamic_commit as OutputFunc,
                OutputFuncs::Watch => do_watch as OutputFunc,
                OutputFuncs::Intermediate => do_intermediate_insert as OutputFunc,
                OutputFuncs::Project => do_project as OutputFunc,
                OutputFuncs::Aggregate => do_aggregate as OutputFunc,
            }
        }).collect();

        // Dynamic commits need to compare their type to "add" and "remove", so to reduce the
        // runtime pressure of that, we can get the interned id for remove and just use that to
        // compare.
        let interned_remove = interner.string_id("remove");

        Solver { block, id, moves, input_checks, get_iters, accepts, get_rounds, dynamic_commits, commits, binds, intermediates, intermediate_accepts, outputs, watch_registers, project_fields, aggregates, finished_mask, interned_remove }
    }

    pub fn run(&self, state:&mut RuntimeState, pool:&mut EstimateIterPool, frame:&mut Frame) {
        if !self.do_move(state, frame) { return; }
        if frame.row.solved_fields != self.finished_mask {
            self.solve_variables(state, pool, frame, 0);
        } else {
            self.clear_rounds(&mut state.output_rounds, frame);
            self.do_output(state, frame);
        }
    }

    pub fn run_intermediate(&self, state:&mut RuntimeState, pool:&mut EstimateIterPool, frame:&mut Frame) {
        if !self.do_intermediate_move(frame) { return }
        for accept in self.accepts.iter() {
            let res = (*accept)(state, frame, usize::MAX);
            if !res { return }
        }
        if frame.row.solved_fields != self.finished_mask {
            self.solve_variables(state, pool, frame, 0);
        } else {
            self.clear_rounds(&mut state.output_rounds, frame);
            self.do_output(state, frame);
        }
    }

    pub fn run_remote(&self, state:&mut RuntimeState, pool:&mut EstimateIterPool, frame:&mut Frame) {
        if !self.do_remote_move(frame) { return }
        for accept in self.accepts.iter() {
            let res = (*accept)(state, frame, usize::MAX);
            if !res { return }
        }
        if frame.row.solved_fields != self.finished_mask {
            self.solve_variables(state, pool, frame, 0);
        } else {
            self.clear_rounds(&mut state.output_rounds, frame);
            self.do_output(state, frame);
        }
    }

    pub fn do_move(&self, state: &mut RuntimeState, frame:&mut Frame) -> bool {
        if self.moves.len() > 0 {
            let change = frame.input.expect("running solver without an input!");
            for &(from, to) in self.moves.iter() {
                match from {
                    0 => { frame.row.set_multi(to, change.e); }
                    1 => { frame.row.set_multi(to, change.a); }
                    2 => { frame.row.set_multi(to, change.v); }
                    _ => { unreachable!() },
                }
            }
            for check in self.input_checks.iter() {
                match check {
                    &(InputField::Round, v) => {
                        if change.round != v { return false }
                    }
                    _ => { unimplemented!() },
                }
            }
            for accept in self.accepts.iter() {
                if !(*accept)(state, frame, usize::MAX) { return false }
            }
        }
        true
    }

    pub fn do_intermediate_move(&self, frame:&mut Frame) -> bool {
        if let Some(ref intermediate) = frame.intermediate {
            for &(from, to) in self.moves.iter() {
                frame.row.set_multi(to, intermediate.key[from]);
            }
            for &(from, value) in self.intermediate_accepts.iter() {
                if intermediate.key[from] != value { return false }
            }
        }
        true
    }

    pub fn do_remote_move(&self, frame:&mut Frame) -> bool {
        if let Some(ref remote) = frame.remote {
            for &(from, to) in self.moves.iter() {
                match from {
                    0 => { frame.row.set_multi(to, remote.e); }
                    1 => { frame.row.set_multi(to, remote.a); }
                    2 => { frame.row.set_multi(to, remote.v); }
                    3 => { frame.row.set_multi(to, remote._for); }
                    4 => { frame.row.set_multi(to, remote._type); }
                    5 => { frame.row.set_multi(to, remote.from); }
                    6 => { frame.row.set_multi(to, remote.to); }
                    _ => { unreachable!() },
                }
            }
            for &(from, value) in self.intermediate_accepts.iter() {
                match from {
                    0 => { if remote.e != value { return false } }
                    1 => { if remote.a != value { return false } }
                    2 => { if remote.v != value { return false } }
                    3 => { if remote._for != value { return false } }
                    4 => { if remote._type != value { return false } }
                    5 => { if remote.from != value { return false } }
                    6 => { if remote.to != value { return false } }
                    _ => { unreachable!() },
                }
            }
        }
        true
    }

    pub fn clear_rounds(&self, output_rounds:&mut OutputRounds, frame: &mut Frame) {
        output_rounds.clear();
        if let Some(ref change) = frame.input {
            output_rounds.output_rounds.push((change.round, change.count));
        } else if let Some(ref change) = frame.intermediate {
            let count = if change.negate { change.count * -1 } else { change.count };
            output_rounds.output_rounds.push((change.round, count));
        } else if let Some(_) = frame.remote {
            output_rounds.output_rounds.push((0, 1));
        }
    }

    pub fn solve_variables(&self, state:&mut RuntimeState, pool:&mut EstimateIterPool, frame:&mut Frame, ix:usize) {
        let active_constraint = {
            let iterator = pool.get(ix);
            for func in self.get_iters.iter() {
                if !(*func)(iterator, state, frame) {
                    iterator.reset();
                    return;
                }
            }
            iterator.constraint
        };
        'main: while { pool.get(ix).iter.next(&mut frame.row, ix) } {
            for accept in self.accepts.iter() {
                if !(*accept)(state, frame, active_constraint) {
                    continue 'main;
                }
            }
            frame.row.put_solved(ix);
            if frame.row.solved_fields == self.finished_mask {
                self.clear_rounds(&mut state.output_rounds, frame);
                for get in self.get_rounds.iter() {
                    (*get)(state, frame);
                    if state.output_rounds.get_output_rounds().len() == 0 {
                        continue 'main;
                    }
                }
                self.do_output(state, frame);
            } else {
                self.solve_variables(state, pool, frame, ix + 1);
            }
        }
        let iterator = pool.get(ix);
        if iterator.estimate != 0 && iterator.estimate != usize::MAX {
            frame.row.clear_solved(ix);
            iterator.clear(&mut frame.row, ix);
        }
        // frame.counters.considered += iter.estimate as u64;
        iterator.reset();
    }

    #[inline(always)]
    pub fn do_output(&self, state:&mut RuntimeState, frame:&mut Frame) {
        for output in self.outputs.iter() {
            output(self, state, frame);
        }
    }



}

//-------------------------------------------------------------------------
// Scan
//-------------------------------------------------------------------------

pub fn make_scan_get_iterator(scan:&Constraint, ix: usize) -> Arc<GetIteratorFunc> {
    let (e,a,v,register_mask) = match scan {
        &Constraint::Scan { e, a, v, register_mask} => (e,a,v,register_mask),
        &Constraint::LookupCommit { e, a, v, register_mask} => (e,a,v,register_mask),
        _ => unreachable!()
    };
    Arc::new(move |iter, state, frame| {
        // if we have already solved all of this scan's vars, we just move on
        if check_bits(frame.row.solved_fields, register_mask) {
            return true;
        }

        let resolved_e = frame.resolve(&e);
        let resolved_a = frame.resolve(&a);
        let resolved_v = frame.resolve(&v);

        if state.index.propose(iter, resolved_e, resolved_a, resolved_v) {
            iter.constraint = ix;
            match iter.iter {
                OutputingIter::Single(ref mut output, _) => {
                    *output = match (*output, e, a, v) {
                        (0, Field::Register(reg), _, _) => reg,
                        (1, _, Field::Register(reg), _) => reg,
                        (2, _, _, Field::Register(reg)) => reg,
                        _ => panic!("bad scan output {:?} {:?} {:?} {:?}", output,e,a,v),
                    };
                }
                _ => {}
            }
        }
        true
    })
}

pub fn make_scan_accept(scan:&Constraint, me:usize) -> Arc<AcceptFunc>  {
    let (e,a,v,register_mask) = match scan {
        &Constraint::Scan { e, a, v, register_mask} => (e,a,v,register_mask),
        &Constraint::LookupCommit { e, a, v, register_mask} => (e,a,v,register_mask),
        _ => unreachable!()
    };
    Arc::new(move |state, frame, cur_constraint| {
        // if we aren't solving for something this scan cares about, then we
        // automatically accept it.
        if cur_constraint == me || !has_any_bits(register_mask, frame.row.solving_for) {
            return true;
        }
        let resolved_e = frame.resolve(&e);
        let resolved_a = frame.resolve(&a);
        let resolved_v = frame.resolve(&v);
        state.index.check(resolved_e, resolved_a, resolved_v)
    })
}

pub fn make_scan_get_rounds(scan:&Constraint) -> Arc<GetRoundsFunc> {
    let (e,a,v,_) = match scan {
        &Constraint::Scan { e, a, v, register_mask} => (e,a,v,register_mask),
        _ => unreachable!()
    };
    Arc::new(move |state, frame| {
            let resolved_e = frame.resolve(&e);
            let resolved_a = frame.resolve(&a);
            let resolved_v = frame.resolve(&v);
            let iter = state.distinct_index.iter(resolved_e, resolved_a, resolved_v);
            state.output_rounds.compute_output_rounds(iter);
    })
}

//-------------------------------------------------------------------------
// LookupCommit
//-------------------------------------------------------------------------

pub fn make_commit_lookup_get_rounds(scan:&Constraint) -> Arc<GetRoundsFunc> {
    let (e,a,v,_) = match scan {
        &Constraint::LookupCommit { e, a, v, register_mask} => (e,a,v,register_mask),
        _ => unreachable!()
    };
    Arc::new(move |state, frame| {
            let resolved_e = frame.resolve(&e);
            let resolved_a = frame.resolve(&a);
            let resolved_v = frame.resolve(&v);
            if !state.distinct_index.is_commit(resolved_e, resolved_a, resolved_v) {
                state.output_rounds.clear();
            }
    })
}

//-------------------------------------------------------------------------
// LookupRemote
//-------------------------------------------------------------------------

pub fn make_lookup_remote_get_iterator(scan:&Constraint, ix: usize) -> Arc<GetIteratorFunc> {
    let (e,a,v,_for,_type,from,to,register_mask) = match scan {
        &Constraint::LookupRemote { e, a, v, _for, _type, from, to, register_mask} => (e,a,v,_for,_type,from,to,register_mask),
        _ => unreachable!()
    };
    let mut fields = vec![];
    let mut outputs = vec![];
    if let Field::Register(ix) = e { fields.push(RemoteChangeField::E); outputs.push(ix); }
    if let Field::Register(ix) = a { fields.push(RemoteChangeField::A); outputs.push(ix); }
    if let Field::Register(ix) = v { fields.push(RemoteChangeField::V); outputs.push(ix); }
    if let Field::Register(ix) = _for { fields.push(RemoteChangeField::For); outputs.push(ix); }
    if let Field::Register(ix) = _type { fields.push(RemoteChangeField::Type); outputs.push(ix); }
    if let Field::Register(ix) = from { fields.push(RemoteChangeField::From); outputs.push(ix); }
    if let Field::Register(ix) = to { fields.push(RemoteChangeField::To); outputs.push(ix); }
    Arc::new(move |iter, state, frame| {
        // if we have already solved all of this scan's vars, we just move on
        if check_bits(frame.row.solved_fields, register_mask) {
            return true;
        }

        if iter.is_better(state.remote_index.len()) {
            let resolved_e = frame.resolve(&e);
            let resolved_a = frame.resolve(&a);
            let resolved_v = frame.resolve(&v);
            let resolved_for = frame.resolve(&_for);
            let resolved_type = frame.resolve(&_type);
            let resolved_from = frame.resolve(&from);
            let resolved_to = frame.resolve(&to);
            // @FIXME: why does this need to be collected into a vector first? If the iter is
            // passed directly, it's always empty.
            let remote_iter = state.remote_index.index.iter().filter(|x| {
               (resolved_e == 0 || x.e == resolved_e) &&
               (resolved_a == 0 || x.a == resolved_a) &&
               (resolved_v == 0 || x.v == resolved_v) &&
               (resolved_for == 0 || x._for == resolved_for) &&
               (resolved_type == 0 || x._type == resolved_type) &&
               (resolved_from == 0 || x.from == resolved_from) &&
               (resolved_to == 0 || x.to == resolved_to)
            }).map(|x| {
                x.extract(&fields)
            }).collect::<Vec<_>>();
            iter.estimate = state.remote_index.len();
            iter.iter = OutputingIter::Multi(outputs.clone(), OutputingIter::make_multi_ptr(Box::new(remote_iter.into_iter())));
            iter.constraint = ix;
        }
        true
    })
}

//-------------------------------------------------------------------------
// Filter
//-------------------------------------------------------------------------

pub fn make_filter_accept(scan:&Constraint, me:usize) -> Arc<AcceptFunc>  {
    let (left, right, func, param_mask) = match scan {
        &Constraint::Filter {ref left, ref right, ref func, param_mask, .. } => (left.clone(), right.clone(), *func, param_mask),
        _ => unreachable!()
    };
    Arc::new(move |state, frame, cur_constraint| {
        if cur_constraint == me || !has_any_bits(param_mask, frame.row.solving_for) {
            return true;
        }
        if check_bits(frame.row.solved_fields, param_mask) {
            let resolved_left = state.interner.get_value(frame.resolve(&left));
            let resolved_right = state.interner.get_value(frame.resolve(&right));
            func(resolved_left, resolved_right)
        } else {
            true
        }
    })
}

//-------------------------------------------------------------------------
// Function
//-------------------------------------------------------------------------

pub fn make_function_get_iterator(scan:&Constraint, ix: usize) -> Arc<GetIteratorFunc> {
    let (func, output, params, param_mask, output_mask) = match scan {
        &Constraint::Function {ref func, ref output, ref params, param_mask, output_mask, ..} => (*func, output.clone(), params.clone(), param_mask, output_mask),
        _ => unreachable!()
    };
    Arc::new(move |iter, state, frame| {
        let solved = frame.row.solved_fields;
        if check_bits(solved, param_mask) && !check_bits(solved, output_mask) {
            let result = {
                let mut resolved = vec![];
                for param in params.iter() {
                    resolved.push(state.interner.get_value(frame.resolve(param)));
                }
                func(resolved)
            };
            match result {
                Some(v) => {
                    if iter.is_better(1) {
                        let id = state.interner.internable_to_id(v);
                        let reg = if let Field::Register(reg) = output {
                            reg
                        } else {
                            panic!("Function output is not a register");
                        };
                        iter.constraint = ix;
                        iter.estimate = 1;
                        iter.iter = OutputingIter::Single(reg, OutputingIter::make_ptr(Box::new(iter::once(id))));
                    }
                    true
                }
                _ => false,
            }
        } else {
            true
        }
    })
}

pub fn make_function_accept(scan:&Constraint, me:usize) -> Arc<AcceptFunc>  {
    let (func, output, params, param_mask, output_mask) = match scan {
        &Constraint::Function {ref func, ref output, ref params, param_mask, output_mask, ..} => (*func, output.clone(), params.clone(), param_mask, output_mask),
        _ => unreachable!()
    };
    Arc::new(move |state, frame, cur_constraint| {
            if cur_constraint == me { return true; }
            // We delay actual accept until all but one of our attributes are satisfied. Either:
            // - We have all inputs and solving for output OR,
            // - We have the output and all but one input and solving for the remaining input

            let solved = frame.row.solved_fields;
            let solving_output_with_inputs = check_bits(solved, param_mask) && has_any_bits(frame.row.solving_for, output_mask);
            let solving_input_with_output = check_bits(solved, param_mask | output_mask) && has_any_bits(frame.row.solving_for, param_mask);

            if !solving_output_with_inputs && !solving_input_with_output {
                return true
            }

            let result = {
                let mut resolved = vec![];
                for param in params.iter() {
                    resolved.push(state.interner.get_value(frame.resolve(param)));
                }
                func(resolved)
            };
            match result {
                Some(v) => {
                    let id = state.interner.internable_to_id(v);
                    id == frame.resolve(&output)
                }
                _ => false,
            }
    })
}

//-------------------------------------------------------------------------
// MultiFunction
//-------------------------------------------------------------------------

pub fn make_multi_get_iterator(scan:&Constraint, ix: usize) -> Arc<GetIteratorFunc> {
    let (func, output_fields, params, param_mask, output_mask) = match scan {
        &Constraint::MultiFunction {ref func, outputs:ref output_fields, ref params, param_mask, output_mask, ..} => (*func, output_fields.clone(), params.clone(), param_mask, output_mask),
        _ => unreachable!()
    };
    Arc::new(move |iter, state, frame| {
        let solved = frame.row.solved_fields;
        if check_bits(solved, param_mask) && !check_bits(solved, output_mask) {
            let result = {
                let mut resolved = vec![];
                for param in params.iter() {
                    resolved.push(state.interner.get_value(frame.resolve(param)));
                }
                func(resolved)
            };
            match result {
                Some(mut result_values) => {
                    let estimate = result_values.len();
                    if iter.is_better(estimate) {
                        let outputs = output_fields.iter().map(|x| {
                            if let &Field::Register(reg) = x {
                                reg
                            } else {
                                panic!("Non-register multi-function output")
                            }
                        }).collect();
                        let result_vec = result_values.drain(..).map(|mut row| {
                            row.drain(..).map(|field| state.interner.internable_to_id(field)).collect()
                        }).collect::<Vec<Vec<Interned>>>();
                        iter.constraint = ix;
                        iter.estimate = estimate;
                        iter.iter = OutputingIter::Multi(outputs, OutputingIter::make_multi_ptr(Box::new(result_vec.into_iter())));
                    }
                    true
                }
                _ => false,
            }
        } else {
            true
        }
    })
}

pub fn make_multi_accept(_:&Constraint, _:usize) -> Arc<AcceptFunc>  {
    // let (e,a,v,register_mask) = match scan {
    //     &Constraint::Scan { e, a, v, register_mask} => (e,a,v,register_mask),
    //     _ => unreachable!()
    // };
    Arc::new(move |_, _, _| {
        // FIXME why don't we need this?
        true
    })
}

//-------------------------------------------------------------------------
// IntermediateScan
//-------------------------------------------------------------------------

pub fn make_intermediate_get_iterator(scan:&Constraint, ix: usize) -> Arc<GetIteratorFunc> {
    let (key, value, register_mask, output_mask) = match scan {
        &Constraint::IntermediateScan { ref key, ref value, register_mask, output_mask, .. } => (key.clone(), value.clone(), register_mask, output_mask),
        _ => unreachable!()
    };
    Arc::new(move |mut iter, state, frame| {
        // if we have already solved all of this scan's outputs or we don't have all of our
        // inputs, we just move on
        if !check_bits(frame.row.solved_fields, register_mask) ||
            check_bits(frame.row.solved_fields, output_mask) {
                return true;
            }

        let resolved = key.iter().map(|param| frame.resolve(param)).collect();
        let outputs = value.iter().map(|x| {
            if let &Field::Register(reg) = x {
                reg
            } else {
                panic!("Non-register intermediate scan output")
            }
        }).collect();
        if state.intermediates.propose(&mut iter, resolved, outputs) {
            iter.constraint = ix;
        }
        true
    })
}

pub fn make_intermediate_accept(scan:&Constraint, me:usize) -> Arc<AcceptFunc>  {
    let (key, value, register_mask, output_mask) = match scan {
        &Constraint::IntermediateScan { ref key, ref value, register_mask, output_mask, .. } => (key.clone(), value.clone(), register_mask, output_mask),
        _ => unreachable!()
    };
    Arc::new(move |state, frame, cur_constraint| {
        // if we haven't solved all our inputs and outputs, just skip us
        if cur_constraint == me ||
           !check_bits(frame.row.solved_fields, register_mask) ||
           !check_bits(frame.row.solved_fields, output_mask) {
                return true;
            }

        let resolved = key.iter().map(|param| frame.resolve(param)).collect();
        let resolved_value = value.iter().map(|param| frame.resolve(param)).collect();

        state.intermediates.check(&resolved, &resolved_value)
    })
}

pub fn make_intermediate_get_rounds(scan:&Constraint) -> Arc<GetRoundsFunc> {
    let (key, value) = match scan {
        &Constraint::IntermediateScan { ref key, ref value, .. } => (key.clone(), value.clone()),
        _ => unreachable!()
    };
    Arc::new(move |state, frame| {
        let resolved:Vec<Interned> = key.iter().map(|v| frame.resolve(v)).collect();
        let resolved_value:Vec<Interned> = value.iter().map(|v| frame.resolve(v)).collect();
        state.output_rounds.compute_output_rounds(state.intermediates.distinct_iter(&resolved, &resolved_value));
    })
}

//-------------------------------------------------------------------------
// AntiScan
//-------------------------------------------------------------------------

pub fn make_anti_get_rounds(scan:&Constraint) -> Arc<GetRoundsFunc> {
    let key = match scan {
        &Constraint::AntiScan { ref key, .. } => key.clone(),
        _ => unreachable!()
    };
    Arc::new(move |state, frame| {
        let resolved:Vec<Interned> = key.iter().map(|v| frame.resolve(v)).collect();
        state.output_rounds.compute_anti_output_rounds(state.intermediates.distinct_iter(&resolved, &vec![]));
    })
}

//-------------------------------------------------------------------------
// Outputs
//-------------------------------------------------------------------------

pub fn do_bind(me: &Solver, state:&mut RuntimeState, frame: &mut Frame) {
    for &(round, count) in state.output_rounds.get_output_rounds().iter() {
        for &(e, a, v) in me.binds.iter() {
            let output = Change { e: frame.resolve(&e), a: frame.resolve(&a), v:frame.resolve(&v), n: 0, round: round + 1, transaction: 0, count, };
            frame.counters.inserts += 1;
            state.distinct_index.distinct(&output, &mut state.rounds);
        }
    }
}

pub fn do_commit(me: &Solver, state: &mut RuntimeState, frame: &mut Frame) {
    let n = (me.block * 10000) as u32;
    for &(_, count) in state.output_rounds.get_output_rounds().iter() {
        for &(e, a, v, change_type) in me.commits.iter() {
            let correct_count = if change_type == ChangeType::Remove { count * -1 } else { count };
            let output = Change { e: frame.resolve(&e), a: frame.resolve(&a), v:frame.resolve(&v), n, round:0, transaction: 0, count:correct_count };
            frame.counters.inserts += 1;
            state.rounds.commit(output, change_type)
        }
    }
}

pub fn do_dynamic_commit(me: &Solver, state: &mut RuntimeState, frame: &mut Frame) {
    let n = (me.block * 10000) as u32;
    for &(_, count) in state.output_rounds.get_output_rounds().iter() {
        for &(e, a, v, _type) in me.dynamic_commits.iter() {
            let (correct_count, change_type) = if frame.resolve(&_type) == me.interned_remove { (count * -1, ChangeType::Remove) } else { (count, ChangeType::Insert) };
            let output = Change { e: frame.resolve(&e), a: frame.resolve(&a), v:frame.resolve(&v), n, round:0, transaction: 0, count:correct_count };
            frame.counters.inserts += 1;
            state.rounds.commit(output, change_type)
        }
    }
}

pub fn do_project(me: &Solver, _:&mut RuntimeState, frame: &mut Frame) {
    for from in me.project_fields.iter().cloned() {
        let value = frame.get_register(from);
        frame.results.push(value);
    }
}

pub fn do_intermediate_insert(me: &Solver, state: &mut RuntimeState, frame: &mut Frame) {
    for &(ref key, ref value, negate) in me.intermediates.iter() {
        let resolved:Vec<Interned> = key.iter().map(|v| frame.resolve(v)).collect();
        let resolved_value:Vec<Interned> = value.iter().map(|v| frame.resolve(v)).collect();
        let mut full_key = resolved.clone();
        full_key.extend(resolved_value.iter());
        for &(round, count) in state.output_rounds.get_output_rounds().iter() {
            frame.counters.inserts += 1;
            state.intermediates.distinct(full_key.clone(), resolved.clone(), resolved_value.clone(), round, count, negate);
        }
    }
}

pub fn do_aggregate(me: &Solver, state: &mut RuntimeState, frame: &mut Frame) {
    for &(ref group, ref projection, ref params, ref output_key, add, remove, kind) in me.aggregates.iter() {
        let resolved_group:Vec<Interned> = group.iter().map(|v| frame.resolve(v)).collect();
        let resolved_projection = if kind == FunctionKind::Sort || kind == FunctionKind::NeedleSort {
            projection.iter().map(|v| state.interner.get_value(frame.resolve(v)).clone()).collect()
        } else {
            vec![]
        };
        let resolved_params:Vec<Internable> = { params.iter().map(|v| state.interner.get_value(frame.resolve(v)).clone()).collect() };
        let resolved_output:Vec<Interned> = output_key.iter().map(|v| frame.resolve(v)).collect();
        for &(round, count) in state.output_rounds.get_output_rounds().iter() {
            let action = if count < 0 { remove } else { add };
            frame.counters.inserts += 1;
            state.intermediates.aggregate(&mut state.interner, resolved_group.clone(), resolved_projection.clone(), resolved_params.clone(), round, count, action, resolved_output.clone(), kind);
        }
    }
}

pub fn do_watch(me: &Solver, state: &mut RuntimeState, frame: &mut Frame) {
    for &(ref name, ref registers) in me.watch_registers.iter() {
        let resolved:Vec<Interned> = registers.iter().map(|x| frame.resolve(x)).collect();
        let mut total = 0;
        for &(_, count) in state.output_rounds.get_output_rounds().iter() {
            total += count;
        }
        frame.counters.inserts += 1;
        let index = state.watch_indexes.entry(name.to_string()).or_insert_with(|| WatchIndex::new());
        index.insert(resolved, total);
    }
}
