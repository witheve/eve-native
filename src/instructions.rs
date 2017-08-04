use ops::*;
use indexes::{DistinctIndex, HashIndex, IntermediateIndex, WatchIndex};
use std::collections::{HashSet, HashMap};
use std::usize;
use std::iter;

pub type OutputFunc = Fn(&mut DistinctIndex, &OutputRounds, &mut RoundHolder, &mut Frame);
pub type AcceptFunc = Fn(&mut Interner, &HashIndex, &IntermediateIndex, &mut Frame, usize) -> bool;
pub type GetIteratorFunc = Fn(&mut Interner, &mut EstimateIter, &HashIndex, &IntermediateIndex, &mut Frame) -> bool;
pub type GetRoundsFunc = Fn(&DistinctIndex, &mut OutputRounds, &mut IntermediateIndex, &mut Frame);

//-------------------------------------------------------------------------
// Solve Variable
//-------------------------------------------------------------------------

pub struct Solver {
    block: usize,
    get_iters: Vec<Box<GetIteratorFunc>>,
    accepts: Vec<Box<AcceptFunc>>,
    get_rounds: Vec<Box<GetRoundsFunc>>,
    finished_mask: u64,
    moves: Vec<(usize, usize)>,
    commits: Vec<(Field, Field, Field, ChangeType)>,
    binds: Vec<(Field, Field, Field)>,
    watch_registers: Vec<(String, Vec<Field>)>,
    project_fields: Vec<usize>,
    intermediates: Vec<(Vec<Field>, Vec<Field>, bool)>,
    aggregates: Vec<(Vec<Field>, Vec<Field>, Vec<Field>, AggregateFunction, AggregateFunction)>,
}

impl Solver {

    pub fn new(block:usize, active_scan:Option<&Constraint>, constraints:&Vec<Constraint>) -> Solver {
        let mut moves = vec![];
        let mut get_iters = vec![];
        let mut accepts = vec![];
        let mut get_rounds = vec![];
        let mut commits = vec![];
        let mut binds = vec![];
        let mut watch_registers = vec![];
        let mut project_fields = vec![];
        let mut intermediates = vec![];
        let mut aggregates = vec![];

        let mut to_solve = HashSet::new();

        match active_scan {
            Some(&Constraint::Scan { e, a, v, .. }) => {
                to_solve.extend(active_scan.unwrap().get_registers());
                if let Field::Register(ix) = e { moves.push((0, ix)); }
                if let Field::Register(ix) = a { moves.push((1, ix)); }
                if let Field::Register(ix) = v { moves.push((2, ix)); }
            }
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
                &Constraint::AntiScan { ref key, ..}  => {
                    get_rounds.push(make_anti_get_rounds(constraint));
                }
                &Constraint::IntermediateScan { ref full_key, ..} => {
                    get_iters.push(make_intermediate_get_iterator(constraint, ix));
                    accepts.push(make_intermediate_accept(constraint, ix));
                    get_rounds.push(make_intermediate_get_rounds(constraint));
                }
                &Constraint::Function {ref op, ref output, ref params, ..} => {
                    get_iters.push(make_function_get_iterator(constraint, ix));
                    accepts.push(make_function_accept(constraint, ix));
                }
                &Constraint::MultiFunction {ref op, ref outputs, ref params, ..} => {
                    get_iters.push(make_multi_get_iterator(constraint, ix));
                }
                &Constraint::Aggregate {ref output_key, ref group, ref params, add, remove, ..} => {
                    aggregates.push((group.clone(), params.clone(), output_key.clone(), add, remove));
                }
                &Constraint::Filter {ref op, ref left, ref right, ..} => {
                    accepts.push(make_filter_accept(constraint, ix));
                }
                &Constraint::Insert { e,a,v,commit } => {
                    if commit {
                        commits.push((e,a,v, ChangeType::Insert));
                    } else {
                        binds.push((e,a,v));
                    }
                },
                &Constraint::InsertIntermediate { ref key, ref value, negate } => {
                    intermediates.push((key.clone(), value.clone(), negate));
                }
                &Constraint::Remove { e,a,v } => {
                    commits.push((e,a,v, ChangeType::Remove));
                },
                &Constraint::RemoveAttribute { e,a } => {
                    commits.push((e,a, Field::Value(0), ChangeType::Remove));
                },
                &Constraint::RemoveEntity { e } => {
                    commits.push((e, Field::Value(0), Field::Value(0), ChangeType::Remove));
                },
                &Constraint::Project { ref registers } => {
                    project_fields.extend(registers.iter());
                },
                &Constraint::Watch { ref name, ref registers } => {
                    watch_registers.push((name.to_owned(), registers.clone()));
                },
            }
        }

        let finished_mask = 2u64.pow(to_solve.len() as u32) - 1;
        Solver { block, moves, get_iters, accepts, get_rounds, commits, binds, intermediates, watch_registers, project_fields, aggregates, finished_mask }
    }

    pub fn run(&self, state:&mut RuntimeState, pool:&mut EstimateIterPool, frame:&mut Frame) {
        self.do_move(frame);
        if self.get_iters.len() > 0 {
            self.solve_variables(state, pool, frame, 0);
        } else {
            self.clear_rounds(&mut state.output_rounds, frame);
            self.do_output(state, frame);
        }
    }

    pub fn do_move(&self, frame:&mut Frame) {
        let change = frame.input.expect("running solver without an input!");
        for &(from, to) in self.moves.iter() {
            match from {
                0 => { frame.row.set_multi(to, change.e); }
                1 => { frame.row.set_multi(to, change.a); }
                2 => { frame.row.set_multi(to, change.v); }
                _ => { unreachable!() },
            }
        }
    }

    pub fn clear_rounds(&self, output_rounds:&mut OutputRounds, frame: &mut Frame) {
        output_rounds.clear();
        if let Some(ref change) = frame.input {
            output_rounds.output_rounds.push((change.round, change.count));
        } else if let Some(ref change) = frame.intermediate {
            let count = if change.negate { change.count * -1 } else { change.count };
            output_rounds.output_rounds.push((change.round, count));
        }
    }

    pub fn solve_variables(&self, state:&mut RuntimeState, pool:&mut EstimateIterPool, frame:&mut Frame, ix:usize) {
        let active_constraint = 10000;
        {
            let iterator = pool.get(ix);
            for func in self.get_iters.iter() {
                if !(*func)(&mut state.interner, iterator, &state.index, &state.intermediates, frame) {
                    iterator.reset();
                    return;
                }
            }
            iterator.constraint
        };
        'main: while { pool.get(ix).iter.next(&mut frame.row, ix) } {
            for accept in self.accepts.iter() {
                if !(*accept)(&mut state.interner, &state.index, &state.intermediates, frame, active_constraint) { continue 'main; }
            }
            frame.row.put_solved(ix);
            if frame.row.solved_fields == self.finished_mask {
                self.clear_rounds(&mut state.output_rounds, frame);
                for get in self.get_rounds.iter() {
                    (*get)(&state.distinct_index, &mut state.output_rounds, &mut state.intermediates, frame);
                    // if state.output_rounds.get_output_rounds().len() == 0 {
                    //     continue 'main;
                    // }
                    // self.do_output(state, frame);

                    if self.binds.len() > 0 {
                        self.do_binds(&mut state.distinct_index, &state.output_rounds, &mut state.rounds, frame);
                    }
                    if self.commits.len() > 0 {
                        self.do_commits(&mut state.distinct_index, &state.output_rounds, &mut state.rounds, frame);
                    }
                }
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

    pub fn do_output(&self, state:&mut RuntimeState, frame:&mut Frame) {
        if self.binds.len() > 0 {
            self.do_binds(&mut state.distinct_index, &state.output_rounds, &mut state.rounds, frame);
        }
        if self.commits.len() > 0 {
            self.do_commits(&mut state.distinct_index, &state.output_rounds, &mut state.rounds, frame);
        }
        // self.do_intermediate_insert(&mut state.intermediates, &state.output_rounds, frame);
        // self.do_project(frame);
        // self.do_aggregate(&mut state.interner, &mut state.intermediates, &state.output_rounds, frame);
        // self.do_watch(&mut state.watch_indexes, &state.output_rounds, frame);
    }


    pub fn do_binds(&self, distinct_index: &mut DistinctIndex, output_rounds: &OutputRounds, rounds: &mut RoundHolder, frame: &mut Frame) {
        for &(round, count) in output_rounds.get_output_rounds().iter() {
            for &(e, a, v) in self.binds.iter() {
                let output = Change { e: frame.resolve(&e), a: frame.resolve(&a), v:frame.resolve(&v), n: 0, round: round + 1, transaction: 0, count, };
                frame.counters.inserts += 1;
                distinct_index.distinct(&output, rounds);
            }
        }
    }

    pub fn do_commits(&self, distinct_index: &mut DistinctIndex, output_rounds: &OutputRounds, rounds: &mut RoundHolder, frame: &mut Frame) {
        let n = (frame.block_ix * 10000) as u32;
        for &(_, count) in output_rounds.get_output_rounds().iter() {
            for &(e, a, v, change_type) in self.commits.iter() {
                let output = Change { e: frame.resolve(&e), a: frame.resolve(&a), v:frame.resolve(&v), n, round:0, transaction: 0, count };
                frame.counters.inserts += 1;
                rounds.commit(output, change_type)
            }
        }
    }

    pub fn do_project(&self, frame: &mut Frame) {
        for from in self.project_fields.iter().cloned() {
            let value = frame.get_register(from);
            frame.results.push(value);
        }
    }

    pub fn do_intermediate_insert(&self, intermediates: &mut IntermediateIndex, output_rounds: &OutputRounds, frame: &mut Frame) {
        for &(ref key, ref value, negate) in self.intermediates.iter() {
            let resolved:Vec<Interned> = key.iter().map(|v| frame.resolve(v)).collect();
            let resolved_value:Vec<Interned> = value.iter().map(|v| frame.resolve(v)).collect();
            let mut full_key = resolved.clone();
            full_key.extend(resolved_value.iter());
            for &(round, count) in output_rounds.get_output_rounds().iter() {
                frame.counters.inserts += 1;
                intermediates.distinct(full_key.clone(), resolved.clone(), resolved_value.clone(), round, count, negate);
            }
        }
    }

    pub fn do_aggregate(&self, interner:&mut Interner, intermediates: &mut IntermediateIndex, output_rounds: &OutputRounds, frame: &mut Frame) {
        for &(ref group, ref params, ref output_key, add, remove) in self.aggregates.iter() {
            let resolved_group:Vec<Interned> = group.iter().map(|v| frame.resolve(v)).collect();
            let resolved_params:Vec<Internable> = { params.iter().map(|v| interner.get_value(frame.resolve(v)).clone()).collect() };
            let resolved_output:Vec<Interned> = output_key.iter().map(|v| frame.resolve(v)).collect();
            for &(round, count) in output_rounds.get_output_rounds().iter() {
                let action = if count < 0 { remove } else { add };
                frame.counters.inserts += 1;
                intermediates.aggregate(interner, resolved_group.clone(), resolved_params.clone(), round, action, resolved_output.clone());
            }
        }
    }

    pub fn do_watch(&self, watches: &mut HashMap<String, WatchIndex>, output_rounds: &OutputRounds, frame: &mut Frame) {
        for &(ref name, ref registers) in self.watch_registers.iter() {
            let resolved:Vec<Interned> = registers.iter().map(|x| frame.resolve(x)).collect();
            let mut total = 0;
            for &(_, count) in output_rounds.get_output_rounds().iter() {
                total += count;
            }
            frame.counters.inserts += 1;
            let index = watches.entry(name.to_string()).or_insert_with(|| WatchIndex::new());
            index.insert(resolved, total);
        }
    }

}

//-------------------------------------------------------------------------
// Scan
//-------------------------------------------------------------------------

pub fn make_scan_get_iterator(scan:&Constraint, ix: usize) -> Box<GetIteratorFunc> {
    let (e,a,v,register_mask) = match scan {
        &Constraint::Scan { e, a, v, register_mask} => (e,a,v,register_mask),
        _ => unreachable!()
    };
    Box::new(move |interner, iter, index, intermediates, frame| {
        // if we have already solved all of this scan's vars, we just move on
        if check_bits(frame.row.solved_fields, register_mask) {
            return true;
        }

        let resolved_e = frame.resolve(&e);
        let resolved_a = frame.resolve(&a);
        let resolved_v = frame.resolve(&v);

        // println!("Getting proposal for {:?} {:?} {:?}", resolved_e, resolved_a, resolved_v);
        if index.propose(iter, resolved_e, resolved_a, resolved_v) {
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

pub fn make_scan_accept(scan:&Constraint, me:usize) -> Box<AcceptFunc>  {
    let (e,a,v,register_mask) = match scan {
        &Constraint::Scan { e, a, v, register_mask} => (e,a,v,register_mask),
        _ => unreachable!()
    };
    Box::new(move |interner, index, intermediates, frame, cur_constraint| {
        // if we aren't solving for something this scan cares about, then we
        // automatically accept it.
        if cur_constraint == me || !has_any_bits(register_mask, frame.row.solving_for) {
            return true;
        }
        let resolved_e = frame.resolve(&e);
        let resolved_a = frame.resolve(&a);
        let resolved_v = frame.resolve(&v);
        index.check(resolved_e, resolved_a, resolved_v)
    })
}

pub fn make_scan_get_rounds(scan:&Constraint) -> Box<GetRoundsFunc> {
    let (e,a,v,_) = match scan {
        &Constraint::Scan { e, a, v, register_mask} => (e,a,v,register_mask),
        _ => unreachable!()
    };
    Box::new(move |distinct_index, rounds, intermediates, frame| {
            let resolved_e = frame.resolve(&e);
            let resolved_a = frame.resolve(&a);
            let resolved_v = frame.resolve(&v);
            rounds.compute_output_rounds(distinct_index.iter(resolved_e, resolved_a, resolved_v));
    })
}

//-------------------------------------------------------------------------
// Filter
//-------------------------------------------------------------------------

pub fn make_filter_accept(scan:&Constraint, me:usize) -> Box<AcceptFunc>  {
    let (left, right, func, param_mask) = match scan {
        &Constraint::Filter {ref left, ref right, ref func, param_mask, .. } => (left.clone(), right.clone(), *func, param_mask),
        _ => unreachable!()
    };
    Box::new(move |interner, index, intermediates, frame, cur_constraint| {
        if cur_constraint == me || !has_any_bits(param_mask, frame.row.solving_for) {
            return true;
        }
        if check_bits(frame.row.solved_fields, param_mask) {
            let resolved_left = interner.get_value(frame.resolve(&left));
            let resolved_right = interner.get_value(frame.resolve(&right));
            func(resolved_left, resolved_right)
        } else {
            true
        }
    })
}

//-------------------------------------------------------------------------
// Function
//-------------------------------------------------------------------------

pub fn make_function_get_iterator(scan:&Constraint, ix: usize) -> Box<GetIteratorFunc> {
    let (func, output, params, param_mask, output_mask) = match scan {
        &Constraint::Function {ref func, ref output, ref params, param_mask, output_mask, ..} => (*func, output.clone(), params.clone(), param_mask, output_mask),
        _ => unreachable!()
    };
    Box::new(move |interner, iter, index, intermediates, frame| {
        let solved = frame.row.solved_fields;
        if check_bits(solved, param_mask) && !check_bits(solved, output_mask) {
            let result = {
                let mut resolved = vec![];
                for param in params.iter() {
                    resolved.push(interner.get_value(frame.resolve(param)));
                }
                func(resolved)
            };
            match result {
                Some(v) => {
                    if iter.is_better(1) {
                        let id = interner.internable_to_id(v);
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
            false
        }
    })
}

pub fn make_function_accept(scan:&Constraint, me:usize) -> Box<AcceptFunc>  {
    let (func, output, params, param_mask, output_mask) = match scan {
        &Constraint::Function {ref func, ref output, ref params, param_mask, output_mask, ..} => (*func, output.clone(), params.clone(), param_mask, output_mask),
        _ => unreachable!()
    };
    Box::new(move |interner, index, intermediates, frame, cur_constraint| {
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
                    resolved.push(interner.get_value(frame.resolve(param)));
                }
                func(resolved)
            };
            match result {
                Some(v) => {
                    let id = interner.internable_to_id(v);
                    id == frame.resolve(&output)
                }
                _ => false,
            }
    })
}

//-------------------------------------------------------------------------
// MultiFunction
//-------------------------------------------------------------------------

pub fn make_multi_get_iterator(scan:&Constraint, ix: usize) -> Box<GetIteratorFunc> {
    let (func, output_fields, params, param_mask, output_mask) = match scan {
        &Constraint::MultiFunction {ref func, outputs:ref output_fields, ref params, param_mask, output_mask, ..} => (*func, output_fields.clone(), params.clone(), param_mask, output_mask),
        _ => unreachable!()
    };
    Box::new(move |interner, iter, index, intermediates, frame| {
        let solved = frame.row.solved_fields;
        if check_bits(solved, param_mask) && !check_bits(solved, output_mask) {
            let result = {
                let mut resolved = vec![];
                for param in params.iter() {
                    resolved.push(interner.get_value(frame.resolve(param)));
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
                            row.drain(..).map(|field| interner.internable_to_id(field)).collect()
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

pub fn make_multi_accept(scan:&Constraint, me:usize) -> Box<AcceptFunc>  {
    let (e,a,v,register_mask) = match scan {
        &Constraint::Scan { e, a, v, register_mask} => (e,a,v,register_mask),
        _ => unreachable!()
    };
    Box::new(move |interner, index, intermediates, frame, cur_constraint| {
        // FIXME why don't we need this?
        true
    })
}

//-------------------------------------------------------------------------
// IntermediateScan
//-------------------------------------------------------------------------

pub fn make_intermediate_get_iterator(scan:&Constraint, ix: usize) -> Box<GetIteratorFunc> {
    let (key, value, register_mask, output_mask) = match scan {
        &Constraint::IntermediateScan { ref key, ref value, register_mask, output_mask, .. } => (key.clone(), value.clone(), register_mask, output_mask),
        _ => unreachable!()
    };
    Box::new(move |interner, mut iter, index, intermediates, frame| {
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
        if intermediates.propose(&mut iter, resolved, outputs) {
            iter.constraint = ix;
        }
        true
    })
}

pub fn make_intermediate_accept(scan:&Constraint, me:usize) -> Box<AcceptFunc>  {
    let (key, value, register_mask, output_mask) = match scan {
        &Constraint::IntermediateScan { ref key, ref value, register_mask, output_mask, .. } => (key.clone(), value.clone(), register_mask, output_mask),
        _ => unreachable!()
    };
    Box::new(move |interner, index, intermediates, frame, cur_constraint| {
        // if we haven't solved all our inputs and outputs, just skip us
        if cur_constraint == me ||
           !check_bits(frame.row.solved_fields, register_mask) ||
           !check_bits(frame.row.solved_fields, output_mask) {
                return true;
            }

        let resolved = key.iter().map(|param| frame.resolve(param)).collect();
        let resolved_value = value.iter().map(|param| frame.resolve(param)).collect();

        intermediates.check(&resolved, &resolved_value)
    })
}

pub fn make_intermediate_get_rounds(scan:&Constraint) -> Box<GetRoundsFunc> {
    let (key, value) = match scan {
        &Constraint::IntermediateScan { ref key, ref value, .. } => (key.clone(), value.clone()),
        _ => unreachable!()
    };
    Box::new(move |distinct_index, rounds, intermediates, frame| {
        let resolved:Vec<Interned> = key.iter().map(|v| frame.resolve(v)).collect();
        let resolved_value:Vec<Interned> = value.iter().map(|v| frame.resolve(v)).collect();
        rounds.compute_output_rounds(intermediates.distinct_iter(&resolved, &resolved_value));
    })
}

//-------------------------------------------------------------------------
// AntiScan
//-------------------------------------------------------------------------

pub fn make_anti_get_rounds(scan:&Constraint) -> Box<GetRoundsFunc> {
    let key = match scan {
        &Constraint::AntiScan { ref key, .. } => key.clone(),
        _ => unreachable!()
    };
    Box::new(move |distinct_index, rounds, intermediates, frame| {
        let resolved:Vec<Interned> = key.iter().map(|v| frame.resolve(v)).collect();
        rounds.compute_anti_output_rounds(intermediates.distinct_iter(&resolved, &vec![]));
    })
}
