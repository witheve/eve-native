use ops::*;
use indexes::{DistinctIndex};
use std::fmt;
use std::sync::Arc;

//-------------------------------------------------------------------------
// Bind
//-------------------------------------------------------------------------

pub type BindFunc = Fn(&mut DistinctIndex, &OutputRounds, &mut RoundHolder, &mut Frame) -> i32;

#[derive(Clone)]
pub struct BindCallback(pub Arc<BindFunc>);

impl fmt::Debug for BindCallback {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BindCallback")
    }
}

impl PartialEq for BindCallback {
    fn eq(&self, other:&BindCallback) -> bool {
        (&self.0 as *const _) == (&other.0 as *const _)
    }
}

impl Eq for BindCallback {}
unsafe impl Send for BindCallback {}

macro_rules! bind_instruction (
    ($name:ident $(, ($ix:expr, $e:ident, $a:ident, $v:ident, $change:ident))*) => (
        fn $name(constraints: &Vec<&Constraint>, next:i32) -> BindCallback {
            $(
            let ($e, $a, $v) = match constraints[$ix] {
                &Constraint::Insert {e, a, v, ..} => { (e, a, v) },
                _ => { panic!("non insert") }
            };
            )*
            BindCallback(Arc::new(move |distinct_index, output_rounds, rounds, frame| {
                $(
                let $change = Change { e: frame.resolve(&$e), a: frame.resolve(&$a), v:frame.resolve(&$v), n: 0, round:0, transaction: 0, count:0, };
                )*
                // println!("rounds {:?}", rounds.output_rounds);
                for &(round, count) in output_rounds.get_output_rounds().iter() {
                $(
                    distinct_index.distinct(&$change.with_round_count(round + 1, count), rounds);
                    frame.counters.inserts += 1;
                )*
                }
                next
            }))
        }
    );
);

bind_instruction!(make_bind1, (0, e1, a1, v1, c1));
bind_instruction!(make_bind2, (0, e1, a1, v1, c1), (1, e2, a2, v2, c2));
bind_instruction!(make_bind3, (0, e1, a1, v1, c1), (1, e2, a2, v2, c2), (2, e3, a3, v3, c3));
bind_instruction!(make_bind4, (0, e1, a1, v1, c1), (1, e2, a2, v2, c2), (2, e3, a3, v3, c3), (3, e4, a4, v4, c4));
bind_instruction!(make_bind5, (0, e1, a1, v1, c1), (1, e2, a2, v2, c2), (2, e3, a3, v3, c3), (3, e4, a4, v4, c4), (4, e5, a5, v5, c5));
bind_instruction!(make_bind6, (0, e1, a1, v1, c1), (1, e2, a2, v2, c2), (2, e3, a3, v3, c3), (3, e4, a4, v4, c4), (4, e5, a5, v5, c5), (5, e6, a6, v6, c6));
bind_instruction!(make_bind7, (0, e1, a1, v1, c1), (1, e2, a2, v2, c2), (2, e3, a3, v3, c3), (3, e4, a4, v4, c4), (4, e5, a5, v5, c5), (5, e6, a6, v6, c6), (6, e7, a7, v7, c7));
bind_instruction!(make_bind8, (0, e1, a1, v1, c1), (1, e2, a2, v2, c2), (2, e3, a3, v3, c3), (3, e4, a4, v4, c4), (4, e5, a5, v5, c5), (5, e6, a6, v6, c6), (6, e7, a7, v7, c7), (7, e8, a8, v8, c8));

pub fn make_bind_instruction(constraints:&Vec<&Constraint>, next:i32) -> BindCallback {
    match constraints.len() {
        1 => { make_bind1(constraints, next) }
        2 => { make_bind2(constraints, next) }
        3 => { make_bind3(constraints, next) }
        4 => { make_bind4(constraints, next) }
        5 => { make_bind5(constraints, next) }
        6 => { make_bind6(constraints, next) }
        7 => { make_bind7(constraints, next) }
        8 => { make_bind8(constraints, next) }
        _ => { unimplemented!() }
    }
}

//-------------------------------------------------------------------------
// Commit
//-------------------------------------------------------------------------

pub type CommitFunc = Fn(&OutputRounds, &mut RoundHolder, &mut Frame) -> i32;

#[derive(Clone)]
pub struct CommitCallback(pub Arc<CommitFunc>);

impl fmt::Debug for CommitCallback {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CommitCallback")
    }
}

impl PartialEq for CommitCallback {
    fn eq(&self, other:&CommitCallback) -> bool {
        (&self.0 as *const _) == (&other.0 as *const _)
    }
}

impl Eq for CommitCallback {}
unsafe impl Send for CommitCallback {}

macro_rules! commit_instruction (
    ($name:ident $(, ($ix:expr, $e:ident, $a:ident, $v:ident, $type:ident, $change:ident))*) => (
        fn $name(constraints: &Vec<&Constraint>, next:i32) -> CommitCallback {
            $(
            let ($e, $a, $v, $type) = match constraints[$ix] {
                &Constraint::Insert {e, a, v, ..} => { (e, a, v, ChangeType::Insert) },
                &Constraint::Remove {e, a, v, ..} => { (e, a, v, ChangeType::Remove) },
                &Constraint::RemoveAttribute {e, a, ..} => { (e, a, Field::Value(0), ChangeType::Remove) },
                &Constraint::RemoveEntity {e, ..} => { (e, Field::Value(0), Field::Value(0), ChangeType::Remove) },
                _ => { panic!("unknown commit type") }
            };
            )*
            CommitCallback(Arc::new(move |output_rounds, rounds, frame| {
                $(
                let n = (frame.block_ix * 10000 + $ix) as u32;
                let $change = Change { e: frame.resolve(&$e), a: frame.resolve(&$a), v:frame.resolve(&$v), n, round:0, transaction: 0, count:0, };
                )*
                // println!("rounds {:?}", rounds.output_rounds);
                for &(_, count) in output_rounds.get_output_rounds().iter() {
                $(
                    let real_c = if $type == ChangeType::Remove { count * -1 } else { count };
                    rounds.commit($change.with_round_count(0, real_c), $type);
                    frame.counters.inserts += 1;
                )*
                }
                next
            }))
        }
    );
);

commit_instruction!(make_commit1, (0, e1, a1, v1, type1, c1));
commit_instruction!(make_commit2, (0, e1, a1, v1, type1, c1), (1, e2, a2, v2, type2, c2));
commit_instruction!(make_commit3, (0, e1, a1, v1, type1, c1), (1, e2, a2, v2, type2, c2), (2, e3, a3, v3, type3, c3));
commit_instruction!(make_commit4, (0, e1, a1, v1, type1, c1), (1, e2, a2, v2, type2, c2), (2, e3, a3, v3, type3, c3), (3, e4, a4, v4, type4, c4));

pub fn make_commit_instruction(constraints:&Vec<&Constraint>, next:i32) -> CommitCallback {
    match constraints.len() {
        1 => { make_commit1(constraints, next) }
        2 => { make_commit2(constraints, next) }
        3 => { make_commit3(constraints, next) }
        4 => { make_commit4(constraints, next) }
        _ => { unimplemented!() }
    }
}
