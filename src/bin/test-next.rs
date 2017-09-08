extern {}

extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
use serde_json::Error;

extern crate eve;
extern crate time;

extern crate clap;
use clap::{Arg, App};

use std::collections::{BTreeMap, HashSet, btree_map};
use std::iter::{FromIterator};
use std::io::prelude::*;
use std::fs::{self, File};
use std::path::Path;

use eve::paths::EvePaths;
use eve::ops::{Internable, aggregate_next_add, aggregate_next_remove};
use eve::indexes::{AggregateEntry, update_active_rounds_vec, print_debug_table};
use eve::watchers::system::{SystemTimerWatcher, PanicWatcher};
use eve::watchers::console::{ConsoleWatcher, PrintDiffWatcher};
use eve::watchers::file::FileWatcher;

//////////////////////////////////////////////////////////////////////

pub type Round = u32;
pub type Count = i32;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Change(Vec<Internable>, Round, Count);
impl Change {
    pub fn from((proj, r, c):(Vec<Internable>, Round, Count)) -> Change {
        Change(proj, r, c)
    }
}


#[derive(Debug)]
pub struct Diff {
    missing: HashSet<Change>,
    excess: HashSet<Change>
}

pub enum TestResult {
    Success,
    Fail(usize, Diff),
    Error
}

//////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct Input {
    round: Round,
    count: Count,
    value: String
}

impl Input {
    pub fn to_projection(&self) -> Vec<Internable> {
        vec![Internable::String(self.value.to_owned())]
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Output {
    round: Round,
    count: Count,
    from: String,
    to: String,
}

impl Output {
    pub fn to_change(&self) -> Change {
        Change(vec![Internable::String(self.from.to_owned()), Internable::String(self.to.to_owned())], self.round, self.count)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Problem {
    name: String,
    suite: String,
    inputs: Vec<Input>,
    expected: Vec<Vec<Output>>
}

impl Problem {
    pub fn compare_expected(self:&Problem, expected:&Vec<Output>, actual: &Vec<Change>) -> Option<Diff> {
        let expected_set:HashSet<Change> = HashSet::from_iter(expected.iter().map(|output| output.to_change()));
        let actual_set:HashSet<Change> = HashSet::from_iter(actual.iter().cloned());

        if expected_set == actual_set { None }
        else { Some(Diff{missing: expected_set.difference(&actual_set).cloned().collect(),
                         excess: actual_set.difference(&expected_set).cloned().collect()}) }
    }

    pub fn exec_test(self:&Problem) -> TestResult {
        println!("");
        let mut entry = AggregateEntry::Sorted{ items: BTreeMap::new(), current_round: 0, input_round: 0, current_params:None, changes: vec![], limit: 0 };

        let mut rounds:Vec<Round> = vec![];
        for (ix, (ref input, ref expected)) in self.inputs.iter().zip(self.expected.iter()).enumerate() {
            let round = input.round;
            let count = input.count;

            let mut projection = input.to_projection();

            if let AggregateEntry::Sorted { ref mut current_params, ref mut input_round, .. } = entry {
                *current_params = Some(projection.clone());
                *input_round = round;
            }

            let start = match rounds.binary_search(&round) {
                Ok(pos) =>  { pos }
                Err(pos) => { rounds.insert(pos, round); pos },
            };

            for round in rounds[start..].iter().cloned() {
                if let AggregateEntry::Sorted { ref mut current_round, .. } = entry {
                    *current_round = round;
                }
                if count > 0 {
                    aggregate_next_add(&mut entry, &projection, &projection);
                } else {
                    aggregate_next_remove(&mut entry, &projection, &projection);
                }

            }

            if let AggregateEntry::Sorted { current_params: Some(ref value), ref mut items, changes:ref mut entry_changes, .. } = entry {
                // we have to extend the projection with the params in order to account for
                // things like the limit changing. If we didn't store that, we might
                // mistakenly remove keys when the param changes. E.g. the projection
                // remains the same but the limit changes: [foo, bar] [1] vs [foo, bar]
                // [2], depending on the order we receive the adds/removes, we might end up
                // with no [foo, bar] key at all.
                //projection.extend(value.iter().cloned());
                let debug_cause = projection[0].clone();
                // Insert it into the items btree
                match items.entry(projection) {
                    btree_map::Entry::Occupied(ref mut ent) => {
                        update_active_rounds_vec(ent.get_mut(), round, count);
                    },
                    btree_map::Entry::Vacant(ent) => {
                        ent.insert(vec![round as i32 * count]);
                    }
                }

                // Vec<(Vec<Internable>, Round, Count)>
                match self.compare_expected(expected, &entry_changes.drain(..).map(|c| Change::from(c)).collect()) {
                    None => {},
                    Some(diff) => {
                        return TestResult::Fail(ix, diff);
                    }
                }

                //     let mut debug_changes = DebugEntry{input: debug_cause, count: (round as i32) * count, pairs: vec![]};
                //     for &(ref proj, r, c) in entry_changes.iter() {
                //         if proj.len() == 4 {
                //             debug_changes.pairs.push((proj[0].clone(), proj[2].clone(), (r as i32) * c));
                //         }
                //     }
                //     self.debug_vec.push(debug_changes);
                //     println!("--------------------------------------------------------------------------------");
                //     print_debug_table(&self.debug_vec);
            }
        }
        return TestResult::Success;
    }


}


//-------------------------------------------------------------------------
// Main
//-------------------------------------------------------------------------

fn main() {
    let path = Path::new("/home/josh/repos/kodowa/truth-table-parser/tt.json");
    let mut file = File::open(path).expect("Unable to open the file");
    let mut contents = String::new();
    file.read_to_string(&mut contents).expect("Unable to read the file");

    let problems:Vec<Problem> = serde_json::from_str(&contents).unwrap();
    println!("Running gather/next test suite from truth tables");
    let mut prev_suite = "".to_owned();
    for problem in problems {
        if problem.suite != prev_suite {
            println!("");
            println!("# {}", problem.suite);
            prev_suite = problem.suite.to_owned();
        }
        print!("    {} ... ", problem.name);
        match problem.exec_test() {
            TestResult::Success => println!("SUCCESS"),
            TestResult::Fail(ix, diff) => {
                println!("FAIL @ {}    {:?}", ix + 1, &problem.inputs[ix]);
                if diff.missing.len() > 0 {
                    println!("        MISSING:");
                    for change in diff.missing.iter() {
                        let c = (change.1 as i32) * change.2;
                        let p:Vec<String> = change.0.iter().map(|v| Internable::to_string(v)).collect();
                        println!("            {:+} {}", c, p.join("->"));
                    }
                }
                if diff.excess.len() > 0 {
                    println!("        EXCESS:");
                    for change in diff.excess.iter() {
                        let c = (change.1 as i32) * change.2;
                        let p:Vec<String> = change.0.iter().map(|v| Internable::to_string(v)).collect();
                        println!("            {:+} {}", c, p.join("->"));
                    }
                }
            },
            TestResult::Error => println!("ERROR")
        }
    }
}
