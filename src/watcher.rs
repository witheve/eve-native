extern crate tokio_timer;
extern crate tokio_core;
extern crate futures;
extern crate time;

use std::time::*;
use indexes::{WatchDiff};
use ops::{make_scan, Constraint, Interned, Internable, Interner, Field, RawChange, RunLoopMessage};
use compiler::{Compilation, compilation_to_blocks};
use std::sync::mpsc::{self, Sender};
use std::thread::{self};
use std::process;
use std::collections::{HashMap};
use std::collections::hash_map::{Entry};

pub trait Watcher {
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff);
}

//-------------------------------------------------------------------------
// System Watcher
//-------------------------------------------------------------------------

pub struct SystemTimerWatcher {
    outgoing: Sender<RunLoopMessage>,
    timers: HashMap<Interned, (usize, Sender<()>)>
}

impl SystemTimerWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>) -> SystemTimerWatcher {
        SystemTimerWatcher { outgoing, timers: HashMap::new() }
    }
}

impl Watcher for SystemTimerWatcher {
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        for remove in diff.removes {
            if let Entry::Occupied(mut entry) = self.timers.entry(remove[1]) {
                let should_remove = {
                    let pair = entry.get_mut();
                    let ref mut count = pair.0;
                    if *count > 1 {
                        *count -= 1;
                        false
                    } else {
                        pair.1.send(()).unwrap();
                        true
                    }
                };
                if should_remove {
                    entry.remove_entry();
                }
            }
        }

        for add in diff.adds {
            if let Entry::Occupied(mut entry) = self.timers.entry(add[1]) {
                let ref mut count = entry.get_mut().0;
                *count += 1;
                continue;
            }

            println!("timer: {:?}", add.iter().map(|v| interner.get_value(*v).print()).collect::<Vec<String>>());
            let internable_resolution = interner.get_value(add[1]).clone();
            let resolution = Internable::to_number(&internable_resolution) as u64;
            let id = Internable::String(format!("system/timer/change/{}", add[0]));

            let duration = Duration::from_millis(resolution);
            let (sender, receiver) = mpsc::channel();
            let outgoing = self.outgoing.clone();
            self.timers.insert(add[1], (1, sender));

            thread::spawn(move || {
                let mut tick = 0;
                loop {
                    thread::sleep(duration);
                    if receiver.try_recv().is_ok() {
                        break;
                    }
                    let cur_time = time::now();
                    // println!("It's time! {:?}", cur_time);
                    let changes = vec![
                        RawChange {e: id.clone(), a: Internable::String("tag".to_string()), v: Internable::String("system/timer/change".to_string()), n: Internable::String("System/timer".to_string()), count: 1},
                        RawChange {e: id.clone(), a: Internable::String("resolution".to_string()), v: internable_resolution.clone(), n: Internable::String("System/timer".to_string()), count: 1},
                        RawChange {e: id.clone(), a: Internable::String("hour".to_string()), v: Internable::from_number(cur_time.tm_hour as f32), n: Internable::String("System/timer".to_string()), count: 1},
                        RawChange {e: id.clone(), a: Internable::String("minute".to_string()), v: Internable::from_number(cur_time.tm_min as f32), n: Internable::String("System/timer".to_string()), count: 1},
                        RawChange {e: id.clone(), a: Internable::String("second".to_string()), v: Internable::from_number(cur_time.tm_sec as f32), n: Internable::String("System/timer".to_string()), count: 1},
                        RawChange {e: id.clone(), a: Internable::String("tick".to_string()), v: Internable::from_number(tick as f32), n: Internable::String("System/timer".to_string()), count: 1},
                    ];
                    tick += 1;
                    match outgoing.send(RunLoopMessage::Transaction(changes)) {
                        Err(_) => break,
                        _ => {}
                    }
                }
            });
        }
    }
}

//-------------------------------------------------------------------------
// Console Watcher
//-------------------------------------------------------------------------

pub struct ConsoleLogWatcher { }

impl Watcher for ConsoleLogWatcher {
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        for add in diff.adds {
            let text = add.iter().map(|v| interner.get_value(*v).print()).collect::<Vec<String>>().into_iter();
            for t in text {
                println!("{}",t);
            }
        }
    }
}

pub struct ConsoleErrorWatcher { }

impl Watcher for ConsoleErrorWatcher {
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        for add in diff.adds {
            let text = add.iter().map(|v| interner.get_value(*v).print()).collect::<Vec<String>>().into_iter();
            for t in text {
                eprintln!("{}", t);
                process::exit(1);
            }
        }
    }
}

pub struct ConsoleWarnWatcher { }

impl Watcher for ConsoleWarnWatcher {
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        for add in diff.adds {
            let text = add.iter().map(|v| interner.get_value(*v).print()).collect::<Vec<String>>().into_iter();
            for t in text {
                println!("{}",t);
            }
        }
    }
}

pub struct PrintDiffWatcher { }

impl Watcher for PrintDiffWatcher {
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        for remove in diff.removes {
            println!("Printer: - {:?}", remove.iter().map(|v| interner.get_value(*v).print()).collect::<Vec<String>>());
        }
        for add in diff.adds {
            println!("Printer: + {:?}", add.iter().map(|v| interner.get_value(*v).print()).collect::<Vec<String>>());
        }
    }
}

// pub enum RawConstraint {
//     Scan{e:Internable, a:Internable, v:Internable},
//     Output{e:Internable, a:Internable, v:Internable}
// }

pub struct CompilerWatcher {
    outgoing: Sender<RunLoopMessage>,
    variable_ix: usize,
    variables: HashMap<Interned, Field>,
    block_types: HashMap<Interned, Interned>,
    blocks_to_constraints: HashMap<Interned, Vec<Constraint>>,
}

impl CompilerWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>) -> CompilerWatcher {
        CompilerWatcher{outgoing,
                        variable_ix: 0,
                        variables: HashMap::new(),
                        block_types: HashMap::new(),
                        blocks_to_constraints: HashMap::new()}
    }

    pub fn get_field(&self, value:Interned) -> Field {
        self.variables.get(&value).cloned().unwrap_or_else(|| Field::Value(value))
    }
}

impl Watcher for CompilerWatcher {
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        for _ in diff.removes {
            println!("WARNING: Compile watcher ignoring removals for now");
        }

        let interned_variable = interner.string_id("variable");
        for v in diff.adds.iter().filter(|v| interned_variable == v[0]) {
            match self.variables.entry(v[1]) {
                Entry::Occupied(_) => {},
                Entry::Vacant(entry) => {
                    let ix = self.variable_ix;
                    self.variable_ix += 1;
                    entry.insert(Field::Register(ix));
                }
            };
        }

        for add in diff.adds {
            if let &Internable::String(ref kind) = interner.get_value(add[0]) {
                match (kind.as_ref(), &add[1..]) {
                    ("block", &[block, kind]) => {
                        match self.block_types.entry(block) {
                            Entry::Occupied(_) => panic!("Cannot compile block with multiple types."),
                            Entry::Vacant(entry) => { entry.insert(kind); }
                        }
                    },
                    ("scan", &[block, e, a, v]) => {
                        let scan = make_scan(self.get_field(e), self.get_field(a), self.get_field(v));
                        let constraints = self.blocks_to_constraints.entry(block).or_insert_with(|| vec![]);
                        constraints.push(scan);
                    },
                    ("output", &[block, e, a, v]) => {
                        let output = Constraint::Insert{e: self.get_field(e), a: self.get_field(a), v: self.get_field(v), commit: false};
                        let constraints = self.blocks_to_constraints.entry(block).or_insert_with(|| vec![]);
                        constraints.push(output);
                    },
                    ("variable", _) => {},
                    _ => println!("Found other '{:?}'", add)
                }
            }
        }

        let mut added_blocks = vec![];
        for (block, _) in self.block_types.iter() {
            let mut comp = Compilation::new(format!("made up block's nice string (it's for him) {}", block));
            let constraints = self.blocks_to_constraints.get(block).unwrap();
            comp.constraints.extend(constraints.iter().cloned());
            comp.finalize();
            added_blocks.extend(compilation_to_blocks(comp, "compiler_watcher", ""));
        }
        println!("got some blocks? {:?}", added_blocks);
        self.outgoing.send(RunLoopMessage::CodeTransaction(added_blocks, vec![])).unwrap();
    }
}
