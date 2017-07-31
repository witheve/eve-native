use super::super::ops::{make_scan, Constraint, Interned, Internable, Interner, Field, RunLoopMessage};
use indexes::{WatchDiff};
use std::sync::mpsc::{Sender};
use super::Watcher;
use compiler::{Compilation, compilation_to_blocks};
use std::collections::{HashMap};
use std::collections::hash_map::{Entry};

//-------------------------------------------------------------------------
// Compiler Watcher
//-------------------------------------------------------------------------

pub struct CompilerWatcher {
    name: String,
    outgoing: Sender<RunLoopMessage>,
    variable_ix: usize,
    variables: HashMap<Interned, Field>,
    block_types: HashMap<Interned, Interned>,
    blocks_to_constraints: HashMap<Interned, Vec<Constraint>>,
}

impl CompilerWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>) -> CompilerWatcher {
        CompilerWatcher{name: "eve/compiler".to_string(),
                        outgoing,
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
    fn get_name(& self) -> String {
        self.name.clone()
    }
    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
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


