use super::super::ops::{make_scan, Constraint, Interned, Internable, Interner, Field, RunLoopMessage};
use indexes::{WatchDiff};
use std::sync::mpsc::{Sender};
use super::Watcher;
use compiler::{Compilation, compilation_to_blocks};
use std::collections::{HashMap, HashSet};
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
    blocks_to_constraints: HashMap<Interned, Vec<Interned>>,
    constraints: HashMap<Interned, Constraint>
}

impl CompilerWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>) -> CompilerWatcher {
        CompilerWatcher{name: "eve/compiler".to_string(),
                        outgoing,
                        variable_ix: 0,
                        variables: HashMap::new(),
                        block_types: HashMap::new(),
                        blocks_to_constraints: HashMap::new(),
                        constraints: HashMap::new()}
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

        let mut existing_blocks:HashSet<Interned> = HashSet::new();
        existing_blocks.extend(self.block_types.keys());
        let mut damaged_blocks:Vec<Interned> = vec![];

        for remove in diff.removes {
            if let &Internable::String(ref kind) = interner.get_value(remove[0]) {
                match (kind.as_ref(), &remove[1..]) {
                    ("block", &[block, _]) => {
                        self.block_types.remove(&block);
                        damaged_blocks.push(block);
                    },
                    ("scan", &[id, block, ..]) => {
                        self.constraints.remove(&id).unwrap();
                        self.blocks_to_constraints.get_mut(&block).unwrap().remove_item(&id);
                        damaged_blocks.push(block);
                    },
                    ("output", &[id, block, ..]) => {
                        self.constraints.remove(&id).unwrap();
                        self.blocks_to_constraints.get_mut(&block).unwrap().remove_item(&id);
                        damaged_blocks.push(block);
                    },
                    ("variable", _) => {},
                    _ => println!("Found other removal '{:?}'", remove)
                }
            }
        }

        for add in diff.adds {
            if let &Internable::String(ref kind) = interner.get_value(add[0]) {
                match (kind.as_ref(), &add[1..]) {
                    ("block", &[block, kind]) => {
                        match self.block_types.entry(block) {
                            Entry::Occupied(_) => panic!("Cannot compile block with multiple types."),
                            Entry::Vacant(entry) => { entry.insert(kind); }
                        }
                        damaged_blocks.push(block);
                    },
                    ("scan", &[id, block, e, a, v]) => {
                        let scan = make_scan(self.get_field(e), self.get_field(a), self.get_field(v));
                        let constraints = self.blocks_to_constraints.entry(block).or_insert_with(|| vec![]);
                        constraints.push(id);
                        self.constraints.insert(id, scan);
                        damaged_blocks.push(block);
                    },
                    ("output", &[id, block, e, a, v]) => {
                        let output = Constraint::Insert{e: self.get_field(e), a: self.get_field(a), v: self.get_field(v), commit: false};
                        let constraints = self.blocks_to_constraints.entry(block).or_insert_with(|| vec![]);
                        constraints.push(id);
                        self.constraints.insert(id, output);
                        damaged_blocks.push(block);
                    },
                    ("variable", _) => {},
                    _ => println!("Found other '{:?}'", add)
                }
            }
        }


        let mut removed_blocks = vec![];
        let mut added_blocks = vec![];

        for block in damaged_blocks.iter() {
            if existing_blocks.contains(&block) {
                let name = format!("dynamic block {}", block);
                removed_blocks.push(name);
            }
            if self.block_types.contains_key(block) {
                let mut comp = Compilation::new(format!("dynamic block {}", block));
                let constraints = self.blocks_to_constraints.get(block).unwrap();
                comp.constraints.extend(constraints.iter().map(|&id| self.constraints.get(&id).unwrap()).cloned());
                comp.finalize();
                added_blocks.extend(compilation_to_blocks(comp, "compiler_watcher", ""));
            }
        }

        // @FIXME: Gotta plumb remove all the way through.
        self.outgoing.send(RunLoopMessage::CodeTransaction(added_blocks, removed_blocks)).unwrap();
    }
}
