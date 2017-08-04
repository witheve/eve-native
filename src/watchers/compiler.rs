use super::super::ops::{make_scan, make_function, Constraint, Interned, Internable, Interner, Field, RunLoopMessage};
use super::super::compiler::{get_function_info};
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
    block_to_constraints: HashMap<Interned, Vec<Interned>>,
    constraints: HashMap<Interned, Constraint>,
    constraint_to_args: HashMap<Interned, HashMap<String, Interned>>,
    constraint_to_varargs: HashMap<Interned, Vec<Interned>>,
    variable_to_identity_attrs: HashMap<Interned, HashMap<Interned, Interned>>
}

impl CompilerWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>) -> CompilerWatcher {
        CompilerWatcher{name: "eve/compiler".to_string(),
                        outgoing,
                        variable_ix: 0,
                        variables: HashMap::new(),
                        block_types: HashMap::new(),
                        block_to_constraints: HashMap::new(),
                        constraints: HashMap::new(),
                        constraint_to_args: HashMap::new(),
                        constraint_to_varargs: HashMap::new(),
                        variable_to_identity_attrs: HashMap::new()}
    }

    pub fn get_field(&self, value:Interned) -> Field {
        self.variables.get(&value).cloned().unwrap_or_else(|| Field::Value(value))
    }

    pub fn update_variables(&mut self, interner:&mut Interner, diff:&WatchDiff) {
        let kind = interner.string_id("variable");
        for var in diff.adds.iter().filter(|v| kind == v[0]) {
            match self.variables.entry(var[1]) {
                Entry::Occupied(_) => {},
                Entry::Vacant(entry) => {
                    let ix = self.variable_ix;
                    self.variable_ix += 1;
                    entry.insert(Field::Register(ix));
                }
            };
        }
    }

    pub fn update_arguments(&mut self, interner:&mut Interner, diff:&WatchDiff) {
        let kind = interner.string_id("argument");
        for arg in diff.adds.iter().filter(|v| kind == v[0]) {
            if let &[id, attr, val] = &arg[1..] {
                let mut args = self.constraint_to_args.entry(id).or_insert_with(|| HashMap::new());
                match interner.get_value(attr) {
                    &Internable::String(ref a) => { args.insert(a.to_string(), val); },
                    _ => unimplemented!()
                }
            }
        }
    }

    pub fn update_varargs(&mut self, interner:&mut Interner, diff:&WatchDiff) {
        let kind = interner.string_id("variadic-argument");
        for arg in diff.adds.iter().filter(|v| kind == v[0]) {
            if let &[id, interned_ix, val] = &arg[1..] {
                let mut args = self.constraint_to_varargs.entry(id).or_insert_with(|| vec![]);
                let ix = Internable::to_number(interner.get_value(interned_ix)) as usize;
                args.insert(ix, val);
            }
        }
    }

    pub fn update_identity_attrs(&mut self, interner:&mut Interner, diff:&WatchDiff) {
        let kind = interner.string_id("identity-attribute");
        for arg in diff.adds.iter().filter(|v| kind == v[0]) {
            if let &[id, attribute, value] = &arg[1..] {
                let mut identity_attrs = self.variable_to_identity_attrs.entry(id).or_insert_with(|| HashMap::new());
                identity_attrs.insert(attribute, value);
            }
        }
    }


    pub fn args_to_vec(&mut self, id:Interned, op:&str) -> Option<Vec<Field>> {
        if let Some(info) = get_function_info(op) {
            if let Some(args) = self.constraint_to_args.get(&id) {
                let mut params:Vec<Field> = vec![];
                for param in info.get_params() {
                    if let Some(&val) = args.get(param) {
                        params.push(self.get_field(val));
                    } else {
                        return None;
                    }
                }
                return Some(params);
            }
        }
        None
    }

    pub fn varargs_to_vec(&mut self, id:Interned) -> Option<Vec<Field>> {
        if let Some(args) = self.constraint_to_varargs.get(&id) {
            Some(args.iter().map(|&arg| self.get_field(arg)).collect())
        } else {
            None
        }

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
        self.update_variables(interner, &diff);
        self.update_arguments(interner, &diff);
        self.update_varargs(interner, &diff);

        let mut existing_blocks:HashSet<Interned> = HashSet::new();
        existing_blocks.extend(self.block_types.keys());
        let mut damaged_blocks:HashSet<Interned> = HashSet::new();

        for remove in diff.removes {
            if let &Internable::String(ref kind) = interner.get_value(remove[0]) {
                match (kind.as_ref(), &remove[1..]) {
                    ("block", &[block, _]) => {
                        self.block_types.remove(&block);
                        damaged_blocks.insert(block);
                    },
                    ("scan", &[id, block, ..]) |
                    ("output", &[id, block, ..]) |
                    ("function", &[id, block, ..]) |
                    ("variadic", &[id, block, ..]) => {
                        self.constraints.remove(&id).unwrap();
                        self.block_to_constraints.get_mut(&block).unwrap().remove_item(&id);
                        damaged_blocks.insert(block);
                    },
                    ("argument", ..) | ("variable", _) => {},
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
                        damaged_blocks.insert(block);
                    },
                    ("scan", &[id, block, e, a, v]) => {
                        let scan = make_scan(self.get_field(e), self.get_field(a), self.get_field(v));
                        let constraints = self.block_to_constraints.entry(block).or_insert_with(|| vec![]);
                        constraints.push(id);
                        self.constraints.insert(id, scan);
                        damaged_blocks.insert(block);
                    },
                    ("output", &[id, block, e, a, v]) => {
                        let output = Constraint::Insert{e: self.get_field(e), a: self.get_field(a), v: self.get_field(v), commit: false};
                        let constraints = self.block_to_constraints.entry(block).or_insert_with(|| vec![]);
                        constraints.push(id);
                        self.constraints.insert(id, output);
                        damaged_blocks.insert(block);
                    },
                    ("function", &[id, block, name, output]) => {
                        if let &Internable::String(ref op) = interner.get_value(name) {
                            if let Some(params) = self.args_to_vec(id, op) {
                                let function = make_function(op, params, self.get_field(output));
                                let constraints = self.block_to_constraints.entry(block).or_insert_with(|| vec![]);
                                constraints.push(id);
                                self.constraints.insert(id, function);
                                damaged_blocks.insert(block);
                            }
                        }
                    },
                    ("variadic", &[id, block, name, output]) => {
                        if let &Internable::String(ref op) = interner.get_value(name) {
                            if let Some(params) = self.varargs_to_vec(id) {
                                // @FIXME: Just actually rename it gen-id.
                                let real_op = match op.as_str() {
                                    "gen-id" => "gen_id",
                                    _ => op
                                };
                                let function = make_function(real_op, params, self.get_field(output));
                                let constraints = self.block_to_constraints.entry(block).or_insert_with(|| vec![]);
                                constraints.push(id);
                                self.constraints.insert(id, function);
                                damaged_blocks.insert(block);
                            }
                        }
                    },
                    ("argument", ..) | ("variable", _) => {},
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
                let constraints = self.block_to_constraints.get(block).unwrap();
                comp.constraints.extend(constraints.iter().map(|&id| self.constraints.get(&id).unwrap()).cloned());
                comp.finalize();
                added_blocks.extend(compilation_to_blocks(comp, "compiler_watcher", ""));
            }
        }

        // @FIXME: Gotta plumb remove all the way through.
        self.outgoing.send(RunLoopMessage::CodeTransaction(added_blocks, removed_blocks)).unwrap();
    }
}
