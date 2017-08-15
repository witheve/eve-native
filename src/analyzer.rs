use ops::{Field, Interned, Constraint, Block, TAG_INTERNED_ID, Interner};
use std::collections::{HashSet, HashMap};

//-------------------------------------------------------------------------
// Domain
//-------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Domain {
    Unknown,
    Exists(bool),
    Number(u64, u64),
    String,
    Record,
}

impl Domain {
    pub fn intersects(&self, other: &Domain) -> bool {
        match (self, other) {
            (&Domain::Unknown, &Domain::Unknown) => true,
            (&Domain::Exists(true), &Domain::Exists(true)) => true,
            (&Domain::String, &Domain::String) => true,
            (&Domain::Number(a, b), &Domain::Number(x, y)) => {
                a <= x && b >= y
            },
            _ => false,
        }
    }

    pub fn merge(&mut self, other: &Domain) {

    }
}

//-------------------------------------------------------------------------
// Attribute Info
//-------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum ValueType {
    Number,
    String,
    Record,
    Any,
}

pub struct AttributeInfo {
    singleton: bool,
    types: HashSet<ValueType>,
    constraints: HashSet<(usize, Constraint)>,
    // outputs: HashSet<Constraint>,
}

impl AttributeInfo {
    pub fn new() -> AttributeInfo {
        let singleton = false;
        let types = HashSet::new();
        let constraints = HashSet::new();
        AttributeInfo { singleton, types, constraints }
    }
}

//-------------------------------------------------------------------------
// Tag Info
//-------------------------------------------------------------------------

pub struct TagInfo {
    attributes: HashMap<String, AttributeInfo>,
    other_tags: HashSet<String>,
    tag_relationships: HashSet<String>,
    external: bool,
    event: bool,
}

impl TagInfo {
    pub fn new() -> TagInfo {
        let attributes = HashMap::new();
        let other_tags = HashSet::new();
        let tag_relationships = HashSet::new();
        let external = false;
        let event = false;
        TagInfo { attributes, other_tags, tag_relationships, external, event }
    }
}

//-------------------------------------------------------------------------
// Block Info
//-------------------------------------------------------------------------

pub struct BlockInfo {
    id: Interned,
    has_scans: bool,
    constraints: Vec<Constraint>,
    field_to_tags: HashMap<Field, Vec<Interned>>,
    inputs: Vec<(Interned, Interned, Interned)>,
    outputs: Vec<(Interned, Interned, Interned)>,
}

impl BlockInfo {
    pub fn new(block: &Block) -> BlockInfo {
        let id = block.block_id;
        let constraints = block.constraints.clone();
        let field_to_tags = HashMap::new();
        let inputs = vec![];
        let outputs = vec![];
        let has_scans = false;
        BlockInfo { id, has_scans, constraints, field_to_tags, inputs, outputs }
    }

    pub fn gather_tags(&mut self) {
        let tag = TAG_INTERNED_ID;
        // find all the e -> tag mappings
        for scan in self.constraints.iter() {
            match scan {
                &Constraint::Scan {ref e, ref a, ref v, ..} |
                &Constraint::Insert {ref e, ref a, ref v, ..} |
                &Constraint::LookupCommit { ref e, ref a, ref v, ..} => {
                        let actual_a = if let &Field::Value(val) = a { val } else { 0 };
                        let actual_v = if let &Field::Value(val) = v { val } else { 0 };
                        if actual_a == tag && actual_v != 0 {
                            let mut tags = self.field_to_tags.entry(e.clone()).or_insert_with(|| vec![]);
                            tags.push(actual_v);
                        }
                    }
                _ => (),
            }
        }
    }

    pub fn gather_domains(&mut self) {
        let no_tags:Vec<Interned> = vec![];
        let mut field_domains:HashMap<Field, Domain> = HashMap::new();
        // determine the constraints per register
        // while changed
        //      for each constraint
        //          determine all the domains for the registers
        //          determine the domains for static attributes as well
        //          if there was a change
        //              set changed
        // go through the scans
        //      set the domain for (tag, attribute) pairs for inputs and outputs
        for scan in self.constraints.iter() {
            match scan {
                &Constraint::Scan {ref e, ref a, ref v, ..} |
                &Constraint::LookupCommit { ref e, ref a, ref v, ..} => {
                    if let &Field::Register(_) = e {
                        let domain = field_domains.entry(*e).or_insert_with(|| Domain::Unknown);
                        domain.merge(&Domain::Record);
                    }
                    if let &Field::Register(_) = a {
                        let domain = field_domains.entry(*a).or_insert_with(|| Domain::Unknown);
                        domain.merge(&Domain::String);
                    }
                    if let &Field::Register(_) = v {
                        let domain = field_domains.entry(*v).or_insert_with(|| Domain::Unknown);
                        domain.merge(&Domain::Exists(true));
                    }
                },
                &Constraint::Filter { ref left, ref right, ref op, .. } => {
                    match op.as_str() {
                        ">" => {

                        }
                        "<" => {}
                        ">=" => {}
                        "<=" => {}
                        _ => { }
                    }
                }
                _ => (),
            }
        }
    }

    pub fn gather_inputs_outputs(&mut self) {
        self.gather_tags();
        self.has_scans = false;
        self.inputs.clear();
        self.outputs.clear();
        let no_tags = vec![0];
        for scan in self.constraints.iter() {
            match scan {
                &Constraint::Scan {ref e, ref a, ref v, ..} |
                &Constraint::LookupCommit { ref e, ref a, ref v, ..} => {
                    self.has_scans = true;
                    let tags = self.field_to_tags.get(e).unwrap_or(&no_tags);
                    let actual_a = if let &Field::Value(val) = a { val } else { 0 };
                    let actual_v = if let &Field::Value(val) = v { val } else { 0 };
                    for tag in tags {
                        self.inputs.push((*tag, actual_a, actual_v));
                    }
                }
                &Constraint::Insert {ref e, ref a, ref v, ..} => {
                    let tags = self.field_to_tags.get(e).unwrap_or(&no_tags);
                    let actual_a = if let &Field::Value(val) = a { val } else { 0 };
                    let actual_v = if let &Field::Value(val) = v { val } else { 0 };
                    for tag in tags {
                        self.outputs.push((*tag, actual_a, actual_v));
                    }
                }
                &Constraint::Remove {ref e, ref a, ref v, ..} => {
                    let tags = self.field_to_tags.get(e).unwrap_or(&no_tags);
                    let actual_a = if let &Field::Value(val) = a { val } else { 0 };
                    let actual_v = if let &Field::Value(val) = v { val } else { 0 };
                    for tag in tags {
                        self.outputs.push((*tag, actual_a, actual_v));
                    }
                }
                &Constraint::RemoveAttribute {ref e, ref a, ..} => {
                    let tags = self.field_to_tags.get(e).unwrap_or(&no_tags);
                    let actual_a = if let &Field::Value(val) = a { val } else { 0 };
                    for tag in tags {
                        self.outputs.push((*tag, actual_a, 0));
                    }
                }
                &Constraint::RemoveEntity {ref e, ..} => {
                    let tags = self.field_to_tags.get(e).unwrap_or(&no_tags);
                    for tag in tags {
                        self.outputs.push((*tag, 0, 0));
                    }
                }
                _ => (),
            }
        }
    }
}

//-------------------------------------------------------------------------
// Chain node
//-------------------------------------------------------------------------

#[derive(Debug)]
pub struct Node {
    id: usize,
    block: Interned,
    next: HashSet<usize>,
    back_edges: HashSet<usize>,
}

//-------------------------------------------------------------------------
// Analysis
//-------------------------------------------------------------------------

pub struct Analysis {
    blocks: HashMap<Interned, BlockInfo>,
    inputs: HashMap<(Interned, Interned, Interned), HashSet<Interned>>,
    setup_blocks: Vec<Interned>,
    root_blocks: HashSet<Interned>,
    tags: HashMap<String, TagInfo>,
    externals: HashSet<Interned>,
    chains: Vec<usize>,
    nodes: Vec<Node>,
    dirty_blocks: Vec<Interned>,
}

impl Analysis {
    pub fn new(interner: &mut Interner) -> Analysis {
        let blocks = HashMap::new();
        let tags = HashMap::new();
        let chains = vec![];
        let nodes = vec![];
        let dirty_blocks = vec![];
        let inputs = HashMap::new();
        let setup_blocks = vec![];
        let root_blocks = HashSet::new();
        let mut external_tags = vec![];
        external_tags.push("system/timer/change");
        let mut externals = HashSet::new();
        externals.extend(external_tags.iter().map(|x| interner.string_id(x)));
        Analysis { blocks, tags, dirty_blocks, inputs, setup_blocks, root_blocks, externals, chains, nodes }
    }

    pub fn block(&mut self, block: &Block) {
        let id = block.block_id;
        self.blocks.insert(id, BlockInfo::new(block));
        self.dirty_blocks.push(id);
    }

    pub fn analyze(&mut self) {
        println!("\n-----------------------------------------------------------");
        println!("\nAnalysis starting...");
        println!("  Dirty blocks: {:?}", self.dirty_blocks);

        for block_id in self.dirty_blocks.drain(..) {
            let block = self.blocks.get_mut(&block_id).unwrap();
            block.gather_inputs_outputs();
            for input in block.inputs.iter() {
                let entry = self.inputs.entry(input.clone()).or_insert_with(|| HashSet::new());
                entry.insert(block.id);
                if self.externals.contains(&input.0) {
                    self.root_blocks.insert(block.id);
                }
            }
            if !block.has_scans {
                self.setup_blocks.push(block.id);
            }
        }

        let mut chains = vec![];
        let mut nodes = vec![];
        let mut seen = HashMap::new();
        let mut node_ix = 0;
        for setup in self.setup_blocks.iter().cloned() {
            seen.clear();
            chains.push(self.build_chain(setup, &mut nodes, &mut seen, &mut node_ix));
        }
        for root in self.root_blocks.iter().cloned() {
            seen.clear();
            chains.push(self.build_chain(root, &mut nodes, &mut seen, &mut node_ix));
        }
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        self.chains.extend(chains);
        self.nodes.extend(nodes);
        println!("NODES: {:?}", self.nodes);
    }

    pub fn build_chain(&self, root_block:Interned, nodes: &mut Vec<Node>, seen: &mut HashMap<Interned, usize>, next_ix:&mut usize) -> usize {
        let mut root = Node { id: *next_ix, block:root_block, next: HashSet::new(), back_edges: HashSet::new() };
        *next_ix += 1;
        seen.insert(root_block, root.id);
        let block = self.blocks.get(&root_block).unwrap();
        let mut followers = HashSet::new();
        for output in block.outputs.iter() {
            if let Some(nexts) = self.inputs.get(output) {
                followers.extend(nexts);
            }
        }
        for next in followers.iter().cloned() {
            match seen.get(&next).cloned() {
                Some(edge) => {
                    root.back_edges.insert(edge);
                },
                _ => {
                    let next_id = self.build_chain(next, nodes, seen, next_ix);
                    root.next.insert(next_id);
                }
            }
        }
        seen.remove(&root_block);
        let id = root.id;
        nodes.push(root);
        id
    }

    pub fn dot_chain_link(&self, node_id:usize, graph:&mut String) {
        let me = &self.nodes[node_id];
        graph.push_str(&format!("{:?} [label=\"{:?}\"]\n", me.id, me.block));
        for next in me.next.iter().cloned() {
            graph.push_str(&format!("{:?} -> {:?};\n", me.id, next));
            self.dot_chain_link(next, graph);
        }
        for next in me.back_edges.iter().cloned() {
            graph.push_str(&format!("{:?} -> {:?};\n", me.id, next));
        }
    }

    pub fn make_dot_chains(&self) -> String {
        let mut graph = "digraph program {\n".to_string();
        for chain in self.chains.iter().cloned() {
            self.dot_chain_link(chain, &mut graph);
        }
        graph.push_str("}");
        graph
    }

    pub fn make_dot_graph(&self) -> String {
        let mut graph = "digraph program {\n".to_string();
        let mut followers:HashSet<Interned> = HashSet::new();
        for block in self.blocks.values() {
            followers.clear();
            for output in block.outputs.iter() {
                if let Some(nexts) = self.inputs.get(output) {
                    followers.extend(nexts.iter());
                }
            }
            for next in followers.iter() {
                graph.push_str(&format!("{:?} -> {:?};\n", block.id, next));
            }
        }
        graph.push_str("}");
        graph
    }
}

