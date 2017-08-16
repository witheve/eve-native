use ops::{Field, Interned, Constraint, Block, TAG_INTERNED_ID, Interner, Internable};
use std::collections::{HashSet, HashMap, Bound};
use std::collections::Bound::{Unbounded, Excluded, Included};
use std::mem::transmute;

//-------------------------------------------------------------------------
// Domain
//-------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Domain {
    Unknown,
    Number(Bound<u64>, Bound<u64>),
    String,
    Record,
    MultiType,
    Removed,
}

impl Domain {
    pub fn intersects(&self, other: &Domain) -> bool {
        match (self, other) {
            (&Domain::Removed, _) => false,
            (&Domain::Unknown, _) => true,
            (_, &Domain::Unknown) => true,
            (&Domain::String, &Domain::String) => true,
            (&Domain::Number(a, b), &Domain::Number(x, y)) => {
                a.lte(&x) && b.gte(&y)
            },
            _ => false,
        }
    }

    pub fn merge(&mut self, other: &Domain) {
        let neue = match (self.clone(), other) {
            (Domain::Unknown, _) => other.clone(),
            (_, &Domain::Unknown) => self.clone(),
            (Domain::Removed, _) => self.clone(),
            (_, &Domain::Removed) => other.clone(),
            (Domain::Number(a, b), &Domain::Number(x, y)) => {
                Domain::Number(a.shrink_left(&x), b.shrink_right(&y))
            },
            (a, b) => {
                if &a == b {
                    a
                } else {
                    Domain::MultiType
                }
            },
        };
        *self = neue;
    }
}

fn to_float(num: u64) -> f64 {
    unsafe { transmute::<u64, f64>(num) }
}

fn from_float(num: f64) -> u64 {
    unsafe { transmute::<f64, u64>(num) }
}

trait BoundMath {
    fn add(&self, b: f64) -> Self;
    fn subtract(&self, b: f64) -> Self;
    fn multiply(&self, b: f64) -> Self;
    fn divide(&self, b: f64) -> Self;
    fn unwrap(&self) -> u64;
    fn shrink_left(&self, other: &Self) -> Self;
    fn shrink_right(&self, other: &Self) -> Self;
    fn lte(&self, other: &Self) -> bool;
    fn gte(&self, other: &Self) -> bool;
}

impl BoundMath for Bound<u64> {
    fn add(&self, b: f64) -> Bound<u64> {
        match self {
            &Included(v) => Included(from_float(to_float(v) + b)),
            &Excluded(v) => Excluded(from_float(to_float(v) + b)),
            &Unbounded => Unbounded,
        }
    }

    fn subtract(&self, b: f64) -> Bound<u64> {
        match self {
            &Included(v) => Included(from_float(to_float(v) - b)),
            &Excluded(v) => Excluded(from_float(to_float(v) - b)),
            &Unbounded => Unbounded,
        }
    }

    fn multiply(&self, b: f64) -> Bound<u64> {
        match self {
            &Included(v) => Included(from_float(to_float(v) * b)),
            &Excluded(v) => Excluded(from_float(to_float(v) * b)),
            &Unbounded => Unbounded,
        }
    }

    fn divide(&self, b: f64) -> Bound<u64> {
        match self {
            &Included(v) => Included(from_float(to_float(v) / b)),
            &Excluded(v) => Excluded(from_float(to_float(v) / b)),
            &Unbounded => Unbounded,
        }
    }

    fn unwrap(&self) -> u64 {
        match self {
            &Included(v) => v,
            &Excluded(v) => v,
            &Unbounded => panic!("Unwrapped an unbounded"),
        }
    }

    fn shrink_left(&self, other: &Self) -> Self {
        match (self, other) {
            (&Unbounded, _) => other.clone(),
            (_, &Unbounded) => self.clone(),
            (&Included(a), &Included(b)) => {
                if to_float(a) >= to_float(b) {
                   self.clone()
                } else {
                    other.clone()
                }
            }
            (&Excluded(a), _) => {
                if to_float(a) > to_float(other.unwrap()) {
                    self.clone()
                } else {
                    other.clone()
                }
            }
            (_, &Excluded(b)) => {
                if to_float(b) > to_float(self.unwrap()) {
                    self.clone()
                } else {
                    other.clone()
                }
            }
        }
    }

    fn shrink_right(&self, other: &Self) -> Self {
        match (self, other) {
            (&Unbounded, _) => other.clone(),
            (_, &Unbounded) => self.clone(),
            (&Included(a), &Included(b)) => {
                if to_float(a) <= to_float(b) {
                   self.clone()
                } else {
                    other.clone()
                }
            }
            (&Excluded(a), _) => {
                if to_float(a) < to_float(other.unwrap()) {
                    self.clone()
                } else {
                    other.clone()
                }
            }
            (_, &Excluded(b)) => {
                if to_float(b) < to_float(self.unwrap()) {
                    self.clone()
                } else {
                    other.clone()
                }
            }
        }
    }

    fn lte(&self, other: &Self) -> bool {
        match (self, other) {
            (&Unbounded, _) => true,
            (_, &Unbounded) => true,
            (&Included(a), &Included(b)) => { to_float(a) <= to_float(b) }
            _ => { to_float(self.unwrap()) < to_float(other.unwrap()) }
        }
    }

    fn gte(&self, other: &Self) -> bool {
        match (self, other) {
            (&Unbounded, _) => true,
            (_, &Unbounded) => true,
            (&Included(a), &Included(b)) => { to_float(a) >= to_float(b) }
            _ => { to_float(self.unwrap()) > to_float(other.unwrap()) }
        }
    }
}

pub fn add_domain(a: &Domain, b: f64) -> Domain {
    if let &Domain::Number(start, stop) = a {
        Domain::Number(start.add(b), stop.add(b))
    } else {
        panic!("Domain math on non-number");
    }
}

pub fn subtract_domain(a: &Domain, b: f64) -> Domain {
    if let &Domain::Number(start, stop) = a {
        Domain::Number(start.subtract(b), stop.subtract(b))
    } else {
        panic!("Domain math on non-number");
    }
}

pub fn multiply_domain(a: &Domain, b: f64) -> Domain {
    if let &Domain::Number(start, stop) = a {
        Domain::Number(start.multiply(b), stop.multiply(b))
    } else {
        panic!("Domain math on non-number");
    }
}

pub fn divide_domain(a: &Domain, b: f64) -> Domain {
    if let &Domain::Number(start, stop) = a {
        Domain::Number(start.divide(b), stop.divide(b))
    } else {
        panic!("Domain math on non-number");
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
    input_domains: HashMap<(Interned, Interned), Domain>,
    output_domains: HashMap<(Interned, Interned), Vec<Domain>>,
}

impl BlockInfo {
    pub fn new(block: &Block) -> BlockInfo {
        let id = block.block_id;
        let constraints = block.constraints.clone();
        let field_to_tags = HashMap::new();
        let inputs = vec![];
        let outputs = vec![];
        let input_domains = HashMap::new();
        let output_domains = HashMap::new();
        let has_scans = false;
        BlockInfo { id, has_scans, constraints, field_to_tags, inputs, outputs, input_domains, output_domains }
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

    pub fn gather_domains(&mut self, interner: &Interner) -> HashMap<Field, Domain> {
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
                    if e.is_register() {
                        merge_field_domain(e, &mut field_domains, Domain::Record);
                    }
                    if a.is_register() {
                        merge_field_domain(a, &mut field_domains, Domain::String);
                    }
                    if v.is_register() {
                        merge_field_domain(v, &mut field_domains, Domain::Unknown);
                    }
                },
                &Constraint::Filter { ref left, ref right, ref op, .. } => {
                    match op.as_str() {
                        "=" => {
                            let to_merge = field_to_domain(interner, right, &field_domains);
                            merge_field_domain(left, &mut field_domains, to_merge);
                        }
                        ">" => {
                            match (left.is_register(), right.is_register()) {
                                (true, false) => {
                                    let to_merge = match field_to_domain(interner, right, &field_domains) {
                                        Domain::Number(start, stop) => Domain::Number(Excluded(start.unwrap()), Unbounded),
                                        a => a,
                                    };
                                    merge_field_domain(left, &mut field_domains, to_merge);
                                }
                                (false, true) => {
                                    let to_merge = match field_to_domain(interner, left, &field_domains) {
                                        Domain::Number(start, stop) => Domain::Number(Unbounded, Excluded(start.unwrap())),
                                        a => a,
                                    };
                                    merge_field_domain(right, &mut field_domains, to_merge);
                                }
                                (true, true) => {
                                    // @TODO
                                    unimplemented!()
                                }
                                (false, false) => {
                                    // huh?
                                }
                            }
                        }
                        "<" => {
                            match (left.is_register(), right.is_register()) {
                                (true, false) => {
                                    let to_merge = match field_to_domain(interner, right, &field_domains) {
                                        Domain::Number(start, stop) => Domain::Number(Unbounded, Excluded(start.unwrap())),
                                        a => a,
                                    };
                                    merge_field_domain(left, &mut field_domains, to_merge);
                                }
                                (false, true) => {
                                    let to_merge = match field_to_domain(interner, left, &field_domains) {
                                        Domain::Number(start, stop) => Domain::Number(Excluded(start.unwrap()), Unbounded),
                                        a => a,
                                    };
                                    merge_field_domain(right, &mut field_domains, to_merge);
                                }
                                (true, true) => {
                                    // @TODO
                                    unimplemented!()
                                }
                                (false, false) => {
                                    // huh?
                                }
                            }
                        }
                        ">=" => {
                            match (left.is_register(), right.is_register()) {
                                (true, false) => {
                                    let to_merge = match field_to_domain(interner, right, &field_domains) {
                                        Domain::Number(start, stop) => Domain::Number(Included(start.unwrap()), Unbounded),
                                        a => a,
                                    };
                                    merge_field_domain(left, &mut field_domains, to_merge);
                                }
                                (false, true) => {
                                    let to_merge = match field_to_domain(interner, left, &field_domains) {
                                        Domain::Number(start, stop) => Domain::Number(Unbounded, Included(start.unwrap())),
                                        a => a,
                                    };
                                    merge_field_domain(right, &mut field_domains, to_merge);
                                }
                                (true, true) => {
                                    // @TODO
                                    unimplemented!()
                                }
                                (false, false) => {
                                    // huh?
                                }
                            }
                        }
                        "<=" => {
                            match (left.is_register(), right.is_register()) {
                                (true, false) => {
                                    let to_merge = match field_to_domain(interner, right, &field_domains) {
                                        Domain::Number(start, stop) => Domain::Number(Unbounded, Included(start.unwrap())),
                                        a => a,
                                    };
                                    merge_field_domain(left, &mut field_domains, to_merge);
                                }
                                (false, true) => {
                                    let to_merge = match field_to_domain(interner, left, &field_domains) {
                                        Domain::Number(start, stop) => Domain::Number(Included(start.unwrap()), Unbounded),
                                        a => a,
                                    };
                                    merge_field_domain(right, &mut field_domains, to_merge);
                                }
                                (true, true) => {
                                    // @TODO
                                    unimplemented!()
                                }
                                (false, false) => {
                                    // huh?
                                }
                            }

                        }
                        _ => { }
                    }
                }
                _ => (),
            }
        }
        field_domains
    }

    pub fn gather_inputs_outputs(&mut self, interner: &Interner) {
        self.gather_tags();
        self.has_scans = false;
        self.inputs.clear();
        self.outputs.clear();
        self.input_domains.clear();
        self.output_domains.clear();
        let field_domains = self.gather_domains(interner);
        let no_tags = vec![0];
        for scan in self.constraints.iter() {
            match scan {
                &Constraint::Scan {ref e, ref a, ref v, ..} |
                &Constraint::LookupCommit { ref e, ref a, ref v, ..} => {
                    self.has_scans = true;
                    let tags = self.field_to_tags.get(e).unwrap_or(&no_tags);
                    let actual_a = if let &Field::Value(val) = a { val } else { 0 };
                    let actual_v = if let &Field::Value(val) = v { val } else { 0 };
                    if actual_a == TAG_INTERNED_ID {
                        self.inputs.push((0, actual_a, actual_v));
                    }
                    for tag in tags {
                        self.inputs.push((*tag, actual_a, actual_v));
                        merge_tag_domain(interner, &mut self.input_domains, &field_domains, *tag, actual_a, v);
                    }
                }
                &Constraint::Insert {ref e, ref a, ref v, ..} => {
                    let tags = self.field_to_tags.get(e).unwrap_or(&no_tags);
                    let actual_a = if let &Field::Value(val) = a { val } else { 0 };
                    let actual_v = if let &Field::Value(val) = v { val } else { 0 };
                    if actual_a == TAG_INTERNED_ID {
                        self.outputs.push((0, actual_a, actual_v));
                    }
                    for tag in tags {
                        self.outputs.push((*tag, actual_a, actual_v));
                        merge_output_domain(interner, &mut self.output_domains, &field_domains, *tag, actual_a, v, false);
                    }
                }
                &Constraint::Remove {ref e, ref a, ref v, ..} => {
                    let tags = self.field_to_tags.get(e).unwrap_or(&no_tags);
                    let actual_a = if let &Field::Value(val) = a { val } else { 0 };
                    let actual_v = if let &Field::Value(val) = v { val } else { 0 };
                    if actual_a == TAG_INTERNED_ID {
                        self.outputs.push((0, actual_a, actual_v));
                    }
                    for tag in tags {
                        self.outputs.push((*tag, actual_a, actual_v));
                        merge_output_domain(interner, &mut self.output_domains, &field_domains, *tag, actual_a, v, true);
                    }
                }
                &Constraint::RemoveAttribute {ref e, ref a, ..} => {
                    let tags = self.field_to_tags.get(e).unwrap_or(&no_tags);
                    let actual_a = if let &Field::Value(val) = a { val } else { 0 };
                    if actual_a == TAG_INTERNED_ID {
                        self.outputs.push((0, actual_a, 0));
                    }
                    for tag in tags {
                        self.outputs.push((*tag, actual_a, 0));
                        merge_output_domain(interner, &mut self.output_domains, &field_domains, *tag, actual_a, &Field::Value(0), true);
                    }
                }
                &Constraint::RemoveEntity {ref e, ..} => {
                    let tags = self.field_to_tags.get(e).unwrap_or(&no_tags);
                    for tag in tags {
                        self.outputs.push((*tag, 0, 0));
                        self.outputs.push((*tag, TAG_INTERNED_ID, *tag));
                        merge_output_domain(interner, &mut self.output_domains, &field_domains, *tag, TAG_INTERNED_ID, &Field::Value(0), true);
                    }
                }
                _ => (),
            }
        }
        println!("INPUTS: {:?}", self.inputs);
        println!("OUTPUTS: {:?}", self.outputs);
        println!("INPUT DOMAINS: {:?}", self.input_domains);
        println!("OUTPUT DOMAINS: {:?}", self.output_domains);
    }
}

pub fn field_to_domain(interner:&Interner, field:&Field, field_domains:&HashMap<Field, Domain>) -> Domain {
    if let &Field::Value(v) = field {
        match interner.get_value(v) {
            &Internable::String(_) => { Domain::String },
            &Internable::Number(num) => {
                Domain::Number(Included(num as u64), Included(num as u64))
            },
            &Internable::Null => { panic!("Got a null field!") }
        }
    } else {
        field_domains.get(field).cloned().unwrap_or(Domain::Unknown)
    }
}

pub fn merge_field_domain(field:&Field, field_domains:&mut HashMap<Field, Domain>, to_merge:Domain) {
    let domain = field_domains.entry(*field).or_insert_with(|| Domain::Unknown);
    domain.merge(&to_merge);
}

pub fn merge_tag_domain(interner:&Interner, tag_domains:&mut HashMap<(Interned, Interned), Domain>, field_domains:&HashMap<Field, Domain>, tag:Interned, attribute:Interned, field:&Field) {
    let domain = tag_domains.entry((tag, attribute)).or_insert_with(|| Domain::Unknown);
    domain.merge(&field_to_domain(interner, field, field_domains));
}

pub fn merge_output_domain(interner:&Interner, tag_domains:&mut HashMap<(Interned, Interned), Vec<Domain>>, field_domains:&HashMap<Field, Domain>, tag:Interned, attribute:Interned, field:&Field, remove:bool) {
    let domains = tag_domains.entry((tag, attribute)).or_insert_with(|| vec![]);
    if remove {
        domains.push(Domain::Removed);
    } else {
        let mut field_domain = field_to_domain(interner, field, field_domains);
        domains.push(field_domain);
    }
}

//-------------------------------------------------------------------------
// Chain node
//-------------------------------------------------------------------------

#[derive(Debug)]
pub struct Node {
    id: usize,
    block: Interned,
    input: Interned,
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
    root_blocks: HashMap<Interned, HashSet<Interned>>,
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
        let root_blocks = HashMap::new();
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

    pub fn analyze(&mut self, interner: &Interner) {
        println!("\n-----------------------------------------------------------");
        println!("\nAnalysis starting...");
        println!("  Dirty blocks: {:?}", self.dirty_blocks);

        for block_id in self.dirty_blocks.drain(..) {
            let block = self.blocks.get_mut(&block_id).unwrap();
            block.gather_inputs_outputs(interner);
            for input in block.inputs.iter() {
                let entry = self.inputs.entry(input.clone()).or_insert_with(|| HashSet::new());
                entry.insert(block.id);
                if self.externals.contains(&input.0) {
                    let entry = self.root_blocks.entry(input.0).or_insert_with(|| HashSet::new());
                    entry.insert(block.id);
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
        for (input_tag, roots) in self.root_blocks.iter() {
            let id = node_ix;
            let mut input_root = Node { id, block:0, input:*input_tag, next: HashSet::new(), back_edges: HashSet::new() };
            node_ix += 1;
            for root in roots.iter().cloned() {
                seen.clear();
                input_root.next.insert(self.build_chain(root, &mut nodes, &mut seen, &mut node_ix));
            }
            chains.push(id);
            nodes.push(input_root);
        }
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        self.nodes.extend(nodes);
        println!("NODES: {:?}", self.nodes);
        for chain in chains.iter().cloned() {
            self.optimize_chain(chain);
            self.chains.push(chain);
        }
    }

    pub fn optimize_chain(&mut self, chain_id:usize) {
        let mut keep = HashSet::new();
        let mut parents = vec![chain_id];
        let mut parents_next = vec![];
        let mut frame_state:HashMap<(Interned, Interned), Vec<Domain>> = HashMap::new();
        frame_state.insert((self.nodes[chain_id].input, TAG_INTERNED_ID), vec![Domain::String]);

        println!("OPTIMIZING ---------------------------------------");

        while parents.len() > 0 {
            for parent_id in parents.iter() {
                keep.clear();
                {
                    let parent = &self.nodes[*parent_id];
                    let output_domains = self.blocks.get(&parent.block).map(|x| &x.output_domains).unwrap_or(&frame_state);
                    'outer: for next in parent.next.iter().chain(parent.back_edges.iter()).cloned() {
                        println!("CHECKING: {:?}", next);
                        let node = &self.nodes[next];
                        let block = self.blocks.get(&node.block).unwrap();
                        for (input, domain) in block.input_domains.iter() {
                            println!("   input: {:?} {:?}", input, domain);
                            match output_domains.get(&input) {
                                Some(output_domains) => {
                                    for output_domain in output_domains {
                                        println!("      intersects?: {:?}", output_domain);
                                        if output_domain.intersects(&domain) {
                                            keep.insert(next);
                                            continue 'outer;
                                        }
                                    }
                                }
                                _ => {  }
                            }
                        }
                    }
                }
                let parent = self.nodes.get_mut(*parent_id).unwrap();
                parent.next.retain(|x| keep.contains(x));
                parent.back_edges.retain(|x| keep.contains(x));
            }

            frame_state.clear();

            for parent_id in parents.iter() {
                let parent = &self.nodes[*parent_id];
                for next in parent.next.iter().cloned() {
                    parents_next.push(next);
                    let node = &self.nodes[next];
                    let block = self.blocks.get(&node.block).unwrap();
                    for (output, domains) in block.output_domains.iter() {
                        let entry = frame_state.entry(output.clone()).or_insert_with(|| vec![]);
                        for domain in domains {
                            entry.push(domain.clone());
                        }
                    }
                }
            }

            parents.clear();
            parents.extend(parents_next.drain(..));

            println!("  FRAME --------------------------------------------");

        }

    }

    pub fn build_chain(&self, root_block:Interned, nodes: &mut Vec<Node>, seen: &mut HashMap<Interned, usize>, next_ix:&mut usize) -> usize {
        let mut root = Node { id: *next_ix, block:root_block, input:0, next: HashSet::new(), back_edges: HashSet::new() };
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
