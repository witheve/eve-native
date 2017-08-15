/* ;; #[bench] */
/* ;; fn rust_balls_raw(b: &mut Bencher) { */
/* ;;     let mut raw_boid = vec![]; */
/* ;;     for order in 1..200 { */
/* ;;         let rand = random(order as u32); */
/* ;;         let rand2 = random(order as u32); */
/* ;;         let x = rand * 500.0; */
/* ;;         let y = rand2 * 500.0; */
/* ;;         let vx = rand * 3.0 + 1.0; */
/* ;;         let vy = rand2 * 4.0 + 1.0; */
/* ;;         raw_boid.push(RawBoid { x, y, vx, vy, arc: RawArc { x, y } }); */
/* ;;     } */
/* ;; */
/* ;;     b.iter(move || { */
/* ;;         for boid in raw_boid.iter_mut() { */
/* ;;             boid.x += boid.vx; */
/* ;;             boid.y += boid.vy; */
/* ;;             boid.vy += 0.07; */
/* ;;             if boid.y < 10.0 && boid.vy < 0.0 { */
/* ;;                 boid.vy *= -0.9; */
/* ;;             } */
/* ;;             if boid.x < 10.0 && boid.vx < 0.0 { */
/* ;;                 boid.vx *= -0.9; */
/* ;;             } */
/* ;;             if boid.y > 490.0 && boid.vy > 0.0 { */
/* ;;                 boid.vy *= -0.9; */
/* ;;             } */
/* ;;             if boid.x > 490.0 && boid.vx > 0.0 { */
/* ;;                 boid.vx *= -0.9; */
/* ;;             } */
/* ;;             boid.arc.x = boid.x; */
/* ;;             boid.arc.y = boid.y; */
/* ;;         } */
/* ;;     }); */
/* ;; } */


/* (component boid x number vx number y number vy number) */

/*   (for [i (range 1 200)] */
/*    (let [rand (random i) */
/*          rand2 (random (* i 2)) */
/*          x (* rand 500) */
/*          y (* rand 400) */
/*          vx 10 */
/*          vy 10 */
/*          entity (make-entity)] */
/*      (make-component boid x vx y vy))) */

/*   (on event/tick */
/*     (for [boid (get-components boid)] */
/*      (set! boid.x (+ boid.vx boid.x)) */
/*      (set! boid.y (+ boid.vy boid.y)) */
/*      (set! boid.vy (+ boid.vy 0.07)) */
/*      (if (and (< boid.y 10) (boid.vy < 0)) */
/*       (set! boid.vy (* boid.vy -0.09))) */
/*      (if (and (< boid.y 10) (boid.vy < 0)) */
/*       (set! boid.vy (* boid.vy -0.09))) */
/*      (if (and (< boid.y 10) (boid.vy < 0)) */
/*       (set! boid.vy (* boid.vy -0.09))) */
/*      (if (and (< boid.y 10) (boid.vy < 0)) */
/*       (set! boid.vy (* boid.vy -0.09))) */
/*      )) */

use ops::{Field, Interned, Constraint, Block, TAG_INTERNED_ID};
use std::collections::{HashSet, HashMap};

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

pub struct BlockInfo {
    id: Interned,
    constraints: Vec<Constraint>,
    field_to_tags: HashMap<Field, Vec<Interned>>,
}

impl BlockInfo {
    pub fn new(block: &Block) -> BlockInfo {
        let id = block.block_id;
        let constraints = block.constraints.clone();
        let field_to_tags = HashMap::new();
        BlockInfo { id, constraints, field_to_tags }
    }

    pub fn gather_tags(&mut self) {
        let tag = TAG_INTERNED_ID;
        // find all the e -> tag mappings
        for scan in self.constraints.iter() {
            match scan {
                &Constraint::Scan {ref e, ref a, ref v, ..} |
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
}

pub struct Analysis {
    blocks: HashMap<Interned, BlockInfo>,
    tags: HashMap<String, TagInfo>,
    dirty_blocks: Vec<Interned>,
}

impl Analysis {
    pub fn new() -> Analysis {
        let blocks = HashMap::new();
        let tags = HashMap::new();
        let dirty_blocks = vec![];
        Analysis { blocks, tags, dirty_blocks }
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

        for block in self.dirty_blocks.drain(..) {
            // self.analyze_block(self.blocks.get(&block).unwrap());
        }
    }
}

// let info = HashMap::new();
// info.insert(("boid", "x"), )
// info.insert(("boid", "vx"), )
// info.insert(("boid", "y"), )
// info.insert(("boid", "vy"), )
// info.insert(("boid", "order"), )

// info.insert(("arc", "x"), )
// info.insert(("arc", "y"), )
// info.insert(("arc", "type"), )
// info.insert(("arc", "sort"), )
// info.insert(("arc", "radius"), )
// info.insert(("arc", "startAngle"), )
// info.insert(("arc", "endAngle"), )

// info.insert(("system/timer/change", "tick"), )



