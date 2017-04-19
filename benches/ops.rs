#![feature(test)]

extern crate test;
extern crate eve;

use eve::ops::*;
use eve::indexes::{DistinctIter};
use test::Bencher;

#[bench]
pub fn round_holder_compute_output_rounds_bench(b:&mut Bencher) {
    let mut holder = RoundHolder::new();
    let rounds = vec![1,-1,0,0,1,0,-1];
    holder.output_rounds = vec![(3,1), (5,1)];
    b.iter(|| {
        let iter = DistinctIter::new(&rounds);
        holder.compute_output_rounds(iter);
    });
}

#[bench]
fn bench_simple_gj(b:&mut Bencher) {
    // prog.block("simple block", ({find, record, lib}) => {
    //  let person = find("person");
    //  let text = `name: ${person.name}`;
    //  return [
    //    record("html/div", {person, text})
    //  ]
    // });
    //
    let mut program = Program::new();
    let constraints = vec![
        make_scan(register(0), program.interner.string("tag"), program.interner.string("person")),
        make_scan(register(0), program.interner.string("name"), register(1)),
        make_function("concat", vec![program.interner.string("name: "), register(1)], vec![register(2)]),
        make_function("gen_id", vec![register(0), register(2)], vec![register(3)]),
        // Constraint::Insert {e: register(3), a: int.string("tag"), v: int.string("html/div")},
        // Constraint::Insert {e: register(3), a: int.string("person"), v: register(0)},
        // Constraint::Insert {e: register(3), a: int.string("text"), v: register(2)},
        Constraint::Insert {e: program.interner.string("foo"), a: program.interner.string("tag"), v: program.interner.string("html/div")},
        Constraint::Insert {e: program.interner.string("foo"), a: program.interner.string("person"), v: register(0)},
        Constraint::Insert {e: program.interner.string("foo"), a: program.interner.string("text"), v: register(1)},
    ];
    program.register_block(Block { name: "simple block".to_string(), constraints, pipes: vec![] });

        let mut ix = 0;
        b.iter(|| {
            let mut txn = Transaction::new();
            txn.input(program.interner.number_id(ix as f32), program.interner.string_id("tag"), program.interner.string_id("person"), 1);
            txn.input(program.interner.number_id(ix as f32), program.interner.string_id("name"), program.interner.number_id(ix as f32), 1);
            txn.exec(&mut program);
            ix += 1;
        });
        // println!("Size: {:?}", program.index.size);
}
