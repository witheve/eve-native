#![feature(test)]

extern crate test;
extern crate eve;

use eve::ops::*;
use eve::instructions::*;
use eve::indexes::{DistinctIter};
use test::Bencher;

#[bench]
pub fn round_holder_compute_output_rounds_bench(b:&mut Bencher) {
    let mut holder = OutputRounds::new();
    let rounds = vec![1,-1,0,0,1,0,-1];
    holder.output_rounds = vec![(3,1), (5,1)];
    b.iter(|| {
        let iter = DistinctIter::new(&rounds);
        holder.compute_output_rounds(iter);
    });
}

fn test_pipe(b: &mut Bencher, constraints: Vec<Constraint>, instructions: Vec<Instruction>) {
    let mut program = Program::new();
    program.block_info.blocks.push(Block { name: "foo".to_string(), constraints, pipes:vec![], shapes:vec![] });
    let mut pool = EstimateIterPool::new();
    let mut frame = Frame::new();
    frame.input = Some(Change {e:0, a:0, v:0, n:0, round:0, count:1, transaction:0});
    frame.block_ix = 0;

    b.iter(|| {
        interpret(&mut program.state, &program.block_info, &mut pool, &mut frame, &instructions);
    });
}

fn v(cur:u32) -> Field {
    Field::Value(cur)
}

fn test_closure(constraints: Vec<Constraint>) -> (Program, EstimateIterPool, Frame) {
    let mut program = Program::new();
    program.block_info.blocks.push(Block { name: "foo".to_string(), constraints, pipes:vec![], shapes:vec![] });
    let pool = EstimateIterPool::new();
    let mut frame = Frame::new();
    frame.input = Some(Change {e:0, a:0, v:0, n:0, round:0, count:1, transaction:0});
    frame.block_ix = 0;
    (program, pool, frame)

}

#[bench]
pub fn ops_bind_pipe(b:&mut Bencher) {
    test_pipe(b, vec![
        Constraint::Insert { e:v(1), a:v(2), v:v(3), commit:false },
        Constraint::Insert { e:v(1), a:v(4), v:v(8), commit:false },
        Constraint::Insert { e:v(1), a:v(5), v:v(9), commit:false },
        Constraint::Insert { e:v(1), a:v(6), v:v(10), commit:false },
        Constraint::Insert { e:v(9), a:v(2), v:v(3), commit:false },
        Constraint::Insert { e:v(9), a:v(4), v:v(8), commit:false },
        Constraint::Insert { e:v(9), a:v(5), v:v(9), commit:false },
        Constraint::Insert { e:v(9), a:v(6), v:v(10), commit:false }
    ], vec![
        Instruction::ClearRounds,
        Instruction::Bind { next: 1, constraint: 0 },
        Instruction::Bind { next: 1, constraint: 1 },
        Instruction::Bind { next: 1, constraint: 2 },
        Instruction::Bind { next: 1, constraint: 3 },
        Instruction::Bind { next: 1, constraint: 4 },
        Instruction::Bind { next: 1, constraint: 5 },
        Instruction::Bind { next: 1, constraint: 6 },
        Instruction::Bind { next: 1, constraint: 7 },
    ]);
}

#[bench]
pub fn ops_bind_closure_pipe(b:&mut Bencher) {
    let constraints = vec![
        Constraint::Insert { e:v(1), a:v(2), v:v(3), commit:false },
        Constraint::Insert { e:v(1), a:v(4), v:v(8), commit:false },
        Constraint::Insert { e:v(1), a:v(5), v:v(9), commit:false },
        Constraint::Insert { e:v(1), a:v(6), v:v(10), commit:false },
        Constraint::Insert { e:v(9), a:v(2), v:v(3), commit:false },
        Constraint::Insert { e:v(9), a:v(4), v:v(8), commit:false },
        Constraint::Insert { e:v(9), a:v(5), v:v(9), commit:false },
        Constraint::Insert { e:v(9), a:v(6), v:v(10), commit:false }
    ];
    let func = make_bind_instruction(&constraints.iter().collect(), 1);
    let (mut program, _, mut frame) = test_closure(constraints);
    b.iter(|| {
        clear_rounds(&mut program.state.output_rounds, &mut frame);
        (func.0)(&mut program.state.distinct_index, &program.state.output_rounds, &mut program.state.rounds, &mut frame);
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
    // let mut program = Program::new();
    // let constraints = vec![
    //     make_scan(register(0), program.state.interner.string("tag"), program.state.interner.string("person")),
    //     make_scan(register(0), program.state.interner.string("name"), register(1)),
    //     make_function("concat", vec![program.state.interner.string("name: "), register(1)], register(2)),
    //     make_function("gen_id", vec![register(0), register(2)], register(3)),
    //     // Constraint::Insert {e: program.state.interner.string("foo"), a: program.state.interner.string("tag"), v: program.state.interner.string("html/div")},
    //     // Constraint::Insert {e: program.state.interner.string("foo"), a: program.state.interner.string("person"), v: register(0)},
    //     // Constraint::Insert {e: program.state.interner.string("foo"), a: program.state.interner.string("text"), v: register(1)},
    //     Constraint::Insert {e: register(3), a: program.state.interner.string("tag"), v: program.state.interner.string("html/div"), commit: false},
    //     Constraint::Insert {e: register(3), a: program.state.interner.string("person"), v: register(0), commit: false},
    //     Constraint::Insert {e: register(3), a: program.state.interner.string("text"), v: register(2), commit: false},
    // ];
    // program.register_block(Block::new("simple_block", constraints));

    //     let mut ix = 0;
    //     let mut txn = Transaction::new();
    //     b.iter(|| {
    //         txn.clear();
    //         txn.input(program.state.interner.number_id(ix as f32), program.state.interner.string_id("tag"), program.state.interner.string_id("person"), 1);
    //         txn.input(program.state.interner.number_id(ix as f32), program.state.interner.string_id("name"), program.state.interner.number_id(ix as f32), 1);
    //         txn.exec(&mut program);
    //         ix += 1;
    //     });
        // println!("Size: {:?}", program.index.size);
}
