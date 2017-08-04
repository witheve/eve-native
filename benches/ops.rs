#![feature(test)]

extern crate test;
extern crate eve;

use eve::ops::*;
use eve::instructions::*;
use eve::compiler::{parse_string};
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

fn test_pipe(b: &mut Bencher, code: &str, setup:&str) {
    let mut program = Program::new();

    let to_test = parse_string(&mut program, code, "test").pop().unwrap();
    let pipe = to_test.pipes[0].clone();
    program.block_info.blocks.push(to_test);

    let mut blocks = vec![];
    blocks.extend(parse_string(&mut program, setup, "test"));

    let mut txn = CodeTransaction::new();
    txn.exec(&mut program, blocks, vec![]);

    let mut pool = EstimateIterPool::new();
    let mut frame = Frame::new();
    frame.input = Some(Change {e:0, a:0, v:0, n:0, round:0, count:1, transaction:0});
    frame.block_ix = 0;

    b.iter(|| {
        interpret(&mut program.state, &program.block_info, &mut pool, &mut frame, &pipe);
    });
}

fn test_solver(b: &mut Bencher, code: &str, setup:&str) {
    let mut program = Program::new();

    let to_test = parse_string(&mut program, code, "test").pop().unwrap();
    let solver = Solver::new(0, None, &to_test.constraints);
    program.block_info.blocks.push(to_test);

    let mut blocks = vec![];
    blocks.extend(parse_string(&mut program, setup, "test"));

    let mut txn = CodeTransaction::new();
    txn.exec(&mut program, blocks, vec![]);

    let mut pool = EstimateIterPool::new();
    let mut frame = Frame::new();
    frame.input = Some(Change {e:0, a:0, v:0, n:0, round:0, count:1, transaction:0});
    frame.block_ix = 0;

    b.iter(|| {
        solver.run(&mut program.state, &mut pool, &mut frame)
    });
}

#[bench]
pub fn ops_bind_pipe(b:&mut Bencher) {
    test_pipe(b, r#"
        search
            f = [#foo bar baz]
            [#meep foo:f woah]
        bind
            f.zomg += 3
        end
    "#, r#"
        commit
            foo = [#foo bar: 3 baz: 4]
            [#foo bar: 3 baz: 0]
            [#foo bar: 4 baz: 0]
            [#foo bar: 9 baz: 0]
            [#foo bar: 0 baz: 0]
            [#meep foo woah:"yeah"]
            [#meep foo woah:"leep"]
            [#meep foo woah:"seep"]
        end
    "#);
}

#[bench]
pub fn ops_bind_solver(b:&mut Bencher) {
    test_solver(b, r#"
        search
            f = [#foo bar baz]
            [#meep foo:f woah]
        bind
            f.zomg += 3
        end
    "#, r#"
        commit
            foo = [#foo bar: 3 baz: 4]
            [#foo bar: 3 baz: 0]
            [#foo bar: 4 baz: 0]
            [#foo bar: 9 baz: 0]
            [#foo bar: 0 baz: 0]
            [#meep foo woah:"yeah"]
            [#meep foo woah:"leep"]
            [#meep foo woah:"seep"]
        end
    "#);
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
