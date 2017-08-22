#![feature(test)]

extern crate eve;
extern crate test;

use eve::ops::{Program, EstimateIterPool, Internable, CodeTransaction, Transaction, RawChange};
use eve::compiler::{parse_file};
use test::Bencher;


fn make_program(paths:Vec<&str>) -> Program {
    let mut program = Program::new();
    let mut iter_pool = EstimateIterPool::new();

    let mut blocks = vec![];
    for path in paths {
        blocks.extend(parse_file(&mut program.state.interner, &path, false, false));
    }

    let mut txn = CodeTransaction::new();
    txn.exec(&mut program, blocks, vec![]);

    program
}

fn file_bench(b: &mut Bencher, path: &str) {
    let mut program = make_program(vec!["libraries/", "benches/lib.eve", path]);
    let mut iter_pool = EstimateIterPool::new();
    let mut persistence_channel = None;
    let mut ix = 1;
    b.iter(|| {
        let v = vec![
            RawChange { e: Internable::String("tick".to_string()), a: Internable::String("tag".to_string()), v: Internable::String("time".to_string()), n: Internable::Null, count: 1 },
            RawChange { e: Internable::String("tick".to_string()), a: Internable::String("tick".to_string()), v: Internable::from_number(ix as f32), n: Internable::Null, count: 1 }
        ];
        let mut txn = Transaction::new(&mut iter_pool);
        for cur in v {
            txn.input_change(cur.to_change(&mut program.state.interner));
        };
        txn.exec(&mut program, &mut persistence_channel);
        ix += 1;
    });
}

#[bench]
pub fn base_balls(b: &mut Bencher) { file_bench(b, "benches/balls.eve"); }

#[bench]
pub fn base_infini_flappy(b: &mut Bencher) { file_bench(b, "benches/infini-flappy.eve"); }
