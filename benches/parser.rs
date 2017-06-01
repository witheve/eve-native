#![feature(test)]

extern crate test;
extern crate eve;

use eve::parser::*;
use eve::ops::{Program, Transaction, CodeTransaction};
use test::Bencher;

#[bench]
pub fn parse_clock(b:&mut Bencher) {
    b.iter(|| {
        let mut program = Program::new();
        let blocks = parse_file(&mut program, "/users/ibdknox/scratch/eve-starter/programs/test.eve");
    });
}

// #[bench]
// pub fn parse_run_clock(b:&mut Bencher) {
//     let mut program = Program::new();
//     let blocks = parse_file(&mut program, "/users/ibdknox/scratch/eve-starter/programs/test.eve");
//     println!("blocks {:?}", blocks.len());
//     let mut names = vec![];
//     for block in blocks {
//         names.push(block.name.clone());
//         program.raw_block(block);
//     }
//     let mut txn = Transaction::new();
//     let mut ix = 0;
//     txn.input(program.interner.string_id("time|system/timer|"), program.interner.string_id("minutes"), program.interner.number_id(10.0), 1);
//     txn.input(program.interner.string_id("time|system/timer|"), program.interner.string_id("hours"), program.interner.number_id(10.0), 1);
//     b.iter(|| {
//         // program.clear();
//         txn.input(program.interner.string_id("time|system/timer|"), program.interner.string_id("seconds"), program.interner.number_id(ix as f32), 1);
//         txn.exec(&mut program);
//         txn.clear();
//         ix += 1;
//         // let mut txn = CodeTransaction::new();
//         // for name in names.iter() {
//         //     txn.exec(&mut program, name, true);
//         // }
//     });
// }
