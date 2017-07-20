#![feature(link_args)]

// #[link_args = "-s TOTAL_MEMORY=500000000 EXPORTED_FUNCTIONS=['_coolrand','_makeIter','_next']"]
// #[link_args = "-s TOTAL_MEMORY=503316480"]
extern {}

extern crate eve;
extern crate tokio_timer;
extern crate futures;
extern crate time;

use eve::ops::{Program, Transaction, CodeTransaction};
use eve::compiler::{parse_file};
use eve::watcher::{SystemTimerWatcher, PrintWatcher};
use std::env;

fn main() {
    let mut program = Program::new();
    let outgoing = program.outgoing.clone();
    program.attach("system/timer", Box::new(SystemTimerWatcher::new(outgoing)));
    program.attach("system/print", Box::new(PrintWatcher{}));

    let mut blocks = vec![];
    for file in env::args().skip(1) {
        blocks.extend(parse_file(&mut program, &file));
    }

    let mut txn = CodeTransaction::new();
    txn.exec(&mut program, blocks, vec![]);

    println!("Starting run loop.");
    loop {
        let v = program.incoming.recv().unwrap();
        let start_ns = time::precise_time_ns();
        let mut txn = Transaction::new();
        for cur in v {
            txn.input_change(cur.to_change(&mut program.state.interner));
        };
        txn.exec(&mut program);
        let end_ns = time::precise_time_ns();
        println!("Txn took {:?}", (end_ns - start_ns) as f64 / 1_000_000.0);
    }
}
