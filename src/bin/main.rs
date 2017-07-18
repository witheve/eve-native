#![feature(link_args)]

// #[link_args = "-s TOTAL_MEMORY=500000000 EXPORTED_FUNCTIONS=['_coolrand','_makeIter','_next']"]
// #[link_args = "-s TOTAL_MEMORY=503316480"]
extern {}

extern crate eve;
extern crate tokio_timer;
extern crate futures;
extern crate time;

use eve::ops::{Program, Transaction};
use eve::parser::{parse_file};
use eve::watcher::{SystemTimerWatcher, PrintWatcher};
use std::env;

fn main() {
    let mut program = Program::new();
    let outgoing = program.outgoing.clone();
    program.attach("system/timer", Box::new(SystemTimerWatcher::new(outgoing)));
    program.attach("system/print", Box::new(PrintWatcher{}));

    for file in env::args().skip(1) {
        let blocks = parse_file(&mut program, &file);
        for block in blocks {
            program.raw_block(block);
        }
    }
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
