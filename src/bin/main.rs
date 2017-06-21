#![feature(link_args)]
#![feature(dropck_eyepatch)]
#![feature(generic_param_attrs)]
#![feature(sip_hash_13)]
#![feature(core_intrinsics)]
#![feature(shared)]
#![feature(unique)]
#![feature(placement_new_protocol)]
#![feature(fused)]
#![feature(alloc)]
#![feature(heap_api)]
#![feature(oom)]
#![feature(slice_patterns)]

// #[link_args = "-s TOTAL_MEMORY=500000000 EXPORTED_FUNCTIONS=['_coolrand','_makeIter','_next']"]
// #[link_args = "-s TOTAL_MEMORY=503316480"]
extern {}

#[macro_use]
extern crate nom;

#[macro_use]
extern crate lazy_static;

extern crate eve;
extern crate tokio_timer;
extern crate futures;
extern crate time;

use eve::ops::{Program, Transaction};
use eve::parser::{parse_file};
use eve::watcher::{SystemTimerWatcher};
use std::env;

fn main() {
    let mut program = Program::new();
    let mut file = "examples/test.eve".to_string();
    if let Some(arg1) = env::args().nth(1) {
        file = arg1;
    }
    let blocks = parse_file(&mut program, &file);
    let outgoing = program.outgoing.clone();
    program.attach("system/timer", Box::new(SystemTimerWatcher::new(outgoing)));
    for block in blocks {
        program.raw_block(block);
    }
    loop {
        let mut v = program.incoming.recv().unwrap();
        // println!("GOT {:?}", v);
        let mut start_ns = time::precise_time_ns();
        let mut txn = Transaction::new();
        for cur in v.drain(..) {
            txn.input_change(cur.to_change(&mut program.state.interner));
        };
        txn.exec(&mut program);
        let mut end_ns = time::precise_time_ns();
        println!("Txn took {:?}", (end_ns - start_ns) as f64 / 1_000_000.0);

    }
}

