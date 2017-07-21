#![feature(link_args)]

// #[link_args = "-s TOTAL_MEMORY=500000000 EXPORTED_FUNCTIONS=['_coolrand','_makeIter','_next']"]
// #[link_args = "-s TOTAL_MEMORY=503316480"]
extern {}

extern crate eve;
extern crate tokio_timer;
extern crate futures;
extern crate time;

use eve::ops::{ProgramRunner};
use eve::watcher::{SystemTimerWatcher, PrintWatcher};
use std::env;

fn main() {
    let mut runner = ProgramRunner::new();
    let outgoing = runner.program.outgoing.clone();
    runner.program.attach("system/timer", Box::new(SystemTimerWatcher::new(outgoing)));
    runner.program.attach("system/print", Box::new(PrintWatcher{}));

    for file in env::args().skip(1) {
        runner.load(&file);
    }

    let running = runner.run();
    running.wait();
}
