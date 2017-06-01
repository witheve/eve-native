extern crate tokio_timer;
extern crate tokio_core;
extern crate futures;

use self::tokio_core::reactor::Core;
use tokio_timer::*;
use futures::*;
use std::time::*;

pub fn run_timer() {
    // Create a new timer with default settings. While this is the easiest way
    // to get a timer, usually you will want to tune the config settings for
    // your usage patterns.
    let timer = Timer::default();

    // Set a timeout that expires in 500 milliseconds
    let interval = timer.interval(Duration::from_millis(500));
    println!("int {:?}", interval);
    let foo = interval.for_each(|x| {
        println!("It's time! {:?}", x);
        future::ok(())
    });
    let mut core = Core::new().unwrap();
    core.run(foo);
}


#[test]
pub fn timer_test() {
    run_timer();
}
