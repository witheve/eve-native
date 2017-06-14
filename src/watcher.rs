extern crate tokio_timer;
extern crate tokio_core;
extern crate futures;

use self::tokio_core::reactor::Core;
use tokio_timer::*;
use futures::*;
use std::time::*;
use indexes::{WatchDiff, MyHasher};
use hash::map::{HashMap};
use ops::{Internable, Interner};
use futures::sync::mpsc;
use std::thread::{self, JoinHandle};

pub trait Watcher {
    fn on_diff(&self, interner:&Interner, diff:WatchDiff);
}

pub struct SystemTimerWatcher {
    listeners: HashMap<Internable, Vec<Internable>, MyHasher>,
    thread: JoinHandle<()>,
}

pub struct PrintWatcher {

}

impl Watcher for PrintWatcher {
    fn on_diff(&self, interner:&Interner, diff:WatchDiff) {
        for add in diff.adds {
            println!("Printer: {:?}", add.iter().map(|v| interner.get_value(*v).print()).collect::<Vec<String>>());
        }
    }
}

pub fn run_timer() {
    // Create a new timer with default settings. While this is the easiest way
    // to get a timer, usually you will want to tune the config settings for
    // your usage patterns.
    let timer = Timer::default();

    // Set a timeout that expires in 500 milliseconds
    let interval = timer.interval_at(Instant::now(),Duration::from_millis(500));
    println!("int {:?}", interval);
    let foo = interval.for_each(|x| {
        println!("It's time! {:?}", x);
        future::ok::<(), TimerError>(())
    }).map_err(|x| {
        panic!("uh oh");
    });
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    handle.spawn(foo);
    println!("Should be running");
    core.turn(None);
    // core.run(future::ok::<(), TimerError>(()));
}


#[test]
pub fn timer_test() {
    run_timer();
}
