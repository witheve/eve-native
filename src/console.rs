extern crate tokio_timer;
extern crate tokio_core;
extern crate futures;
extern crate time;

use indexes::{WatchDiff};
use ops::{Internable, Interner};
use watcher::Watcher;

//-------------------------------------------------------------------------
// Console Watcher
//-------------------------------------------------------------------------

pub struct ConsoleWatcher {}

impl Watcher for ConsoleWatcher {
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        for add in diff.adds {
            let kind = Internable::to_string(interner.get_value(add[0]));
            let text = Internable::to_string(interner.get_value(add[1]));
            match (&kind[..], text) {
                ("log", text) => println!("{}", text),
                ("warn", text) => println!("{}", text),
                ("error", text) => eprintln!("{}", text),
                _ => {},
            }
        }
    }
}