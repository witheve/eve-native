extern crate tokio_timer;
extern crate tokio_core;
extern crate futures;
extern crate time;

use super::super::indexes::{WatchDiff};
use super::super::ops::{Internable, Interner};
use super::super::watcher::Watcher;

extern crate term_painter;
use self::term_painter::ToStyle;
use self::term_painter::Color::*;

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
                ("warn", text) => println!("{} {}", BrightYellow.paint("Warn:"), text),
                ("error", text) => println!("{} {}", BrightRed.paint("Error:"), text),
                _ => {},
            }
        }
    }
}
