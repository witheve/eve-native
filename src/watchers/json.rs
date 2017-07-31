use super::super::indexes::{WatchDiff};
use super::super::ops::{Internable, Interner};
use super::Watcher;

extern crate term_painter;
use self::term_painter::ToStyle;
use self::term_painter::Color::*;

pub struct JsonWatcher {
    name: String,
    outgoing: Sender<RunLoopMessage>,
}

impl JsonWatcher {
    pub fn new(outgoing: Sender<RunLoopMessage>) -> JsonWatcher {
        JsonWatcher { name: "json", outgoing }
    }
}

impl Watcher for JsonWatcher {
    fn get_name(& self) -> String {
        self.name.clone()
    }
    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        for add in diff.adds {
            let kind = Internable::to_string(interner.get_value(add[0]));
            match (&kind[..], text) {
                ("parse", text) => {
                  println!("Parsing JSON into EAVs")
                },
                ("enocde", text) => {

                }
                println!("{} {}", BrightYellow.paint("Warn:"), text),
                ("error", text) => println!("{} {}", BrightRed.paint("Error:"), text),
                _ => {},
            }
        }
    }
}