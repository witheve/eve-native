use super::super::indexes::{WatchDiff};
use super::super::ops::{Internable, Interner};
use super::Watcher;

extern crate term_painter;
use self::term_painter::ToStyle;
use self::term_painter::Color::*;

//-------------------------------------------------------------------------
// Console Watcher
//-------------------------------------------------------------------------

pub struct ConsoleWatcher {
    name: String,
}

impl ConsoleWatcher {
    pub fn new() -> ConsoleWatcher {
        ConsoleWatcher{name: "console".to_string()}
    }
}


impl Watcher for ConsoleWatcher {
    fn get_name(& self) -> String {
        self.name.clone()
    }
    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
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

//-------------------------------------------------------------------------
// Print Diff Watcher
//-------------------------------------------------------------------------

pub struct PrintDiffWatcher {
    name: String,
}

impl PrintDiffWatcher {
    pub fn new() -> PrintDiffWatcher {
        PrintDiffWatcher{name: "console/diff".to_string()}
    }
}

impl Watcher for PrintDiffWatcher {
    fn get_name(& self) -> String {
        self.name.clone()
    }
    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        for remove in diff.removes {
            println!("- {:?}", remove.iter().map(|v| interner.get_value(*v).print()).collect::<Vec<String>>());
        }
        for add in diff.adds {
            println!("+ {:?}", add.iter().map(|v| interner.get_value(*v).print()).collect::<Vec<String>>());
        }
    }
}
