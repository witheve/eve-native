use indexes::{WatchDiff};
use ops::{Interner};

pub trait Watcher {
    fn get_name(& self) -> String;
    fn set_name(&mut self, &str);
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff);
}

pub mod file;
pub mod console;
pub mod system;
pub mod compiler;
pub mod json;
pub mod http;
pub mod textcompiler;
pub mod editor;
pub mod json;
pub mod remote;
pub mod websocket;
