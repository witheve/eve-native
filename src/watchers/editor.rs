extern crate ws;
use self::ws::{Sender as WSSender, Message};

extern crate serde_json;

use std::ops::Deref;
use std::path::{PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Sender};
use super::super::indexes::{WatchDiff};
use super::super::ops::{Internable, Interner, RawChange, RunLoop, RunLoopMessage, ProgramRunner};

use super::Watcher;
use super::system::{SystemTimerWatcher, PanicWatcher};
use super::compiler::{CompilerWatcher};
use super::textcompiler::{RawTextCompilerWatcher};
use super::console::{ConsoleWatcher};
use super::remote::{Router, RemoteWatcher};
use super::websocket::{WebsocketClientWatcher};

fn to_s(string:&str) -> Internable {
    return Internable::String(string.to_owned());
}

//-------------------------------------------------------------------------
// Editor Watcher
//-------------------------------------------------------------------------

pub struct EditorWatcher {
    name: String,
    running: RunLoop,
    ws_out: WSSender,
    client_name: String,
    client_out: Sender<RunLoopMessage>,
    editor_name: String,
    editor_out: Sender<RunLoopMessage>,
}

impl EditorWatcher {
    pub fn new(client_runner:&mut ProgramRunner, router:Arc<Mutex<Router>>, ws_out:WSSender, libraries_path:Option<&str>, programs_path:Option<&str>) -> EditorWatcher {
        let client_name = client_runner.program.name.to_owned();
        let client_out = client_runner.program.outgoing.clone();

        let editor_name = format!("{}-editor", &client_name);
        let mut editor_runner = ProgramRunner::new(&editor_name);
        let editor_out = editor_runner.program.outgoing.clone();
        router.lock().unwrap().register(&editor_name, editor_out.clone());

        // @NOTE: Compiler watcher dumps into client!
        editor_runner.program.attach(Box::new(CompilerWatcher::new(client_out.clone(), true)));

        editor_runner.program.attach(Box::new(SystemTimerWatcher::new(editor_out.clone())));
        editor_runner.program.attach(Box::new(RawTextCompilerWatcher::new(editor_out.clone())));
        editor_runner.program.attach(Box::new(WebsocketClientWatcher::new(ws_out.clone(), &editor_name)));
        editor_runner.program.attach(Box::new(ConsoleWatcher::new()));
        editor_runner.program.attach(Box::new(PanicWatcher::new()));
        editor_runner.program.attach(Box::new(RemoteWatcher::new(&editor_name, &router.lock().unwrap().deref())));

        let text = serde_json::to_string(&json!({"type": "load-bundle", "bundle": "programs/editor", "client": &editor_name})).unwrap();
        ws_out.send(Message::Text(text)).unwrap();


        if let Some(path) = libraries_path {
            editor_runner.load(path);
        }

        if let Some(path) = PathBuf::from(programs_path.unwrap()).join("editor").join("server").to_str() {
            editor_runner.load(path);
        }

        if let Some(path) = PathBuf::from(programs_path.unwrap()).join("editor").join("client").to_str() {
            client_runner.load(path);
        }

        let running = editor_runner.run();
        let editor_record = to_s(&format!("editor/program|{}", editor_name));
        let transaction = RunLoopMessage::Transaction(vec![
            RawChange{e: editor_record.clone(), a: to_s("tag"), v: to_s("editor/program"), n: to_s("editor/init"), count: 1},
            RawChange{e: editor_record, a: to_s("name"), v: to_s(&editor_name), n: to_s("editor/init"), count: 1},
        ]);
        match client_out.send(transaction) {
            Err(_) => panic!("Something has gone horribly awry in editor initialization."),
            _ => {}
        }
        EditorWatcher{name: "editor".to_string(),
                      running, ws_out,
                      client_name, client_out,
                      editor_name, editor_out}
    }
}

impl Watcher for EditorWatcher {
    fn get_name(& self) -> String {
        self.name.clone()
    }
    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
    fn on_diff(&mut self, _:&mut Interner, _:WatchDiff) {
    }
}

impl Drop for EditorWatcher {
    fn drop(&mut self) {
        self.running.close();
        let text = serde_json::to_string(&json!({"type": "unload-bundle", "bundle": "programs/editor", "client": &self.editor_name})).unwrap();
        self.ws_out.send(Message::Text(text)).unwrap();
    }
}
