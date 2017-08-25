extern crate ws;
use self::ws::{Sender as WSSender, Message};

extern crate serde_json;

use rand::{self, Rng};
use std::ops::Deref;
use std::path::{PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};
use super::super::indexes::{WatchDiff};
use super::super::ops::{Internable, Interner, RawChange, RunLoop, RunLoopMessage, MetaMessage, ProgramRunner};

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
    meta_thread: thread::JoinHandle<()>,
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

        let (outgoing_meta, incoming_meta) = mpsc::channel();
        client_runner.meta_channel = Some(outgoing_meta);
        let meta_thread = EditorWatcher::make_meta_thread(&format!("{}-meta-receiver", editor_name), incoming_meta, editor_out.clone());

        EditorWatcher{name: "editor".to_string(),
                      running, meta_thread, ws_out,
                      client_name, client_out,
                      editor_name, editor_out}
    }

    pub fn make_meta_thread(name:&str, incoming: mpsc::Receiver<MetaMessage>, outgoing: Sender<RunLoopMessage>) -> thread::JoinHandle<()> {
        thread::Builder::new().name(name.to_owned()).spawn(move || {
            loop {
                match incoming.recv() {
                    Ok(MetaMessage::Transaction{inputs, outputs}) => {
                        // println!("META MESSAGE:\n  inputs: [");
                        // for input in inputs.iter() { println!("    {:?}", input); }
                        // println!("  \n  outputs: [");
                        // for output in outputs.iter() { println!("    {:?}", output); }
                        // println!("  ]\n");

                        let event = format!("|{}|editor/event/meta-transaction", rand::thread_rng().next_u64());
                        let event_id = Internable::String(event.to_owned());
                        let mut changes = vec![
                            make_change_str(event_id.clone(), "tag", "editor/event"),
                            make_change_str(event_id.clone(), "tag", "editor/event/meta-transaction"),
                        ];
                        for input in inputs.iter() {
                            let kind = if input.count > 0 { "add" } else { "remove" };

                            // @FIXME: don't use debug print here.
                            let input_id = Internable::String(format!("{}|input|{:?}", event, input.e));
                            let av_id = Internable::String(format!("{}|input|{:?}|av|{:?}|{:?}", event, input.e, input.a, input.v));
                            changes.push(make_change(event_id.clone(), "input", input_id.clone()));
                            changes.push(make_change(input_id.clone(), "entity", input.e.clone()));
                            changes.push(make_change(input_id.clone(), "av", av_id.clone()));
                            changes.push(make_change(av_id.clone(), "attribute", input.a.clone()));
                            changes.push(make_change(av_id.clone(), "value", input.v.clone()));
                            changes.push(make_change_str(av_id.clone(), "type", kind));
                        }

                        for output in outputs.iter() {
                            let kind = if output.count > 0 { "add" } else { "remove" };

                            // @FIXME: don't use debug print here.
                            let output_id = Internable::String(format!("{}|output|{:?}", event, output.e));
                            let av_id = Internable::String(format!("{}|output|{:?}|av|{:?}|{:?}", event, output.e, output.a, output.v));
                            changes.push(make_change(event_id.clone(), "output", output_id.clone()));
                            changes.push(make_change(output_id.clone(), "entity", output.e.clone()));
                            changes.push(make_change(output_id.clone(), "av", av_id.clone()));
                            changes.push(make_change(av_id.clone(), "attribute", output.a.clone()));
                            changes.push(make_change(av_id.clone(), "value", output.v.clone()));
                            changes.push(make_change_str(av_id.clone(), "type", kind));
                        }

                        outgoing.send(RunLoopMessage::Transaction(changes));

                    },
                    Ok(msg) => panic!("Unknown meta message: {:?}", msg),
                    Err(_) => {
                        println!("Closing meta channel.");
                        break;
                    }
                }
            }
        }).unwrap()
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


fn make_change(e: Internable, a: &str, v: Internable) -> RawChange {
    RawChange{e, a: Internable::String(a.to_owned()), v, n: Internable::String("editor".to_owned()), count: 1}
}

fn make_change_str(e: Internable, a: &str, v: &str) -> RawChange {
    RawChange{e, a: Internable::String(a.to_owned()), v: Internable::String(v.to_owned()), n: Internable::String("editor".to_owned()), count: 1}
}

fn make_change_num(e: Internable, a: &str, v: f32) -> RawChange {
    RawChange{e, a: Internable::String(a.to_owned()), v: Internable::from_number(v), n: Internable::String("editor".to_owned()), count: 1}
}
