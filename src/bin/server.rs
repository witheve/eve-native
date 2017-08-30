extern "C" {}

use std::path::{Path, PathBuf};

extern crate clap;
use clap::{App, Arg};

extern crate ws;
use ws::{CloseCode, Handler, Message, Sender as WSSender, listen};

#[macro_use]
extern crate serde_derive;

extern crate serde_json;
extern crate serde;
use serde_json::Error;

use std::sync::mpsc::{self, Sender};

extern crate notify;
use notify::{DebouncedEvent, RecommendedWatcher, RecursiveMode,
             Watcher};
use std::time::Duration;

extern crate time;

extern crate eve;
use eve::ops::{Internable, JSONInternable, Persister, ProgramRunner,
               RawChange, RunLoop, RunLoopMessage};
use eve::paths::EvePaths;
use eve::watchers::compiler::CompilerWatcher;
use eve::watchers::console::ConsoleWatcher;
use eve::watchers::editor::EditorWatcher;
use eve::watchers::file::FileWatcher;
use eve::watchers::remote::{RemoteWatcher, Router, RouterMessage};
use eve::watchers::system::{PanicWatcher, SystemTimerWatcher};
use eve::watchers::textcompiler::RawTextCompilerWatcher;
use eve::watchers::websocket::WebsocketClientWatcher;

extern crate iron;
extern crate staticfile;
extern crate mount;

use iron::{AfterMiddleware, Chain, Iron, IronError, IronResult,
           Request, Response, status};
use mount::Mount;
use staticfile::Static;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::thread;

extern crate term_painter;
use self::term_painter::Color::*;
use self::term_painter::ToStyle;

//-------------------------------------------------------------------------
// Websocket client handler
//-------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientMessage {
    Block { id: String, code: String },
    RemoveBlock { id: String },
    Transaction {
        client: String,
        adds: Vec<(JSONInternable, JSONInternable, JSONInternable)>,
        removes:
            Vec<(JSONInternable, JSONInternable, JSONInternable)>,
    },
}

pub struct ClientHandler {
    out: WSSender,
    running: RunLoop,
    client_name: String,
    router: Arc<Mutex<Router>>,
    router_channel: Sender<RouterMessage>,
}

impl ClientHandler {
    pub fn new(client_name: &str,
               out: WSSender,
               router: Arc<Mutex<Router>>,
               eve_paths: &EvePaths,
               eve_flags: &EveFlags)
        -> ClientHandler {
        let router_channel = router.lock().expect("ERROR: Failed to lock router: Cannot clone channel.").deref().get_channel();
        let mut runner = ProgramRunner::new(client_name);
        let outgoing = runner.program.outgoing.clone();
        router.lock().expect("ERROR: Failed to lock router: Cannot register new client.").register(&client_name, outgoing.clone());
        if !eve_flags.clean {
            runner.program.attach(Box::new(SystemTimerWatcher::new(outgoing.clone())));
            runner.program.attach(Box::new(CompilerWatcher::new(outgoing.clone(), false)));
            runner.program.attach(Box::new(RawTextCompilerWatcher::new(outgoing.clone())));
            runner.program.attach(Box::new(FileWatcher::new(outgoing.clone())));
            runner.program.attach(Box::new(WebsocketClientWatcher::new(out.clone(), client_name)));
            runner.program
                  .attach(Box::new(ConsoleWatcher::new()));
            runner.program
                  .attach(Box::new(PanicWatcher::new()));
            runner.program.attach(Box::new(RemoteWatcher::new(client_name, &router.lock().expect("ERROR: Failed to lock router: Cannot init RemoteWatcher.").deref())));
            if eve_flags.editor {
                let editor_watcher =
                    EditorWatcher::new(&mut runner,
                                       router.clone(),
                                       out.clone(),
                                       eve_paths.libraries(),
                                       eve_paths.programs());
                runner.program
                      .attach(Box::new(editor_watcher));
            }
        }

        if let Some(path) = eve_paths.libraries() {
            runner.load(path);
        }
        for file in eve_paths.files.iter() {
            runner.load(file);
        }

        let running = runner.run();

        if eve_flags.watch {
            println!("Starting file watcher!");
            ClientHandler::make_file_notifier(eve_paths, &running);
        }

        ClientHandler {
            out,
            running,
            client_name: client_name.to_owned(),
            router,
            router_channel,
        }
    }

    fn make_file_notifier(eve_paths: &EvePaths, run_loop: &RunLoop) {
        println!("WARN: @TODO: Make this die when the client DC's!");
        let client_channel = run_loop.channel();
        let files: Vec<String> =
            eve_paths.files
                     .iter()
                     .map(|f| f.to_string())
                     .collect();
        let libraries =
            eve_paths.libraries()
                     .map(|s| s.to_owned());

        thread::Builder::new().name("client file watcher".to_owned()).spawn(move || {
            let (outgoing, incoming) = mpsc::channel();
            let mut watcher:RecommendedWatcher = Watcher::new(outgoing, Duration::from_secs(1)).unwrap();

            if let Some(path) = libraries {
                watcher.watch(path, RecursiveMode::Recursive).unwrap();
            }
            for file in files.iter() {
                watcher.watch(file, RecursiveMode::Recursive).unwrap();
            }

            loop {
                let mut dirty:HashSet<PathBuf> = HashSet::new();
                match incoming.recv() {
                    Ok(event) => {
                        match event {
                            DebouncedEvent::Error(err, ..) => {
                                println!("Closing client file watcher due to unforeseen error: {:?}", err);
                                break;
                            },
                            DebouncedEvent::NoticeRemove(path) |
                            DebouncedEvent::NoticeWrite(path) => {
                                let should_reload = match path.extension() {
                                    Some(ext) => ext == "eve" || ext == "eve.md",
                                    _ => false
                                };
                                if should_reload {
                                    dirty.insert(path);
                                }
                            },
                            DebouncedEvent::Create(path) |
                            DebouncedEvent::Chmod(path) |
                            DebouncedEvent::Remove(path) |
                            DebouncedEvent::Write(path) => {
                                let should_reload = match path.extension() {
                                    Some(ext) => ext == "eve" || ext == "eve.md",
                                    _ => false
                                };
                                if should_reload {
                                    dirty.insert(path);
                                    if let Err(_) = client_channel.send(RunLoopMessage::Reload(dirty.clone())) {
                                        println!("Closing client file watcher.");
                                        break;
                                    }
                                    dirty.clear();
                                }
                            },
                            DebouncedEvent::Rename(..) | // (old, new) (gotta pass in both)
                            DebouncedEvent::Rescan => {
                                unimplemented!();
                            }
                        };
                    },
                    Err(err) => println!("ERROR: {:?}", err)
                }
            }
        });
    }
}

impl Handler for ClientHandler {
    //fn on_request(&mut self, req: &ws::Request) -> Result<ws::Response,ws::Error> {
    //println!("Handler received request:\n{:?}");
    //ws::Response::from_request(req)
    //}

    fn on_message(&mut self, msg: Message) -> Result<(), ws::Error> {
        // println!("Server got message '{}'. ", msg);
        if let Message::Text(s) = msg {
            let deserialized: Result<ClientMessage,
                                     Error> =
                serde_json::from_str(&s);
            // println!("deserialized = {:?}", deserialized);
            match deserialized {
                Ok(ClientMessage::Transaction {
                       client,
                       adds,
                       removes,
                   }) => {
                    let mut raw_changes = vec![];
                    raw_changes.extend(adds.into_iter().map(|(e,a,v)| {
                        RawChange { e:e.into(), a:a.into(), v:v.into(), n:Internable::String("input".to_string()),count:1 }
                    }));
                    raw_changes.extend(removes.into_iter().map(|(e,a,v)| {
                        RawChange { e:e.into(), a:a.into(), v:v.into(), n:Internable::String("input".to_string()),count:-1 }
                    }));

                    self.router_channel.send(RouterMessage::Local(client, raw_changes)).expect("ERROR: Failed to send message to client");
                }
                _ => {}
            }
            Ok(())
        } else {
            Ok(())
        }
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
        println!("WebSocket closing for ({:?}) {}", code, reason);
        self.router
            .lock()
            .unwrap()
            .unregister(&self.client_name);
        self.running.close();
    }
}

//-------------------------------------------------------------------------
// Static File Server
//-------------------------------------------------------------------------

struct Custom404;

impl AfterMiddleware for Custom404 {
    fn catch(&self,
             _: &mut Request,
             _: IronError)
        -> IronResult<Response> {
        Ok(Response::with((status::NotFound, "File not found...")))
    }
}

fn http_server(address: String) -> std::thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut mount = Mount::new();
        mount.mount("/", Static::new(Path::new("assets/index.html")));
        mount.mount("/assets/", Static::new(Path::new("assets/")));
        mount.mount("/dist/", Static::new(Path::new("dist/")));

        let mut chain = Chain::new(mount);
        chain.link_after(Custom404);

        println!("{} HTTP Server at {}... ",
                 BrightGreen.paint("Starting:"),
                 address);
        match Iron::new(chain).http(&address) {
            Ok(_) => {}
            Err(why) => {
                println!("{} Failed to start HTTP Server: {}",
                         BrightRed.paint("Error:"),
                         why)
            }
        };

    })
}

fn websocket_server(address: String,
                    eve_paths: &EvePaths,
                    eve_flags: &EveFlags) {
    println!("{} Websocket Server at {}... ",
             BrightGreen.paint("Starting:"),
             address);

    // create a server program
    let mut runner = ProgramRunner::new("server");
    let outgoing = runner.program.outgoing.clone();
    let router = Arc::new(Mutex::new(Router::new(outgoing.clone())));
    router.lock()
          .unwrap()
          .register("server", outgoing.clone());

    if !eve_flags.clean {
        runner.program.attach(Box::new(SystemTimerWatcher::new(outgoing.clone())));
        runner.program.attach(Box::new(CompilerWatcher::new(outgoing.clone(), false)));
        runner.program.attach(Box::new(RawTextCompilerWatcher::new(outgoing)));
        runner.program
              .attach(Box::new(ConsoleWatcher::new()));
        runner.program
              .attach(Box::new(PanicWatcher::new()));
        runner.program
              .attach(Box::new(RemoteWatcher::new("server",
                                                  &router.lock()
                                                         .unwrap()
                                                         .deref())));
    }

    if let &Some(persist_file) = &eve_paths.persist() {
        let mut persister = Persister::new(persist_file);
        persister.load(persist_file);
        runner.persist(&mut persister);
    }

    for file in eve_paths.server_files.iter() {
        runner.load(file);
    }

    runner.run();
    let mut ix = 0;

    match listen(address, |out| {
        ix += 1;
        let client_name = format!("ws_client_{}", ix);
        ClientHandler::new(&client_name,
                           out,
                           router.clone(),
                           eve_paths,
                           eve_flags)
    }) {
        Ok(_) => {}
        Err(why) => {
            println!("{} Failed to start Websocket Server: {}",
                     BrightRed.paint("Error:"),
                     why)
        }
    };
}

//-------------------------------------------------------------------------
// Main
//-------------------------------------------------------------------------

pub struct EveFlags {
    editor: bool,
    watch: bool,
    clean: bool,
}

fn main() {
    let matches = App::new("Eve")
                          .version("0.4")
                          .author("Kodowa Inc.")
                          .about("Creates an instance of the Eve server. Default values for options are in parentheses.")
                          .arg(Arg::with_name("editor")
                               .short("E")
                               .long("editor")
                               .help("Attaches an editor instance to each client program."))
                          .arg(Arg::with_name("watch")
                               .short("w")
                               .long("watch")
                               .help("Watches eve files for changes, and injects them into your running program."))
                          .arg(Arg::with_name("persist")
                               .short("s")
                               .long("persist")
                               .value_name("FILE")
                               .help("Sets the name for the database to load from and write to")
                               .takes_value(true))
                          .arg(Arg::with_name("library-path")
                               .short("L")
                               .long("library-path")
                               .value_name("PATH")
                               .help("Override default library path")
                               .takes_value(true))
                          .arg(Arg::with_name("EVE_FILES")
                               .help("The eve files and folders to load")
                               .required(true)
                               .multiple(true))
                          .arg(Arg::with_name("server-file")
                               .long("server")
                               .value_name("FILE")
                               .help("Loads the specified file into the server instance")
                               .takes_value(true))
                          .arg(Arg::with_name("port")
                               .short("p")
                               .long("port")
                               .value_name("PORT")
                               .help("Sets the port for the Eve server (3012)")
                               .takes_value(true))
                          .arg(Arg::with_name("http-port")
                               .short("t")
                               .long("http-port")
                               .value_name("PORT")
                               .help("Sets the port for the HTTP server (8081)")
                               .takes_value(true))
                          .arg(Arg::with_name("address")
                               .short("a")
                               .long("address")
                               .value_name("ADDRESS")
                               .help("Sets the address of the server (127.0.0.1)")
                               .takes_value(true))
                          .arg(Arg::with_name("clean")
                               .short("C")
                               .long("clean")
                               .help("Starts Eve with a clean database and no watchers (false)"))
                          .get_matches();

    println!("");

    let eve_flags = EveFlags {
        clean: matches.is_present("clean"),
        editor: matches.is_present("editor"),
        watch: matches.is_present("watch"),
    };

    let eve_paths = EvePaths::new(
        eve_flags.clean,
        matches.values_of("EVE_FILES").map_or(
            vec![],
            |files| {
                files.collect()
            },
        ),
        matches.value_of("server-file").map_or(
            vec![],
            |file| {
                vec![file]
            },
        ),
        matches.value_of("persist"),
        matches.value_of("libraries-path"),
        matches.value_of("programs-path"),
    );

    let wport = matches.value_of("port")
                       .unwrap_or("3012");
    let hport = matches.value_of("http-port")
                       .unwrap_or("8081");
    let address = matches.value_of("address")
                         .unwrap_or("127.0.0.1");
    let http_address = format!("{}:{}", address, hport);
    let websocket_address = format!("{}:{}", address, wport);

    http_server(http_address);
    websocket_server(websocket_address, &eve_paths, &eve_flags);
}
