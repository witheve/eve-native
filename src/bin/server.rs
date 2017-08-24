extern {}

use std::path::{Path};

extern crate clap;
use clap::{Arg, App};

extern crate ws;
use ws::{listen, Message, Sender as WSSender, Handler, CloseCode};

#[macro_use]
extern crate serde_derive;

extern crate serde_json;
extern crate serde;
use serde_json::{Error};

use std::sync::mpsc::Sender;

extern crate time;

extern crate eve;
use eve::paths::EvePaths;
use eve::ops::{ProgramRunner, RunLoop, RawChange, Internable, Persister, JSONInternable};
use eve::watchers::system::{SystemTimerWatcher, PanicWatcher};
use eve::watchers::compiler::{CompilerWatcher};
use eve::watchers::textcompiler::{RawTextCompilerWatcher};
use eve::watchers::console::{ConsoleWatcher};
use eve::watchers::editor::EditorWatcher;
use eve::watchers::remote::{Router, RouterMessage, RemoteWatcher};
use eve::watchers::websocket::WebsocketClientWatcher;

extern crate iron;
extern crate staticfile;
extern crate mount;

use iron::{Iron, Chain, status, Request, Response, IronResult, IronError, AfterMiddleware};
use staticfile::Static;
use mount::Mount;
use std::thread;
use std::sync::{Arc, Mutex};
use std::ops::Deref;

extern crate term_painter;
use self::term_painter::ToStyle;
use self::term_painter::Color::*;

//-------------------------------------------------------------------------
// Websocket client handler
//-------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug)]
pub enum ClientMessage {
    Block { id:String, code:String },
    RemoveBlock { id:String },
    Transaction { client:String, adds: Vec<(JSONInternable, JSONInternable, JSONInternable)>, removes: Vec<(JSONInternable, JSONInternable, JSONInternable)> },
}

pub struct ClientHandler {
    out: WSSender,
    running: RunLoop,
    client_name: String,
    router: Arc<Mutex<Router>>,
    router_channel: Sender<RouterMessage>
}

impl ClientHandler {
    pub fn new(out:WSSender, router: Arc<Mutex<Router>>, eve_paths:&EvePaths, clean: bool, editor: bool, client_name:&str) -> ClientHandler {
        let router_channel = router.lock().expect("ERROR: Failed to lock router: Cannot clone channel.").deref().get_channel();
        let mut runner = ProgramRunner::new(client_name);
        let outgoing = runner.program.outgoing.clone();
        router.lock().expect("ERROR: Failed to lock router: Cannot register new client.").register(&client_name, outgoing.clone());
        if !clean {
            runner.program.attach(Box::new(SystemTimerWatcher::new(outgoing.clone())));
            runner.program.attach(Box::new(CompilerWatcher::new(outgoing.clone(), false)));
            runner.program.attach(Box::new(RawTextCompilerWatcher::new(outgoing.clone())));
            runner.program.attach(Box::new(WebsocketClientWatcher::new(out.clone(), client_name)));
            runner.program.attach(Box::new(ConsoleWatcher::new()));
            runner.program.attach(Box::new(PanicWatcher::new()));
            runner.program.attach(Box::new(RemoteWatcher::new(client_name, &router.lock().expect("ERROR: Failed to lock router: Cannot init RemoteWatcher.").deref())));
            if editor {
                let editor_watcher = EditorWatcher::new(&mut runner, router.clone(), out.clone(), eve_paths.libraries(), eve_paths.programs());
                runner.program.attach(Box::new(editor_watcher));
            }
        }

        if let Some(path) = eve_paths.libraries() {
            runner.load(path);
        }
        for file in eve_paths.files.iter() {
            runner.load(file);
        }

        let running = runner.run();

        ClientHandler {out, running, client_name: client_name.to_owned(), router, router_channel }
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
            let deserialized: Result<ClientMessage, Error> = serde_json::from_str(&s);
            // println!("deserialized = {:?}", deserialized);
            match deserialized {
                Ok(ClientMessage::Transaction { client, adds, removes }) => {
                    let mut raw_changes = vec![];
                    raw_changes.extend(adds.into_iter().map(|(e,a,v)| {
                        RawChange { e:e.into(), a:a.into(), v:v.into(), n:Internable::String("input".to_string()),count:1 }
                    }));
                    raw_changes.extend(removes.into_iter().map(|(e,a,v)| {
                        RawChange { e:e.into(), a:a.into(), v:v.into(), n:Internable::String("input".to_string()),count:-1 }
                    }));

                    self.router_channel.send(RouterMessage::Local(client, raw_changes)).expect("ERROR: Failed to send message to client");
                }
                _ => { }
            }
            Ok(())
        } else {
            Ok(())
        }
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
        println!("WebSocket closing for ({:?}) {}", code, reason);
        self.router.lock().unwrap().unregister(&self.client_name);
        self.running.close();
    }
}

//-------------------------------------------------------------------------
// Static File Server
//-------------------------------------------------------------------------

struct Custom404;

impl AfterMiddleware for Custom404 {
    fn catch(&self, _: &mut Request, _: IronError) -> IronResult<Response> {
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

        println!("{} HTTP Server at {}... ", BrightGreen.paint("Starting:"), address);
        match Iron::new(chain).http(&address) {
            Ok(_) => {},
            Err(why) => println!("{} Failed to start HTTP Server: {}", BrightRed.paint("Error:"), why),
        };

    })
}

fn websocket_server(address: String, eve_paths:&EvePaths, clean: bool, editor: bool) {
    println!("{} Websocket Server at {}... ", BrightGreen.paint("Starting:"), address);

    // create a server program
    let mut runner = ProgramRunner::new("server");
    let outgoing = runner.program.outgoing.clone();
    let router = Arc::new(Mutex::new(Router::new(outgoing.clone())));
    router.lock().unwrap().register("server", outgoing.clone());

    if !clean {
        runner.program.attach(Box::new(SystemTimerWatcher::new(outgoing.clone())));
        runner.program.attach(Box::new(CompilerWatcher::new(outgoing.clone(), false)));
        runner.program.attach(Box::new(RawTextCompilerWatcher::new(outgoing)));
        runner.program.attach(Box::new(ConsoleWatcher::new()));
        runner.program.attach(Box::new(PanicWatcher::new()));
        runner.program.attach(Box::new(RemoteWatcher::new("server", &router.lock().unwrap().deref())));
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
        ClientHandler::new(out, router.clone(), eve_paths, clean, editor, &client_name)
    }) {
        Ok(_) => {},
        Err(why) => println!("{} Failed to start Websocket Server: {}", BrightRed.paint("Error:"), why),
    };
}

//-------------------------------------------------------------------------
// Main
//-------------------------------------------------------------------------

fn main() {
    let matches = App::new("Eve")
                          .version("0.4")
                          .author("Kodowa Inc.")
                          .about("Creates an instance of the Eve server. Default values for options are in parentheses.")
                          .arg(Arg::with_name("editor")
                               .short("E")
                               .long("editor")
                               .help("Attaches an editor instance to each client program."))
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

    let clean = matches.is_present("clean");
    let editor = matches.is_present("editor");

    let eve_paths = EvePaths::new(clean,
                                  matches.values_of("EVE_FILES").map_or(vec![], |files| files.collect()),
                                  matches.value_of("server-file").map_or(vec![], |file| vec![file]),
                                  matches.value_of("persist"),
                                  matches.value_of("libraries-path"),
                                  matches.value_of("programs-path"));

    let wport = matches.value_of("port").unwrap_or("3012");
    let hport = matches.value_of("http-port").unwrap_or("8081");
    let address = matches.value_of("address").unwrap_or("127.0.0.1");
    let http_address = format!("{}:{}",address,hport);
    let websocket_address = format!("{}:{}",address,wport);

    http_server(http_address);
    websocket_server(websocket_address, &eve_paths, clean, editor);
}
