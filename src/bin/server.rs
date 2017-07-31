extern {}
extern crate tokio_timer;
extern crate futures;

extern crate clap;
use clap::{Arg, App};

extern crate ws;
use ws::{listen, Message, Sender, Handler, CloseCode};

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate serde_json;
extern crate serde;
use serde_json::{Error};

extern crate eve;
extern crate time;
use eve::ops::{ProgramRunner, RunLoop, RunLoopMessage, RawChange, Internable, Interner, Persister, JSONInternable};
use eve::indexes::{WatchDiff};
use eve::watchers::{Watcher};
use eve::watchers::system::{SystemTimerWatcher};
use eve::watchers::compiler::{CompilerWatcher};

extern crate iron;
extern crate staticfile;
extern crate mount;

use std::path::Path;
use iron::{Iron, Chain, status, Request, Response, IronResult, IronError, AfterMiddleware};
use staticfile::Static;
use mount::Mount;
use std::thread;

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
    Transaction { adds: Vec<(JSONInternable, JSONInternable, JSONInternable)>, removes: Vec<(JSONInternable, JSONInternable, JSONInternable)> },
    Yo { message:String },
}

pub struct ClientHandler {
    out: Sender,
    running: RunLoop,
}

impl ClientHandler {
    pub fn new(out:Sender, files:&Vec<&str>, persist:Option<&str>, clean: bool) -> ClientHandler {
        println!("Creating a new guy!");
        let mut runner = ProgramRunner::new();
        let outgoing = runner.program.outgoing.clone();
        if !clean {
            runner.program.attach(Box::new(SystemTimerWatcher::new(outgoing.clone())));
            runner.program.attach(Box::new(CompilerWatcher::new(outgoing)));
            runner.program.attach(Box::new(WebsocketClientWatcher::new(out.clone())));
        }
        
        if let Some(persist_file) = persist {
            let mut persister = Persister::new(persist_file);
            persister.load(persist_file);
            runner.persist(&mut persister);
        }

        for file in files {
            runner.load(file);
        }

        let running = runner.run();
        
        ClientHandler {out, running}
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
                Ok(ClientMessage::Transaction { adds, removes }) => {
                    let mut raw_changes = vec![];
                    raw_changes.extend(adds.into_iter().map(|(e,a,v)| {
                        RawChange { e:e.into(), a:a.into(), v:v.into(), n:Internable::String("input".to_string()),count:1 }
                    }));
                    raw_changes.extend(removes.into_iter().map(|(e,a,v)| {
                        RawChange { e:e.into(), a:a.into(), v:v.into(), n:Internable::String("input".to_string()),count:-1 }
                    }));
                    self.running.send(RunLoopMessage::Transaction(raw_changes));
                }
                _ => { }
            }
            self.out.send(Message::text(serde_json::to_string(&ClientMessage::Yo {message: format!("{} - yo", s)}).unwrap()))
        } else {
            Ok(())
        }
    }

    fn on_close(&mut self, code: CloseCode, reason: &str) {
        println!("WebSocket closing for ({:?}) {}", code, reason);
        self.running.close();
    }
}

//-------------------------------------------------------------------------
// Websocket client watcher
//-------------------------------------------------------------------------

pub struct WebsocketClientWatcher {
    name: String,
    outgoing: Sender,
}

impl WebsocketClientWatcher {
    pub fn new(outgoing: Sender) -> WebsocketClientWatcher {
        WebsocketClientWatcher { name: "client/weboscket".to_string(), outgoing }
    }
}

impl Watcher for WebsocketClientWatcher {
    fn get_name(& self) -> String {
        self.name.clone()
    }
    fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
    fn on_diff(&mut self, interner:&mut Interner, diff:WatchDiff) {
        let adds:Vec<Vec<JSONInternable>> = diff.adds.iter().map(|row| {
            row.iter().map(|v| interner.get_value(*v).into()).collect()
        }).collect();
        let removes:Vec<Vec<JSONInternable>> = diff.removes.iter().map(|row| {
            row.iter().map(|v| interner.get_value(*v).into()).collect()
        }).collect();
        let text = serde_json::to_string(&json!({"adds": adds, "removes": removes})).unwrap();
        self.outgoing.send(Message::Text(text)).unwrap();
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

fn websocket_server(address: String, files:&Vec<&str>, persist:Option<&str>, clean: bool) {
    println!("{} Websocket Server at {}... ", BrightGreen.paint("Starting:"), address);
    match listen(address, |out| {
        ClientHandler::new(out, files, persist, clean)
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
                          .arg(Arg::with_name("persist")
                               .short("s")
                               .long("persist")
                               .value_name("FILE")
                               .help("Sets the name for the database to load from and write to")
                               .takes_value(true))
                          .arg(Arg::with_name("EVE_FILES")
                               .help("The eve files and folders to load")
                               .required(true)
                               .multiple(true))
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
                               .long("Clean")
                               .help("Starts Eve with a clean database and no watchers (false)"))                         
                          .get_matches();

    println!("");
    let wport = matches.value_of("port").unwrap_or("3012");
    let hport = matches.value_of("http-port").unwrap_or("8081");
    let address = matches.value_of("address").unwrap_or("127.0.0.1");
    let http_address = format!("{}:{}",address,hport);
    let websocket_address = format!("{}:{}",address,wport);
    let files = match matches.values_of("EVE_FILES") {
        Some(fs) => fs.collect(),
        None => vec![]
    };
    let persist = matches.value_of("persist");
    let clean = matches.is_present("clean");

    http_server(http_address);
    websocket_server(websocket_address, &files, persist, clean);
}
