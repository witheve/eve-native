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
use eve::watcher::{SystemTimerWatcher, CompilerWatcher, Watcher};

extern crate hyper;
use hyper::header::{ContentLength, Headers, ContentType};
use hyper::server::{Http, Request, Response, Service};
use hyper::mime::CSS;
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use std::path::Path;
use std::fs::File;
use std::io;
use std::io::prelude::*;

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
    pub fn new(out:Sender, files:&Vec<&str>, persist:Option<&str>) -> ClientHandler {

        let mut runner = ProgramRunner::new();
        let outgoing = runner.program.outgoing.clone();
        runner.program.attach("system/timer", Box::new(SystemTimerWatcher::new(outgoing.clone())));
        runner.program.attach("eve/compiler", Box::new(CompilerWatcher::new(outgoing)));
        runner.program.attach("client/websocket", Box::new(WebsocketClientWatcher::new(out.clone())));

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
    outgoing: Sender,
}

impl WebsocketClientWatcher {
    pub fn new(outgoing: Sender) -> WebsocketClientWatcher {
        WebsocketClientWatcher { outgoing }
    }
}

impl Watcher for WebsocketClientWatcher {
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

struct StaticFileServer;

fn file_exists(path: &Path) -> bool {
    path.is_file() && path.exists()
}

fn serve_file(mut res: Response, path: &Path) -> Response {
    let file_path = if file_exists(path) {
        path
    } else {
        //*res.status_mut() = hyper::status::StatusCode::NotFound;
        Path::new("404.html")
    };

    let file = read_file_bytes(file_path.to_str().unwrap());
    //let mut headers = Headers::new();
    
    /*
    headers.set(
        match path.extension().unwrap().to_str().unwrap() {
            _ => ContentType(CSS),
        }
    );*/

    //let mime: Mime = Mime::from_str(mime_types.mime_for_path(Path::new(file_path))).unwrap();
    //res.headers_mut().set(ContentType(mime));
    //let mut res = try!(res.start());
    //try!(res.write_all(&file));
    //try!(res.end());
    res.set_body(file);
    res
}

fn read_file_bytes(filename: &str) -> Vec<u8> {
    let mut file = File::open(filename).unwrap();
    let mut contents: Vec<u8> = Vec::new();
    file.read_to_end(&mut contents).unwrap();
    contents
}

impl Service for StaticFileServer {
    // boilerplate hooking up hyper's server types
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    // The future representing the eventual Response your call will
    // resolve to. This can change to whatever Future you need.
    type Future = futures::future::FutureResult<Self::Response, Self::Error>;
    
    fn call(&self, req: Request) -> Self::Future {
        let mut response = Response::new();
        match (req.method(), req.path()) {
            (&hyper::Get, path) => {
                let relative_path = Path::new(path).strip_prefix("/").unwrap();
                let requested_file = match relative_path.file_name() {
                    Some(file_name) => file_name.to_str().unwrap(),
                    None => "",
                };
                let file_str = relative_path.to_str().unwrap();
                match relative_path.to_str().unwrap() {
                    "index.html" | "" => {
                        println!("Serving Index!");
                        response = serve_file(response, Path::new("index.html"));
                    },
                    _ => {
                        println!("Serving {:?}", relative_path);
                        response = serve_file(response, relative_path);
                    }
                };
            },
            _ => println!("Unknown Request"),
        };
        futures::future::ok(
            response
        )
    }
}

//-------------------------------------------------------------------------
// Main
//-------------------------------------------------------------------------

fn main() {
    let matches = App::new("Eve")
                          .version("0.4")
                          .author("Kodowa Inc.")
                          .about("Creates an instance of the Eve server")
                          .arg(Arg::with_name("persist")
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
                               .value_name("PORT")
                               .help("Sets the port for the server")
                               .takes_value(true))
                          .get_matches();
    let addr = "0.0.0.0:8080".parse().unwrap();
    print!("{} HTTP Server at {}... ", Green.paint("Starting:"), addr);
    let http_server = thread::spawn(move || {
        let server = Http::new().bind(&addr, || Ok(StaticFileServer)).unwrap();
		server.run().unwrap();
	});
    print!("done.\n");
    let ws_addr = "0.0.0.0:3012";
    sleep(Duration::from_millis(500));
    print!("{} Websocket Server at {}... ", Green.paint("Starting:"), ws_addr);
    let ws_server = thread::spawn(move || {
        let port = matches.value_of("port").unwrap_or("3012");    
        let files = match matches.values_of("EVE_FILES") {
            Some(fs) => fs.collect(),
            None => vec![]
        };
        let persist = matches.value_of("persist");
        listen(ws_addr, |out| {
            ClientHandler::new(out, &files, persist)
        }).unwrap()
    });
    print!("done.\n");
    let _ = http_server.join();
    let _ = ws_server.join();
}