import {Program, Library, Diff, RawEAV, libraries} from ".";
import {Connection} from "./connection";

class RemoteProgram implements Program {
  libraries = {};
  attach(libraryId:string):Library {
    return Library.attach(this, libraryId);
  }
  attached(libraryId:string, library:Library) {
    for(let handlerName in library.handlers) {
      this.conn.handlers[`${libraryId}/${handlerName}`] = library.handlers[handlerName];
    }
  }

  conn:Connection;
  constructor(public url:string, public name = "Remote Client") {
    this.connect(url);
  }

  connect(url:string) {
    // @FIXME: if watchers are added asynchronously, results can get dropped on the floor.
    this.conn = new Connection(new WebSocket(url));
  }

  inputEAVs(eavs:RawEAV[]) {
    let diff:Diff<RawEAV[]> = {adds: eavs, removes: []};
    this.conn.send("Transaction", diff);
    return this;
  }
}

let program = new RemoteProgram(`ws://${location.hostname}:3012`);
program.attach("html");
// program.inputEAVs([
//   [1, "tag", "html/element"],
//   [1, "tagname", "div"],
//   [1, "text", "hi!"]
// ]);

console.log(program);
