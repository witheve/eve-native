import {Program, Library, Diff, RawEAV, RawTuple, libraries} from ".";
import {Connection, Message} from "./connection";

export interface DiffMessage extends Message { type: "diff"; adds?:RawTuple[]; removes?:RawTuple[]; }
export interface LoadBundleMessage extends Message { type: "load-bundle"; bundle: string }

const EMPTY:never[] = [];

class RemoteProgram implements Program {
  libraries = {};
  handlers:{[id:string]: (diff:Diff<RawTuple[]>) => void} = {};

  attach(libraryId:string):Library {
    return Library.attach(this, libraryId);
  }
  attached(libraryId:string, library:Library) {
    for(let handlerName in library.handlers) {
      this.handlers[`${libraryId}/${handlerName}`] = library.handlers[handlerName];
    }
  }

  constructor(public name = "Remote Client", public send:(type: string, diff: any) => void) {}

  inputEAVs(eavs:RawEAV[]) {
    let diff:Diff<RawEAV[]> = {adds: eavs, removes: []};
    this.send("Transaction", diff);
    return this;
  }

  handleDiff(diff:Diff<RawTuple[]>) {
    let types:{[type:string]: Diff<RawTuple[]>} = {};
    for(let add of diff.adds || EMPTY) {
      let type = add[0];
      let rest = add.slice(1);
      if(!types[type]) types[type] = {adds: [rest], removes: []};
      else types[type].adds!.push(rest);
    }
    for(let remove of diff.removes || EMPTY) {
      let type = remove[0];
      let rest = remove.slice(1);
      if(!types[type]) types[type] = {adds: [], removes: [rest]};
      else types[type].removes!.push(rest);
    }
    for(let type in this.handlers) {
      if(types[type]) {
        let diff = types[type];
        // console.log(`Received '${type}' with data:`, diff);
        try {
          this.handlers[type](diff);
          types[type] = undefined as any;
        } catch(err) {
          this.send("notice", {type: "error", name: err.name, message: err.message});
          console.error(err);
          return;
        }
      }
    }
    for(let type in types) {
      let diff = types[type];
      if(diff) {
        console.warn(`Received unhandled message '${type}' with data:`, diff);
      }
    }
  }
}

class MultiplexedConnection extends Connection {
  programs:{[client:string]: RemoteProgram} = {};
  handlers = {
    "init": ({client}:Message) => {
      if(this.programs[client]) throw new Error(`Unable to initialize existing program: '${client}'.`);
      let program = this.programs[client] = new RemoteProgram(client, (type: string, diff: any) => this.send(type, diff, client));
      program.attach("html");
      program.attach("canvas");
      program.attach("console");
    },
    "diff": (diff:DiffMessage) => {
      let program = this.programs[diff.client];
      if(!program) throw new Error(`Unable to handle diff for unitialized program: '${diff.client}'.`);
      program.handleDiff(diff);
    }
  };
}

let connection = new MultiplexedConnection(new WebSocket(`ws://${location.hostname}:3012`));

// let program = new RemoteProgram();
// program.attach("html");
// program.attach("canvas");
// program.attach("console");


console.log(connection);
