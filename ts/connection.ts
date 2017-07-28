import {RawValue, RawTuple, Diff} from "./library";

type Tuple = [string]|RawValue[];

interface Message {
  adds?:Tuple[];
  removes?:Tuple[];
}

const EMPTY:any[] = [];

export class Connection {
  _queue:string[] = [];
  connected = false;

  handlers:{[type:string]: (data:any) => void} = {};

  constructor(public ws:WebSocket) {
    ws.addEventListener("open", () => this._opened());
    ws.addEventListener("close", (event) => this._closed(event.code, event.reason));
    ws.addEventListener("message", (event) => this._messaged(event.data));

  }
  send(type:string, data:any) {
    console.log(type, data);
    // This... feels weird. Do we actually expect to pack multiple message types in very frequently?
    let payload = JSON.stringify({[type]: data});
    this._queue.push(payload);
    this._trySend();
  }

  protected _trySend() {
    if(this.connected) {
      // @NOTE: this doesn't gracefully handle partial processing of the queue.
      while(this._queue.length) {
        let payload = this._queue.shift();
        this.ws.send(payload);
      }
    }
  }

  protected _opened() {
    this.connected = true;
    this._trySend();
  }

  protected _closed = (code:number, reason:string) => {
    this.connected = false;
    console.warn("Connection closed.", code, reason);
  }

  protected _messaged = (payload:string) => {
    console.group();
    let parsed:Message;
    try {
      parsed = JSON.parse(payload);
    } catch(err) {
      console.error("Received malformed WS message: '" + payload + "'.");
      return;
    }
    let types:{[type:string]: Diff<RawTuple[]>} = {};
    for(let add of parsed.adds || EMPTY) {
      let type = add[0];
      let rest = add.slice(1);
      if(!types[type]) types[type] = {adds: [rest], removes: []};
      else types[type].adds!.push(rest);
    }
    for(let remove of parsed.removes || EMPTY) {
      let type = remove[0];
      let rest = remove.slice(1);
      if(!types[type]) types[type] = {adds: [], removes: [rest]};
      else types[type].removes!.push(rest);
    }
    for(let type in this.handlers) {
      if(types[type]) {
        let diff = types[type];
        console.log(`Received '${type}' with data:`, diff);
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
    console.groupEnd();
  }
}
