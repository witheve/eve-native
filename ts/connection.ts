import {RawValue, RawTuple, Diff} from "./library";

export type Tuple = [string]|RawValue[];

export interface Message {
  type:string;
  client:string;
}

export interface DiffMessage extends Message {
  type: "diff";
  adds?:Tuple[];
  removes?:Tuple[];

}

const EMPTY:any[] = [];

export class Connection {
  _queue:string[] = [];
  connected = false;

  handlers:{[type:string]: (data:Message) => void} = {};

  constructor(public ws:WebSocket) {
    ws.addEventListener("open", () => this._opened());
    ws.addEventListener("close", (event) => this._closed(event.code, event.reason));
    ws.addEventListener("message", (event) => this._messaged(event.data));

  }
  send(type:string, data:any, client?: string) {
    // console.groupCollapsed("Sent");
    // console.log(type, data, client);
    // console.groupEnd();
    // This... feels weird. Do we actually expect to pack multiple message types in very frequently?
    data.client = client;
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
    let parsed:Message;
    try {
      parsed = JSON.parse(payload);
    } catch(err) {
      console.error("Received malformed WS message: '" + payload + "'.");
      return;
    }

    if(this.handlers[parsed.type]) {
      // console.group(`Received ${parsed.type} from ${parsed.client}`);
      this.handlers[parsed.type](parsed);
      // console.groupEnd();
    }
  }
}
