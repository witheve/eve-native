import {Program, Library, Diff, RawEAV, RawTuple, libraries} from ".";
import {Connection, Message} from "./connection";

export interface DiffMessage extends Message { type: "diff"; adds?:RawTuple[]; removes?:RawTuple[]; }
export interface LoadBundleMessage extends Message { type: "load-bundle"; bundle: string }

interface Bundle { users: string[], css?: HTMLLinkElement, js?: HTMLScriptElement }

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
  bundles:{[name:string]: Bundle} = {};
  panes:{[client:string]: HTMLElement} = {};

  handlers = {
    "init": ({client}:Message) => {
      if(this.programs[client]) throw new Error(`Unable to initialize existing program: '${client}'.`);
      let program = this.programs[client] = new RemoteProgram(client, (type: string, diff: any) => this.send(type, diff, client));
      let html = program.attach("html") as libraries.HTML;
      this.addPane(client, html.getContainer());
      program.attach("canvas");
      program.attach("console");
      program.attach("stream");
      program.attach("code-block");
    },
    "diff": (diff:DiffMessage) => {
      let program = this.programs[diff.client];
      if(!program) throw new Error(`Unable to handle diff for unitialized program: '${diff.client}'.`);
      program.handleDiff(diff);
    },
    "load-bundle": ({bundle, client}:LoadBundleMessage) => {
      this.loadBundle(bundle, client);
    }
  };

  loadBundle(name:string, user:string) {
    let bundle = this.bundles[name];
    if(bundle) {
      if(bundle.users.indexOf(user) === -1) {
        bundle.users.push(user);
      }
    } else {
      let css = document.createElement("link");
      css.setAttribute("rel", "stylesheet");
      css.setAttribute("type", "text/css");
      css.setAttribute("href", `/dist/${name}.css`);
      document.head.appendChild(css);

      let js = document.createElement("script");
      js.setAttribute("src", `/dist/${name}.js`);
      document.head.appendChild(js);

      bundle = {
        users: [user],
        css,
        js
      };
      this.bundles[name] = bundle;
    }
  }

  addPane(name:string, container:HTMLElement) {
    if(this.panes[name] && this.panes[name] !== container) {
      console.warn(`Overwriting container for existing pane '${name}'`);
    }
    this.panes[name] = container;
    container.classList.add("program-pane");
  }
}

let connection = new MultiplexedConnection(new WebSocket(`ws://${location.hostname}:3012`));

console.log(connection);
