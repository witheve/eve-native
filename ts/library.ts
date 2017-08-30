import {v4 as uuid} from "uuid";

// Raw values.
export type RawValue = number|string;
export type RawTuple = RawValue[];
export type RawEAV = [RawValue, RawValue, RawValue];
export interface RawMap<V> {[key:string]: V, [key:number]: V};
export type RawRecord = RawMap<RawValue|RawValue[]>;

// Diffs.
export type Diff<T> = {adds?: T, removes?: T};
export type DiffHandler = (diff:Diff<RawTuple[]>) => void
export type EAVDiffHandler = (diff:Diff<RawEAV[]>) => void
export type RecordDiffHandler<T extends RawRecord> = (diff:Diff<RawMap<T>>) => void

////////////////////////////////////////////////////////////////////////////////
// Program
////////////////////////////////////////////////////////////////////////////////

export interface Program {
  name:string;
  libraries:{[id:string]: Library};

  inputEAVs(eavs:RawEAV[]):this;
  attach(libraryId:string):Library;
  attached(libraryId:string, library:Library):void;
}


////////////////////////////////////////////////////////////////////////////////
// Library
////////////////////////////////////////////////////////////////////////////////

export class Library {
  protected static _registry:{[id:string]: typeof Library} = {};

  static register(id:string, library:typeof Library) {
    if(this._registry[id]) {
      if(this._registry[id] === library) return;
      throw new Error(`Attempting to overwrite existing library with id '${id}'`);
    }
    this._registry[id] = library;
  }

  static unregister(id:string) {
    delete this._registry[id];
  }

  static get(id:string) {
    let library = this._registry[id];
    if(library) return library;
  }

  static attach(program:Program, libraryId:string):Library {
    let LibraryCtor = Library.get(libraryId);
    if(!LibraryCtor) throw new Error(`Unable to attach unknown library '${libraryId}'.`);
    if(program.libraries[libraryId]) return program.libraries[libraryId];
    let library:Library = new LibraryCtor(program);
    program.libraries[libraryId] = library;
    library.setup();
    program.attached(libraryId, library);
    return library;
  }

  static id:string;
  handlers:{[name:string]: DiffHandler};

  protected _order: string[]|undefined;

  get order() {
    if(this._order) return this._order;
    return this._order = Object.keys(this.handlers);
  }
  get program() { return this._program; }

  constructor(protected _program:Program) {}

  setup() {}
}

////////////////////////////////////////////////////////////////////////////////
// Handlers
////////////////////////////////////////////////////////////////////////////////

// Just a convenience fn for type hinting.
export function handleTuples(handler:DiffHandler): DiffHandler {
  return handler;
}

export function handleEAVs(handler:EAVDiffHandler): DiffHandler {
  return (diffs) => {
    let sample = (diffs.adds && diffs.adds[0]) || (diffs.removes && diffs.removes[0]);
    if(sample) {
      if(sample.length < 3) throw new Error(`Unable to parse EAV from tuple with < 3 fields. ${JSON.stringify(sample)}`);
      if(sample.length > 3) console.warn(`Expected 3 values to parse EAV from tuple, got: ${sample.length}`);
    }
    handler(diffs as Diff<RawEAV[]>);
  };
}

export function handleRecords<T extends RawRecord>(attributes: string[], handler:RecordDiffHandler<T>): DiffHandler {
  return (diffs) => {
    let recordDiffs:Diff<RawMap<T>> = {adds: {}, removes: {}};
    if(diffs.adds) {
      for(let add of diffs.adds) {
        let id = add[0];
        if(!id) throw new Error(`Unable to create record with undefined id`);
        recordDiffs.adds![id] = tupleToRecord<T>(attributes, add, recordDiffs.adds![id]);
      }
    }
    if(diffs.removes) {
      for(let remove of diffs.removes) {
        let id = remove[0];
        if(!id) throw new Error(`Unable to remove record with undefined id`);
        recordDiffs.removes![id] = tupleToRecord<T>(attributes, remove, recordDiffs.removes![id]);
      }
    }
    handler(recordDiffs);
  };
}

////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////

export function asValue(value:RawValue|undefined) {
  if(typeof value == "string") {
    if(value == "true") return true;
    if(value == "false") return false;
  }
  return value;
}

export function createId() {
  return "|" + uuid();
}

export function tupleToRecord<T extends RawRecord>(attributes: string[], tuple:RawTuple, record:RawRecord = {}): T {
  let ix = 1; // First slot is ID
  let asArrays = false;
  for(let attr of attributes) {
    if(attr === "|") {
      asArrays = true;
      continue;
    }

    let value = tuple[ix];
    if(value === undefined) throw new Error(`Unable to unpack tuple '${JSON.stringify(tuple)}' into record with attributes '${JSON.stringify(attributes)}'.`);
    if(asArrays) {
      if(record[attr]) (record[attr] as RawValue[]).push(value);
      else record[attr] = [value];
    } else {
      record[attr] = value;
    }
    ix += 1;
  }
  return record as T;
}

export function recordToTuples(attributes: string[], record:RawRecord, id:RawValue, tuples:RawTuple[] = []):RawTuple[] {
  throw new Error("@TODO: Implement me!");
}

export function recordToEAVs(record:RawRecord, id:RawValue, eavs:RawEAV[] = []):RawEAV[] {
  throw new Error("@TODO: Implement me!");
}
