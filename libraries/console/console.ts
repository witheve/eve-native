import {Library, RawValue, handleTuples} from "../../ts";

const EMPTY:never[] = [];

export class Console extends Library {
  static id = "console";
  setup() {
    if(typeof console === "undefined") {
      this.handlers = {} as any;
    }
  }

  handlers = {
    "log": handleTuples(({adds}) => {
      for(let add of adds || EMPTY) console.log.apply(console, add);
    }),
    "warn": handleTuples(({adds}) => {
      for(let add of adds || EMPTY) console.warn.apply(console, add);
    }),
    "error": handleTuples(({adds}) => {
      for(let add of adds || EMPTY) console.error.apply(console, add);
    }),
    "diff": handleTuples(({adds, removes}) => {
      for(let remove of removes || EMPTY) console.info.apply(console, ["- "].concat(remove as any[]));
      for(let add of adds || EMPTY) console.info.apply(console, ["+ "].concat(add as any[]));
    })
  }
}

Library.register(Console.id, Console);
