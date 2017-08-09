import {Library, RawValue, handleTuples} from "../../ts";
import "codemirror/mode/gfm/gfm";
import "codemirror/addon/mode/simple";
import "codemirror-mode-eve";
import CodeMirror from "codemirror";

const EMPTY:never[] = [];

export class CodeBlock extends Library {
  static id = "code-block";
  setup() {
    console.log("Starting up", CodeMirror);
  }

  handlers = {
    "create": handleTuples(({adds}) => {
      console.log("Creating a CM instance");
      let element: any = document.getElementById("testing");
      var myCodeMirror = CodeMirror(element.parentElement, {
        mode:  "eve"
      });
      for(let add of adds || EMPTY) {
        console.log(add);
      };
    }),
  }
}

Library.register(CodeBlock.id, CodeBlock);
