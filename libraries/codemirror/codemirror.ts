import {Library, RawValue, RawEAV, handleTuples, libraries} from "../../ts";
import "codemirror/mode/gfm/gfm";
import "codemirror/addon/mode/simple";
import "codemirror/addon/mode/overlay";
import "codemirror-mode-eve";
import CodeMirror from "codemirror";

const EMPTY:never[] = [];

export class EveCodeMirror extends Library {
  static id = "code-block";
  blocks: any = {};
  html:libraries.HTML;

  setup() {
    this.html = this.program.attach("html") as libraries.HTML;
  }
  
  handlers = {
    "create": handleTuples(({adds, removes}) => {
      for(let [blockID, mode] of adds || EMPTY) {       
        if (this.blocks[blockID] !== undefined) {
          continue;
        }
        let element: any = this.html.getInstances(blockID)
        var code_block = CodeMirror(element[0], {
          mode:  mode
        });
        code_block.on("change", (editor, change) => {
          if(change.origin === "setValue") return;
          let change_id = `codemirror/code-block/change|${blockID}`;
          let new_value = editor.getValue();
          this.program.inputEAVs([
            [change_id, "tag", "codemirror/code-block/change"],
            [change_id, "block", blockID],
            [change_id, "new-value", new_value],
            [change_id, "origin", change.origin]]);
        });
        this.blocks[blockID] = code_block;
      }
      for(let [blockID, value, mode] of removes || EMPTY) { 
        delete this.blocks[blockID];
      }
    }),
    "update": handleTuples(({adds}) => {
      for(let [blockID, value] of adds || EMPTY) {
        let editor: CodeMirror.Editor = this.blocks[blockID] 
        let doc = editor.getDoc();
        let cursor = doc.getCursor()
        doc.setValue(value as string);
        doc.setCursor(cursor);
      }
    })
  }
}

Library.register(EveCodeMirror.id, EveCodeMirror);
