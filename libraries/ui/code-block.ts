import {Library, RawValue, RawEAV, handleTuples, libraries} from "../../ts";
import "codemirror/mode/gfm/gfm";
import "codemirror/addon/mode/simple";
import "codemirror-mode-eve";
import CodeMirror from "codemirror";

const EMPTY:never[] = [];

export class CodeBlock extends Library {
  static id = "code-block";
  blocks: any = {};

  html:libraries.HTML;

  setup() {
    this.html = this.program.attach("html") as libraries.HTML;
  }
  
  handlers = {
    "create": handleTuples(({adds}) => {
      for(let [blockID, value, init] of adds || EMPTY) {
        if (init === "1") break
        let element: any = this.html.getInstances(blockID)
        var code_block = CodeMirror(element[0], {
          mode:  "eve"
        });
        code_block.on("change", (editor, change) => {
          if(change.origin === "setValue") return;
          let changes: RawEAV[] = [];
          let change_id = `ui/code-block/change|${blockID}`;
          let new_value = editor.getValue();
          changes.push(
            [change_id, "tag", "ui/code-block/change"],
            [change_id, "block", blockID],
            [change_id, "new-value", new_value],
            [change_id, "origin", change.origin]
          );
          this.program.inputEAVs(changes);
          
        });
        this.blocks[blockID] = code_block;
        this.program.inputEAVs([[blockID, "init", "1"]]);
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

Library.register(CodeBlock.id, CodeBlock);
