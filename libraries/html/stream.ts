import {Library, createId, RawValue, RawEAV, handleTuples, libraries} from "../../ts";
import Hls from "hls.js"

const EMPTY:never[] = [];

export class Stream extends Library {
  static id = "stream";
  streams: any = {};

  html:libraries.HTML;

  setup() {
    this.html = this.program.attach("html") as libraries.HTML;
  }
  
  handlers = {
    "create": handleTuples(({adds}) => {
      for(let [streamID, source] of adds || EMPTY) {        
        if(Hls.isSupported()) {
            let video: any = document.getElementById(`${streamID}`);
            var hls = new Hls();
            let program = this.program
            hls.loadSource(`${source}`);
            hls.attachMedia(video);
            hls.on(Hls.Events.MANIFEST_PARSED,function() {
    
            });
            video.onplay = function () {
              let play_id = createId();
              program.inputEAVs([
                [play_id, "tag", "html/event/stream-play"],
                [play_id, "stream", streamID],
              ]);                
            };
            video.onpause = function () {
              let paused_id = createId();
              program.inputEAVs([
                [paused_id, "tag", "html/event/stream-pause"],
                [paused_id, "stream", streamID],
              ]);    
            };
            video.onloadeddata = function () {
              let ready_id = createId();
              program.inputEAVs([
                [ready_id, "tag", "html/event/stream-ready"],
                [ready_id, "stream", streamID],
              ]);  
            }
            //window.addEventListener("pageshow", video.onplay());
            this.streams[streamID] = video; 
         }
      }
    }),
    "play": handleTuples(({adds}) => {
      for(let [streamID, play] of adds || EMPTY) {
        let video = this.streams[streamID];
        if (play === "true") {
            video.play();
        } else {
            video.pause();
        }
      }
    })
  }
}

Library.register(Stream.id, Stream);