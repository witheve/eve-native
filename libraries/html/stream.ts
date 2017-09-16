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
              let id = createId();
              program.inputEAVs([
                [id, "tag", "html/event/stream-play"],
                [id, "stream", streamID],
              ]);                
            };
            video.onpause = function () {
              let id = createId();
              program.inputEAVs([
                [id, "tag", "html/event/stream-pause"],
                [id, "stream", streamID],
              ]);    
            };
            video.onloadeddata = function () {
              let id = createId();
              program.inputEAVs([
                [id, "tag", "html/event/stream-ready"],
                [id, "stream", streamID],
              ]);  
            }
            video.ontimeupdate = function () {
              let id = createId();
              program.inputEAVs([
                [id, "tag", "html/event/time-change"],
                [id, "stream", streamID],
                [id, "time", video.currentTime]
              ]);  
            }
            video.ondurationchange = function() {
              let id = createId();
              program.inputEAVs([
                [id, "tag", "html/event/duration-change"],
                [id, "stream", streamID],
                [id, "duration", video.duration]
              ]);  
            }
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