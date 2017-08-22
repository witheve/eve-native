import {Library, RawValue, RawEAV, handleTuples, libraries} from "../../ts";
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
            hls.loadSource(`${source}`);
            hls.attachMedia(video);
            hls.on(Hls.Events.MANIFEST_PARSED,function() {
                console.log("Stream Ready");
            });
            video.onplay = function () {
                console.log("PLAYING");
            };
            video.onpause = function () {
                console.log("Paused");
            };
            this.program.inputEAVs([[streamID, "tag", "ready"]]);
            window.addEventListener("pageshow", video.onplay());
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