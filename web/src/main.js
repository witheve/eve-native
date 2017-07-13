var socket = new WebSocket("ws://localhost:3012");
socket.onmessage = (msg) => {
  console.log("GOT", msg);
}
socket.onopen = () => {
  send(socket, "Block", {id: "yo", code: "dude"})
}

function send(socket, type, content) {
  let thing = {};
  thing[type] = content;
  socket.send(JSON.stringify(thing))}
