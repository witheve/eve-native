var socket = new WebSocket("ws://localhost:3012");
socket.onmessage = (msg) => {
  console.log("GOT", msg);
}
socket.onopen = () => {
  send(socket, "Transaction", {adds: [["yo", "tag", "foo"], ["yo", "value", 1.2]], removes: []})
}

function send(socket, type, content) {
  let thing = {};
  thing[type] = content;
  socket.send(JSON.stringify(thing))
}
