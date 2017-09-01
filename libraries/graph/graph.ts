import {Library, RawValue, RawEAV, handleTuples, libraries} from "../../ts";
import Vis from "vis";

const EMPTY:never[] = [];

export class EveGraph extends Library {
  static id = "graph";
  graphs: any = {};
  html:libraries.HTML;

  setup() {
    this.html = this.program.attach("html") as libraries.HTML;
  }

  ensureGraph(graph:string): any {
    let found = this.graphs[graph]
    if(!found) {
      found = this.graphs[graph] = {id: graph, nodes: new Vis.DataSet([]), edges: new Vis.DataSet([])};

      let element: any = this.html.getInstances(graph)![0];
      var data = {
        nodes: found.nodes,
        edges: found.edges
      };
      var options = {layout: {hierarchical: {direction:"LR", sortMethod:"directed"}}};
      var network = new Vis.Network(element, data, options);
      found.network = network;
    }
    return found;
  }

  handlers = {
    "node": handleTuples(({adds, removes}) => {
      console.log("Node!", adds, removes);
      let graphs:any = {};
      for(let add of adds || EMPTY) {
        let [graphId, node, label] = add;
        graphs[graphId] = true;
        let graph = this.ensureGraph(graphId as string)
        let nodeObj = {id:node, label};
        graph.nodes.add(nodeObj);
      }
      for(let remove of removes || EMPTY) {
        let [graphId, node, label] = remove;
        graphs[graphId] = true;
        let graph = this.ensureGraph(graphId as string)
        graph.nodes.remove(node);
      }
      for(let graphId in graphs) {
        let graph = this.ensureGraph(graphId);
        graph.network.fit();
      }
    }),
    "edge": handleTuples(({adds, removes}) => {
      console.log("edge!", adds, removes);
      let graphs:any = {};
      for(let add of adds || EMPTY) {
        let [graphId, edge, from, to] = add;
        graphs[graphId] = true;
        let graph = this.ensureGraph(graphId as string)
        graph.edges.add({id:edge, from, to});
      }
      for(let remove of removes || EMPTY) {
        let [graphId, edge, from, to] = remove;
        graphs[graphId] = true;
        let graph = this.ensureGraph(graphId as string)
        graph.edges.remove(edge);
      }
      for(let graphId in graphs) {
        let graph = this.ensureGraph(graphId);
        graph.network.fit();
      }
    }),
  }
}

Library.register(EveGraph.id, EveGraph);

