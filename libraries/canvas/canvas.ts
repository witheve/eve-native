import {Program, Library, RawValue, RawEAV, RawMap, handleTuples, asValue, libraries} from "../../ts";

function ixComparator(idMap:{[key:string]:{ix:number}}) {
  return (a:string, b:string) => {
    return idMap[a].ix - idMap[b].ix;
  }
}

let operationFields:{[type:string]: string[]} = {
  moveTo: ["x", "y"],
  lineTo: ["x", "y"],
  bezierQuadraticCurveTo: ["cp1x", "cp1y", "cp2x", "cp2y", "x", "y"],
  quadraticCurveTo: ["cpx", "cpy", "x", "y"],
  arc: ["x", "y", "radius", "startAngle", "endAngle", "anticlockwise"],
  arcTo: ["x1", "y1", "x2", "y2", "radius"],
  ellipse: ["x", "y", "radiusX", "radiusY", "rotation", "startAngle", "endAngle", "anticlockwise"],
  rect: ["x", "y", "width", "height"],
  closePath: []
};

let defaultOperationFieldValue:{[field:string]: any} = {
  rotation: 0,
  startAngle: 0,
  endAngle: 2 * Math.PI,
  anticlockwise: false
};

function isOperationType(val:RawValue): val is OperationType {
  return !!operationFields[val];
}

const EMPTY_OBJ = {};
const EMPTY:never[] = [];

export interface Canvas extends HTMLCanvasElement { __element: RawValue }
export type OperationType = keyof Path2D;
export interface Operation {type: OperationType, args:any, paths:RawValue[]};
// {fillStyle: "#000000", strokeStyle: "#000000", lineWidth: 1, lineCap: "butt", lineJoin: "miter"}
export interface PathStyle {[key:string]: RawValue|undefined, fillStyle?:string, strokeStyle?:string, lineWidth?:number, lineCap?:string, lineJoin?: string };

export class Canvas extends Library {
  static id = "canvas";

  //////////////////////////////////////////////////////////////////////
  // Implementation
  //////////////////////////////////////////////////////////////////////

  html:libraries.HTML;
  canvases:RawMap<RawValue[]|undefined> = {};
  paths:RawMap<RawValue[]|undefined> = {};
  operations:RawMap<Operation|undefined> = {};
  canvasPaths:RawMap<RawValue[]|undefined> = {};
  pathToCanvases:RawMap<RawValue[]|undefined> = {};
  pathStyles:RawMap<PathStyle|undefined> = {};
  pathCache:RawMap<Path2D|undefined> = {};
  dirty:RawMap<boolean|undefined> = {};

  setup() {
    this.html = this.program.attach("html") as libraries.HTML;
  }

  addCanvasInstance(canvasId:RawValue, instanceId:RawValue) {
    let instances = this.canvases[canvasId] = this.canvases[canvasId] || [];
    instances.push(instanceId);
  }
  clearCanvasInstance(canvasId:RawValue, instanceId:RawValue) {
    let instances = this.canvases[canvasId];
    if(!instances) return; // @FIXME: Seems like an error though
    let ix = instances.indexOf(instanceId);
    if(ix !== -1) {
      instances.splice(ix, 1);
      if(!instances.length) this.canvases[canvasId] = undefined;
    }
  }
  getCanvasInstances(canvasId:RawValue) {
    let instances = this.canvases[canvasId];
    if(!instances) throw new Error(`Missing canvas instance(s) for ${canvasId}`);
    return instances;
  }
  getCanvasPaths(canvasId:RawValue) {
    return this.canvasPaths[canvasId];
  }

  addPath(id:RawValue) {
    if(this.paths[id]) throw new Error(`Recreating path instance ${id}`);
    this.pathStyles[id] = {};
    this.dirty[id] = true;
    return this.paths[id] = [];
  }
  clearPath(id:RawValue) {
    if(!this.paths[id]) throw new Error(`Missing path instance ${id}`);
    this.pathStyles[id] = undefined;
    this.paths[id] = undefined;
    this.dirty[id] = true;
  }
  getPath(id:RawValue) {
    let path = this.paths[id];
    if(!path) throw new Error(`Missing path instance ${id}`);
    return path;
  }

  addOperation(id:RawValue, type:RawValue) {
    if(this.operations[id]) throw new Error(`Recreating operation instance ${id}`);
    if(!isOperationType(type)) throw new Error(`Invalid operation type ${type}`);
    return this.operations[id] = {type, args: {}, paths: []};
  }
  clearOperation(id:RawValue) {
    if(!this.operations[id]) { throw new Error(`Missing operation instance ${id}`); }
    this.operations[id] = undefined;
  }
  getOperation(id:RawValue) {
    let operation = this.operations[id];
    if(!operation) throw new Error(`Missing operation instance ${id}`);
    return operation;
  }

  getOperationArgs(operation:Operation) {
    let {type, args} = operation;
    let fields:string[] = operationFields[type as string];

    let input = [];
    let restOptional = false;
    for(let field of fields) {
      let value = asValue(args[field]);
      if(value === undefined) value = defaultOperationFieldValue[field];
      if(value === undefined) return;
      input.push(value);
    }
    return input;
  }

  updateCache(dirtyPaths:RawValue[]) {
    for(let id of dirtyPaths) {
      if(!this.dirty[id]) continue;
      let path = this.paths[id];
      if(!path) continue;
      let path2d = this.pathCache[id] = new window.Path2D();
      for(let opId of path) {
        if(opId === undefined) continue;
        let operation = this.getOperation(opId);
        let input = this.getOperationArgs(operation);
        if(input === undefined) {
          console.warn(`Skipping incomplete or invalid operation ${opId}`, operation.type, operation.args);
          continue;
        }
        if(!path2d[operation.type]) {
          console.warn(`Skipping unavailable operation type ${operation.type}. Check your browser's Path2D compatibility.`);
          continue;
        }
        (path2d[operation.type] as (...args:any[]) => void)(...input);
      }
    }
  }

  rerender(dirtyPaths:RawValue[]) {
    let dirtyCanvases:RawMap<boolean|undefined> = {};
    for(let id of dirtyPaths) {
      let canvasIds = this.pathToCanvases[id];
      if(!canvasIds) continue;
      for(let canvasId of canvasIds) {
        dirtyCanvases[canvasId] = true;
      }
    }

    for(let canvasId of Object.keys(dirtyCanvases)) {
      let pathIds = this.canvasPaths[canvasId];
      for(let instanceId of this.getCanvasInstances(canvasId)) {
        let canvas = this.html.getInstance(instanceId) as Canvas;
        let ctx = canvas.getContext("2d")!;
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        if(!pathIds) continue;

        for(let id of pathIds) {
          let cached = this.pathCache[id];
          if(!cached) continue // This thing isn't a path (yet?)

          let style = this.pathStyles[id] || EMPTY_OBJ as PathStyle;
          let {fillStyle = "#000000", strokeStyle = "#000000", lineWidth = 1, lineCap = "butt", lineJoin = "miter"} = style;
          ctx.fillStyle = fillStyle;
          ctx.strokeStyle = strokeStyle;
          ctx.lineWidth = lineWidth;
          ctx.lineCap = lineCap;
          ctx.lineJoin = lineJoin;
          if(style.strokeStyle) ctx.stroke(cached);
          if(style.fillStyle || !style.strokeStyle) ctx.fill(cached);

        }
      }
    }
  }

  _isChanging = false;
  changed = () => {
    let dirtyPaths = Object.keys(this.dirty);
    this.updateCache(dirtyPaths);
    this.rerender(dirtyPaths);
    this.dirty = {};
    this._isChanging = false;
  }

  changing() {
    if(!this._isChanging) {
      this._isChanging = true;
      setImmediate(this.changed);
    }
  }

  //////////////////////////////////////////////////////////////////////
  // Handlers
  //////////////////////////////////////////////////////////////////////

  handlers = {
    "export instances": handleTuples(({adds, removes}) => {
      for(let [canvasId, instanceId] of removes || EMPTY) this.clearCanvasInstance(canvasId, instanceId);
      for(let [canvasId, instanceId] of adds || EMPTY) this.addCanvasInstance(canvasId, instanceId);
      this.changing();
    }),
    "export paths": handleTuples(({adds, removes}) => {
      console.log("EP", {removes, adds}, this);
      for(let [pathId] of removes || EMPTY) this.clearPath(pathId);
      for(let [pathId] of adds || EMPTY) this.addPath(pathId);
    }),
    "export operations": handleTuples(({adds, removes}) => {
      console.log("EO", {removes, adds}, this);
      for(let [operationId] of removes || EMPTY) this.clearOperation(operationId);
      for(let [operationId, kind] of adds || EMPTY) this.addOperation(operationId, kind);
    }),
    "export canvas paths": handleTuples(({adds, removes}) => {
      console.log("ECP", {removes, adds}, this);
      for(let [canvasId, pathId, ix] of removes || EMPTY) {
        if(typeof ix !== "number") continue;
        let instances = this.canvases[canvasId];
        let paths = this.canvasPaths[canvasId];
        if(!paths || !instances) continue;
        paths[ix - 1] = undefined as any;
        let canvases = this.pathToCanvases[pathId]!;
        canvases.splice(canvases.indexOf(canvasId), 1);

        // @FIXME: need a proper way to indicate dirtyness when an unchanged path is added a canvas.
        // This hack just marks the path dirty, which will rerender any other canvases containing it o_o
        this.dirty[pathId] = true;
      }
      for(let [canvasId, pathId, ix] of adds || EMPTY) {
        if(typeof ix !== "number") continue;
        let instances = this.canvases[canvasId];
        let paths = this.canvasPaths[canvasId] = this.canvasPaths[canvasId] || [];
        paths[ix - 1] = pathId;
        let canvases = this.pathToCanvases[pathId] = this.pathToCanvases[pathId] || [];
        canvases.push(canvasId);

        // @FIXME: need a proper way to indicate dirtyness when an unchanged path is added a canvas.
        // This hack just marks the path dirty, which will rerender any other canvases containing it o_o
        this.dirty[pathId] = true;
      }

      this.changing();
    }),

    "export path operations": handleTuples(({adds, removes}) => {
      console.log("EPO", {removes, adds}, this);
      for(let [pathId, operationId, ix] of removes || EMPTY) {
        if(typeof ix !== "number") continue;
        let path = this.paths[pathId];
        let operation = this.operations[operationId];
        if(path) path[ix - 1] = undefined as any;
        if(operation) operation.paths.splice(operation.paths.indexOf(pathId), 1);

        this.dirty[pathId] = true;
      }
      for(let [pathId, operationId, ix] of adds || EMPTY) {
        if(typeof ix !== "number") continue;
        let path = this.getPath(pathId);
        let operation = this.getOperation(operationId);
        path[ix - 1] = operationId;
        operation.paths.push(pathId);

        this.dirty[pathId] = true;
      }

      this.changing();
    }),

    "export operation attributes": handleTuples(({adds, removes}) => {
      console.log("EOA", {removes, adds}, this);
      for(let [operationId, attribute, value] of removes || EMPTY) {
        let operation = this.operations[operationId];
        if(!operation) continue;
        operation.args[attribute] = undefined;
        for(let pathId of operation.paths) this.dirty[pathId] = true;
      }
      for(let [operationId, attribute, value] of adds || EMPTY) {
        let operation = this.operations[operationId];
        if(!operation) throw new Error(`Missing operation ${operationId} for AV ${attribute}: ${value}`);
        if(operation.args[attribute]) throw new Error(`Attempting to overwrite existing attribute ${attribute} of ${operationId}: ${operation.args[attribute]} => ${value}`);
        operation.args[attribute] = value;
        for(let pathId of operation.paths) this.dirty[pathId] = true;
      }

      this.changing();
    }),

    "export path styles": handleTuples(({adds, removes}) => {
      for(let [pathId, attribute, value] of removes || EMPTY) {
        let pathStyle = this.pathStyles[pathId];
        if(!pathStyle) continue;
        pathStyle[attribute] = undefined;
        this.dirty[pathId] = true;
      }
      for(let [pathId, attribute, value] of adds || EMPTY) {
        let pathStyle = this.pathStyles[pathId];
        if(!pathStyle) throw new Error(`Missing path style for ${pathId}.`);
        pathStyle[attribute] = value;
        this.dirty[pathId] = true;
      }

      this.changing();
    })
  }
}

Library.register(Canvas.id, Canvas);
