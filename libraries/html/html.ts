import md5 from "md5";
import "setimmediate";
import {Program, Library, createId, RawValue, RawEAV, RawMap, handleTuples} from "../../ts";

const EMPTY:never[] = [];

export interface Instance extends HTMLElement {
  __element: RawValue,
  __styles?: RawValue[],
  __sort?:RawValue,
  __autoSort?:RawValue,
  listeners?: {[event:string]: boolean}
}

export interface Style extends RawMap<RawValue> {__count: number}
export interface StyleElement extends HTMLStyleElement {__style: RawValue}

////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////
function isFocusable(x:any) {
  return x instanceof HTMLInputElement ||
    x instanceof HTMLTextAreaElement;
}

let naturalComparator = new Intl.Collator("en", {numeric: true}).compare;

export class HTML extends Library {
  static id = "html";

  //////////////////////////////////////////////////////////////////////
  // Public API
  //////////////////////////////////////////////////////////////////////

  addExternalRoot(tag:string, element:HTMLElement) {
    let elemId = createId();
    let eavs:RawEAV[] = [
      [elemId, "tag", tag],
      [elemId, "tag", "html/root/external"]
    ];

    this._instances[elemId] = this.decorate(element, elemId);
    this._sendEvent(eavs);
  }

  getInstances(elemId:RawValue) {
    let instanceIds = this._elementToInstances[elemId];
    if(!instanceIds) return;
    return instanceIds.map((id) => this._instances[id]);
  }

  // @DEPRECATED
  getInstance(instanceId:RawValue) {
    return this._instances[instanceId];
  }

  isInstance(elem?:any): elem is Instance {
    if(!elem || !(elem instanceof Element)) return false;
    let instance = elem as Instance;
    return instance && !!instance["__element"];
  }

  //////////////////////////////////////////////////////////////////////
  // Implementation
  //////////////////////////////////////////////////////////////////////

  /** Topmost element containing root elements. */
  _container:HTMLElement;
  /** Instances are the physical DOM elements representing Eve elements. */
  _instances:RawMap<Instance> = {};
  /** Eve elements map to one or more instances. */
  _elementToInstances:RawMap<RawValue[]> = {};
  /** Eve style records represent a set of CSS properties for a class (the style id). */
  _styles:RawMap<Style> = {};
  /** Synthetic style container. */
  _syntheticStyleContainer:HTMLElement;
  /** One style element per synthetic style. */
  _syntheticStyles:RawMap<StyleElement> = {};
  /** Dummy used for converting style properties to CSS strings. */
  _dummy:HTMLElement;

  setup() {
    if(typeof document === "undefined") {
      this.handlers = {} as any;
      return;
    }

    this._container = document.body;
    this._syntheticStyleContainer = document.createElement("div");
    this._syntheticStyleContainer.style.display = "none"
    this._syntheticStyleContainer.style.visibility = "hidden";
    this._container.appendChild(this._syntheticStyleContainer);
    this._dummy = document.createElement("div");

    window.addEventListener("click", this._mouseEventHandler("click"));
    window.addEventListener("dblclick", this._mouseEventHandler("double-click"));
    window.addEventListener("mousedown", this._mouseEventHandler("mouse-down"));
    window.addEventListener("mouseup", this._mouseEventHandler("mouse-up"));
    window.addEventListener("contextmenu", this._captureContextMenuHandler());

    window.addEventListener("input", this._inputEventHandler("change"));
    window.addEventListener("keydown", this._keyEventHandler("key-down"));
    window.addEventListener("keyup", this._keyEventHandler("key-up"));
    window.addEventListener("focus", this._focusEventHandler("focus"), true);
    window.addEventListener("blur", this._focusEventHandler("blur"), true);

    document.body.addEventListener("mouseenter", this._hoverEventHandler("hover-in"), true);
    document.body.addEventListener("mouseleave", this._hoverEventHandler("hover-out"), true);

    // window.addEventListener("hashchange", this._hashChangeHandler("url-change"));
  }

  protected decorate(elem:Element, elemId:RawValue): Instance {
    let e = elem as Instance;
    e.__element = elemId;
    return e;
  }
  protected decorateStyle(styleElem:HTMLStyleElement, styleId:RawValue): StyleElement {
    let s = styleElem as StyleElement;
    s.__style = styleId;
    return s;
  }
  protected _sendEvent(eavs:RawEAV[]) {
    this.program.inputEAVs(eavs);
  }

  protected addInstance(id:RawValue, elemId:RawValue, tagname:RawValue, ns?:RawValue) {
    let instance = this._instances[id];
    if(instance) throw new Error(`Recreating existing instance '${id}'`);
    if(ns) instance = this.decorate(document.createElementNS(""+ns, ""+tagname), elemId);
    else instance = this.decorate(document.createElement(""+tagname), elemId);
    if(!this._elementToInstances[elemId]) this._elementToInstances[elemId] = [id];
    else this._elementToInstances[elemId].push(id);
    return this._instances[id] = instance;
  }

  protected removeInstance(id:RawValue) {
    let instance = this._instances[id];
    if(!instance) throw new Error(`Unable to clear nonexistent instance '${id}'`);
    let elemId = instance.__element;
    let instances = this._elementToInstances[elemId];
    if(instances.length === 1) delete this._elementToInstances[elemId];
    else instances[instances.indexOf(id)] = instances.pop()!;
    if(instance.parentElement) instance.parentElement.removeChild(instance);
    delete this._instances[id];
  }

  protected insertRoot(root:Instance) {
    this.insertSortedChild(this._container, root, root.__sort);
  }

  protected insertChild(parent:Element|null, child:Instance, at:RawValue|undefined) {
    if(!parent) return;
    if(at === undefined) {
      parent.appendChild(child);
      return;
    }

    let current;
    for(let curIx = 0; curIx < parent.childNodes.length; curIx++) {
      let cur = parent.childNodes[curIx] as Instance;
      let curSort = cur.__sort;
      if(curSort === undefined) curSort = cur.__autoSort;
      if(cur === child) continue;
      if(curSort === undefined || naturalComparator(""+curSort, ""+at) > 0) {
        current = cur;
        break;
      }
    }

    if(current) parent.insertBefore(child, current);
    else parent.appendChild(child);
  }

  protected insertSortedChild(parent:Element|null, child:Instance, sort?:RawValue) {
    child.__sort = sort;
    this.insertChild(parent, child, sort);
  }

  protected insertAutoSortedChild(parent:Element|null, child:Instance, autoSort?:RawValue) {
    child.__autoSort = autoSort;
    if(child.__sort === undefined) this.insertChild(parent, child, autoSort);
  }

    protected addStyle(id:RawValue, attribute:RawValue, value:RawValue) {
    let style = this._styles[id] || {__count: 0};
    if(style[attribute]) throw new Error(`Cannot order multiple values per style '${id}' attribute '${attribute}'.`);
    style[attribute] = value;
    style.__count += 1;
    this._styles[id] = style;

    if(style.__count === 1) {
      this._syntheticStyles[id] = this.decorateStyle(document.createElement("style"), id);
      this._syntheticStyleContainer.appendChild(this._syntheticStyles[id]);
    }
    return style;
  }

  protected removeStyle(id:RawValue, attribute:RawValue) {
    let style = this._styles[id];
    if(!style) throw new Error(`Cannot remove attribute of nonexistent style '${id}'`);
    delete style[attribute];
    if(style.__count > 1) {
      delete style[attribute];
      style.__count -= 1;
    } else {
      let styleElem = this._syntheticStyles[id];
      if(styleElem && styleElem.parentElement) styleElem.parentElement.removeChild(styleElem);
      delete this._styles[id];
      delete this._syntheticStyles[id];
    }
  }

  protected styleToClass(styleId:RawValue):string {
    return "s-" + md5(""+styleId).slice(16);
  }

  protected toCSS(style:Style):string {
    let dummy = this._dummy;
    // Clear previous values.
    let dummyStyle = dummy.style;
    for(let prop in dummyStyle) {
      if(dummyStyle.hasOwnProperty(prop)) dummyStyle.removeProperty(prop);
    }

    for(let prop in style) {
      dummyStyle.setProperty(prop, ""+style[prop]);
    }
    return dummy.getAttribute("style")!;
  }

  protected updateStyle(id:RawValue) {
    let style = this._styles[id];
    let styleElem = this._syntheticStyles[id];
    if(!style) return;
    if(!styleElem) throw new Error(`Missing style element for synthetic style '${id}'`);
    let klass = this.styleToClass(id);
    styleElem.textContent = `.${klass} {${this.toCSS(style)}}`;
  }


  //////////////////////////////////////////////////////////////////////
  // Handlers
  //////////////////////////////////////////////////////////////////////

  handlers = {
    "export instances": handleTuples(({adds, removes}) => {
      for(let [instanceId, elemId, tagname, ns] of removes || EMPTY) {
        this.removeInstance(instanceId);
      }
      for(let [instanceId, elemId, tagname, ns] of adds || EMPTY) {
        this.addInstance(instanceId, elemId, tagname, ns);
      }
    }),
    "export roots": handleTuples(({adds}) => {
      for(let [instanceId] of adds || EMPTY) {
        this.insertRoot(this._instances[instanceId]);
      }
    }),
    "export parents": handleTuples(({adds, removes}) => {
      for(let [instanceId, parentId] of removes || EMPTY) {
        let instance = this._instances[instanceId];
        let parent = this._instances[parentId];
        if(!instance || !parent || parent != instance.parentElement) continue;
        parent.removeChild(instance);
      }
      for(let [instanceId, parentId] of adds || EMPTY) {
        let instance = this._instances[instanceId];
        let parent = this._instances[parentId];
        if(!instance || !parent) {
          let msg = "";
          if(!instance && !parent) msg = "could not find either instance or parent";
          if(!instance) msg = "could not find instance";
          if(!parent) msg = "could not find parent";
          throw new Error(`Unable to reparent instance '${instanceId}' to '${parentId}', ${msg}.`);
        }
        this.insertChild(parent, instance, (instance.__sort !== undefined) ? instance.__sort : instance.__autoSort);
      }
    }),
    "export styles": handleTuples(({adds, removes}) => {
      let modified:RawMap<true> = {};
      for(let [styleId, attribute] of removes || EMPTY) {
        modified[styleId] = true;
        this.removeStyle(styleId, attribute);
      }
      for(let [styleId, attribute, value] of adds || EMPTY) {
        modified[styleId] = true;
        this.addStyle(styleId, attribute, value);
      }

      for(let styleId of Object.keys(modified)) {
        this.updateStyle(styleId);
      }
    }),
    "export attributes": handleTuples(({adds, removes}) => {
      for(let [e, a, v] of removes || EMPTY) {
        let instance = this._instances[e];

        if(!instance || a === "tagname" || a === "children" || a === "tag" || a === "ns" || a === "sort" || a === "eve-auto-index") continue;
        else if(a === "text") instance.textContent = null
        else if(a === "style") instance.classList.remove(this.styleToClass(v));
        else if(a === "class") instance.classList.remove(""+v);
        // else if(a === "value") (instance as any).value = ""; // @FIXME: This would be flicker-y if we then add something. :(
        else instance.removeAttribute(""+a);
      }
      for(let [e, a, v] of adds || EMPTY) {
        let instance = this._instances[e];
        if(!instance) throw new Error(`Unable to add attribute to nonexistent instance '${e}' '${a}' '${v}'`);

        if(a === "tagname" || a === "children" || a === "tag" || a === "ns") continue;
        else if(a === "text") instance.textContent = ""+v;
        else if(a === "style") instance.classList.add(this.styleToClass(v));
        else if(a === "class") instance.classList.add(""+v);
        else if(a === "value") (instance as any).value = ""+v;
        else if(a === "sort") this.insertSortedChild(instance.parentElement, instance, v);
        else if(a === "eve-auto-index") this.insertAutoSortedChild(instance.parentElement, instance, v);
        else instance.setAttribute(""+a, ""+v);
      }
    }),
    "export triggers": handleTuples(({adds, removes}) => {
      for(let [instanceId, trigger] of adds || EMPTY) {
        let instance = this._instances[instanceId];
        if(!instance) throw new Error(`Unable to trigger '${trigger}' on nonexistent instance '${instanceId}'.`);
        else if(trigger === "html/trigger/focus" && isFocusable(instance)) setImmediate(() => instance.focus());
        else if(trigger === "html/trigger/blur" && isFocusable(instance)) setImmediate(() => instance.blur());
      }
    }),
    "export listeners": handleTuples(({adds, removes}) => {
      for(let [instanceId, listener] of removes || EMPTY) {
        let instance = this._instances[instanceId];
        if(!instance) continue;
        if(!instance.listeners) throw new Error(`Cannot remove never-added listener '${listener}' on instance '${instanceId}'.`);
        else instance.listeners[listener] = false;
      }

      for(let [instanceId, listener] of adds || EMPTY) {
        let instance = this._instances[instanceId];
        if(!instance) throw new Error(`Unable to add listener '${listener}' on nonexistent instance '${instanceId}'.`);
        if(!instance.listeners) instance.listeners = {[listener]: true};
        else instance.listeners[listener] = true;
      }
    })
  };

  //////////////////////////////////////////////////////////////////////
  // Event Handlers
  //////////////////////////////////////////////////////////////////////

    _mouseEventHandler(tagname:string) {
    return (event:MouseEvent) => {
      let {target} = event;
      // if(!this.isInstance(target)) return;

      let eventId = createId();
      let eavs:RawEAV[] = [
        [eventId, "tag", "html/event"],
        [eventId, "tag", `html/event/${tagname}`],
        [eventId, "page-x", event.pageX],
        [eventId, "page-y", event.pageY],
        [eventId, "window-x", event.clientX],
        [eventId, "window-y", event.clientY]
      ];
      let button = event.button;

      if(button === 0) eavs.push([eventId, "button", "left"]);
      else if(button === 2) eavs.push([eventId, "button", "right"]);
      else if(button === 1) eavs.push([eventId, "button", "middle"]);
      else if(button) eavs.push([eventId, "button", button]);

      let capturesContextMenu = false;
      if(this.isInstance(target)) {
        eavs.push([eventId, "target", target.__element]);

        let current:Element|null = target;
        while(current && this.isInstance(current)) {
          eavs.push([eventId, "element", current.__element]);
          if(button === 2 && current.listeners && current.listeners["context-menu"] === true) {
            capturesContextMenu = true;
          }
          current = current.parentElement;
        }
      }

      // @NOTE: You'll get a mousedown but no mouseup for a right click if you don't capture the context menu,
      //   so we throw out the mousedown entirely in that case. :(
      if(button === 2 && !capturesContextMenu) return;
      if(eavs.length) this._sendEvent(eavs);
    };
  }

    _captureContextMenuHandler() {
    return (event:MouseEvent) => {
      let captureContextMenu = false;
      let current:Element|null = event.target as Element;
      while(current && this.isInstance(current)) {
        if(current.listeners && current.listeners["context-menu"] === true) {
          captureContextMenu = true;
        }
        current = current.parentElement;
      }
      if(captureContextMenu && event.button === 2) {
        event.preventDefault();
      }
    };
  }

  _inputEventHandler(tagname:string) {
    return (event:Event) => {
      let target = event.target as (Instance & HTMLInputElement);
      let elementId = target.__element;
      if(elementId) {
        if(target.classList.contains("html-autosize-input")) {
          target.size = target.value.length || 1;
        }
        let eventId = createId();
        let eavs:RawEAV[] = [
          [eventId, "tag", "html/event"],
          [eventId, "tag", `html/event/${tagname}`],
          [eventId, "element", elementId],
          [eventId, "value", target.value]
        ];
        if(eavs.length) this._sendEvent(eavs);
      }
    }
  }

  _keyMap:{[key:number]: string|undefined} = { // Overrides to provide sane names for common control codes.
    9: "tab",
    13: "enter",
    16: "shift",
    17: "control",
    18: "alt",
    27: "escape",
    37: "left",
    38: "up",
    39: "right",
    40: "down",
    91: "meta"
  }

  _keyEventHandler(tagname:string) {
    return (event:KeyboardEvent) => {
      if(event.repeat) return;
      let current:Element|null = event.target as Element;

      let code = event.keyCode;
      let key = this._keyMap[code];

      let eventId = createId();
      let eavs:RawEAV[] = [
        [eventId, "tag", "html/event"],
        [eventId, "tag", `html/event/${tagname}`],
        [eventId, "key-code", code]
      ];
      if(key) eavs.push([eventId, "key", key]);

      while(current && this.isInstance(current)) {
        let elemId = current.__element!;
        eavs.push([eventId, "element", elemId]);
        current = current.parentElement;
      };
      if(eavs.length)this._sendEvent(eavs);
    };
  }

  _focusEventHandler(tagname:string) {
    return (event:FocusEvent) => {
      let target = event.target as (Instance & HTMLInputElement);
      let elementId = target.__element;
      if(elementId) {
        let eventId = createId();
        let eavs:RawEAV[] = [
          [eventId, "tag", "html/event"],
          [eventId, "tag", `html/event/${tagname}`],
          [eventId, "element", elementId]
        ];
        if(target.value !== undefined) eavs.push([eventId, "value", target.value]);
        if(eavs.length) this._sendEvent(eavs);
      }
    }
  }

  _hoverEventHandler(tagname:string) {
    return (event:MouseEvent) => {
      let {target} = event;
      if(!this.isInstance(target)) return;

      let eavs:RawEAV[] = [];
      let elemId = target.__element!;
      if(target.listeners && target.listeners["hover"]) {
        let eventId = createId();
        eavs.push(
          [eventId, "tag", "html/event"],
          [eventId, "tag", `html/event/${tagname}`],
          [eventId, "element", elemId]
        );
      }
      if(eavs.length) this._sendEvent(eavs);
    };
  }
}

Library.register(HTML.id, HTML);
(window as any)["lib"] = Library;
