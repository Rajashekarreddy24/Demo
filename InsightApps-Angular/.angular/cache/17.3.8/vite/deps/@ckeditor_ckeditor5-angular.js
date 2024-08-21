import {
  FormsModule,
  NG_VALUE_ACCESSOR
} from "./chunk-RDDBBE5S.js";
import {
  CommonModule
} from "./chunk-G3I45SOJ.js";
import {
  Component,
  ElementRef,
  EventEmitter,
  Input,
  NgModule,
  NgZone,
  Output,
  forwardRef,
  setClassMetadata,
  ɵɵNgOnChangesFeature,
  ɵɵProvidersFeature,
  ɵɵdefineComponent,
  ɵɵdefineInjector,
  ɵɵdefineNgModule,
  ɵɵdirectiveInject,
  ɵɵtemplate
} from "./chunk-4CQCCVKX.js";
import "./chunk-BS627H3B.js";
import "./chunk-RNFDYRVA.js";
import {
  first
} from "./chunk-435DBNDD.js";
import "./chunk-EM7Y34HP.js";
import {
  __async,
  __spreadValues
} from "./chunk-CXFNIQQO.js";

// node_modules/@ckeditor/ckeditor5-angular/fesm2020/ckeditor-ckeditor5-angular.mjs
function CKEditorComponent_ng_template_0_Template(rf, ctx) {
}
var HEX_NUMBERS = new Array(256).fill(0).map((val, index) => ("0" + index.toString(16)).slice(-2));
function uid() {
  const r1 = Math.random() * 4294967296 >>> 0;
  const r2 = Math.random() * 4294967296 >>> 0;
  const r3 = Math.random() * 4294967296 >>> 0;
  const r4 = Math.random() * 4294967296 >>> 0;
  return "e" + HEX_NUMBERS[r1 >> 0 & 255] + HEX_NUMBERS[r1 >> 8 & 255] + HEX_NUMBERS[r1 >> 16 & 255] + HEX_NUMBERS[r1 >> 24 & 255] + HEX_NUMBERS[r2 >> 0 & 255] + HEX_NUMBERS[r2 >> 8 & 255] + HEX_NUMBERS[r2 >> 16 & 255] + HEX_NUMBERS[r2 >> 24 & 255] + HEX_NUMBERS[r3 >> 0 & 255] + HEX_NUMBERS[r3 >> 8 & 255] + HEX_NUMBERS[r3 >> 16 & 255] + HEX_NUMBERS[r3 >> 24 & 255] + HEX_NUMBERS[r4 >> 0 & 255] + HEX_NUMBERS[r4 >> 8 & 255] + HEX_NUMBERS[r4 >> 16 & 255] + HEX_NUMBERS[r4 >> 24 & 255];
}
var ANGULAR_INTEGRATION_READ_ONLY_LOCK_ID = "Lock from Angular integration (@ckeditor/ckeditor5-angular)";
var CKEditorComponent = class {
  constructor(elementRef, ngZone) {
    this.config = {};
    this.data = "";
    this.tagName = "div";
    this.disableTwoWayDataBinding = false;
    this.ready = new EventEmitter();
    this.change = new EventEmitter();
    this.blur = new EventEmitter();
    this.focus = new EventEmitter();
    this.error = new EventEmitter();
    this.initiallyDisabled = false;
    this.isEditorSettingData = false;
    this.id = uid();
    this.ngZone = ngZone;
    this.elementRef = elementRef;
    this.checkVersion();
  }
  /**
   * When set `true`, the editor becomes read-only.
   * See https://ckeditor.com/docs/ckeditor5/latest/api/module_core_editor_editor-Editor.html#member-isReadOnly
   * to learn more.
   */
  set disabled(isDisabled) {
    this.setDisabledState(isDisabled);
  }
  get disabled() {
    if (this.editorInstance) {
      return this.editorInstance.isReadOnly;
    }
    return this.initiallyDisabled;
  }
  /**
   * The instance of the editor created by this component.
   */
  get editorInstance() {
    let editorWatchdog = this.editorWatchdog;
    if (this.watchdog) {
      editorWatchdog = this.watchdog._watchdogs.get(this.id);
    }
    if (editorWatchdog) {
      return editorWatchdog.editor;
    }
    return null;
  }
  getId() {
    return this.id;
  }
  checkVersion() {
    const {
      CKEDITOR_VERSION
    } = window;
    if (!CKEDITOR_VERSION) {
      return console.warn('Cannot find the "CKEDITOR_VERSION" in the "window" scope.');
    }
    const [major] = CKEDITOR_VERSION.split(".").map(Number);
    if (major >= 42 || CKEDITOR_VERSION.startsWith("0.0.0")) {
      return;
    }
    console.warn("The <CKEditor> component requires using CKEditor 5 in version 42+ or nightly build.");
  }
  // Implementing the OnChanges interface. Whenever the `data` property is changed, update the editor content.
  ngOnChanges(changes) {
    if (Object.prototype.hasOwnProperty.call(changes, "data") && changes.data && !changes.data.isFirstChange()) {
      this.writeValue(changes.data.currentValue);
    }
  }
  // Implementing the AfterViewInit interface.
  ngAfterViewInit() {
    this.attachToWatchdog();
  }
  // Implementing the OnDestroy interface.
  ngOnDestroy() {
    return __async(this, null, function* () {
      if (this.watchdog) {
        yield this.watchdog.remove(this.id);
      } else if (this.editorWatchdog && this.editorWatchdog.editor) {
        yield this.editorWatchdog.destroy();
        this.editorWatchdog = void 0;
      }
    });
  }
  // Implementing the ControlValueAccessor interface (only when binding to ngModel).
  writeValue(value) {
    if (value === null) {
      value = "";
    }
    if (this.editorInstance) {
      this.isEditorSettingData = true;
      this.editorInstance.data.set(value);
      this.isEditorSettingData = false;
    } else {
      this.data = value;
      this.ready.pipe(first()).subscribe((editor) => {
        editor.data.set(this.data);
      });
    }
  }
  // Implementing the ControlValueAccessor interface (only when binding to ngModel).
  registerOnChange(callback) {
    this.cvaOnChange = callback;
  }
  // Implementing the ControlValueAccessor interface (only when binding to ngModel).
  registerOnTouched(callback) {
    this.cvaOnTouched = callback;
  }
  // Implementing the ControlValueAccessor interface (only when binding to ngModel).
  setDisabledState(isDisabled) {
    if (this.editorInstance) {
      if (isDisabled) {
        this.editorInstance.enableReadOnlyMode(ANGULAR_INTEGRATION_READ_ONLY_LOCK_ID);
      } else {
        this.editorInstance.disableReadOnlyMode(ANGULAR_INTEGRATION_READ_ONLY_LOCK_ID);
      }
    }
    this.initiallyDisabled = isDisabled;
  }
  /**
   * Creates the editor instance, sets initial editor data, then integrates
   * the editor with the Angular component. This method does not use the `editor.data.set()`
   * because of the issue in the collaboration mode (#6).
   */
  attachToWatchdog() {
    const creator = (elementOrData, config2) => {
      return this.ngZone.runOutsideAngular(() => __async(this, null, function* () {
        this.elementRef.nativeElement.appendChild(elementOrData);
        const editor = yield this.editor.create(elementOrData, config2);
        if (this.initiallyDisabled) {
          editor.enableReadOnlyMode(ANGULAR_INTEGRATION_READ_ONLY_LOCK_ID);
        }
        this.ngZone.run(() => {
          this.ready.emit(editor);
        });
        this.setUpEditorEvents(editor);
        return editor;
      }));
    };
    const destructor = (editor) => __async(this, null, function* () {
      yield editor.destroy();
      this.elementRef.nativeElement.removeChild(this.editorElement);
    });
    const emitError = (e) => {
      if (hasObservers(this.error)) {
        this.ngZone.run(() => this.error.emit(e));
      }
    };
    const element = document.createElement(this.tagName);
    const config = this.getConfig();
    this.editorElement = element;
    if (this.watchdog) {
      this.watchdog.add({
        id: this.id,
        type: "editor",
        creator,
        destructor,
        sourceElementOrData: element,
        config
      }).catch((e) => {
        emitError(e);
      });
      this.watchdog.on("itemError", (_, {
        itemId
      }) => {
        if (itemId === this.id) {
          emitError();
        }
      });
    } else {
      const editorWatchdog = new this.editor.EditorWatchdog(this.editor, this.editorWatchdogConfig);
      editorWatchdog.setCreator(creator);
      editorWatchdog.setDestructor(destructor);
      editorWatchdog.on("error", emitError);
      this.editorWatchdog = editorWatchdog;
      this.ngZone.runOutsideAngular(() => {
        editorWatchdog.create(element, config).catch((e) => {
          emitError(e);
        });
      });
    }
  }
  getConfig() {
    if (this.data && this.config.initialData) {
      throw new Error("Editor data should be provided either using `config.initialData` or `data` properties.");
    }
    const config = __spreadValues({}, this.config);
    const initialData = this.config.initialData || this.data;
    if (initialData) {
      config.initialData = initialData;
    }
    return config;
  }
  /**
   * Integrates the editor with the component by attaching related event listeners.
   */
  setUpEditorEvents(editor) {
    const modelDocument = editor.model.document;
    const viewDocument = editor.editing.view.document;
    modelDocument.on("change:data", (evt) => {
      this.ngZone.run(() => {
        if (this.disableTwoWayDataBinding) {
          return;
        }
        if (this.cvaOnChange && !this.isEditorSettingData) {
          const data = editor.data.get();
          this.cvaOnChange(data);
        }
        this.change.emit({
          event: evt,
          editor
        });
      });
    });
    viewDocument.on("focus", (evt) => {
      this.ngZone.run(() => {
        this.focus.emit({
          event: evt,
          editor
        });
      });
    });
    viewDocument.on("blur", (evt) => {
      this.ngZone.run(() => {
        if (this.cvaOnTouched) {
          this.cvaOnTouched();
        }
        this.blur.emit({
          event: evt,
          editor
        });
      });
    });
  }
};
CKEditorComponent.ɵfac = function CKEditorComponent_Factory(t) {
  return new (t || CKEditorComponent)(ɵɵdirectiveInject(ElementRef), ɵɵdirectiveInject(NgZone));
};
CKEditorComponent.ɵcmp = ɵɵdefineComponent({
  type: CKEditorComponent,
  selectors: [["ckeditor"]],
  inputs: {
    editor: "editor",
    config: "config",
    data: "data",
    tagName: "tagName",
    watchdog: "watchdog",
    editorWatchdogConfig: "editorWatchdogConfig",
    disableTwoWayDataBinding: "disableTwoWayDataBinding",
    disabled: "disabled"
  },
  outputs: {
    ready: "ready",
    change: "change",
    blur: "blur",
    focus: "focus",
    error: "error"
  },
  features: [ɵɵProvidersFeature([{
    provide: NG_VALUE_ACCESSOR,
    // eslint-disable-next-line @typescript-eslint/no-use-before-define
    useExisting: forwardRef(() => CKEditorComponent),
    multi: true
  }]), ɵɵNgOnChangesFeature],
  decls: 1,
  vars: 0,
  template: function CKEditorComponent_Template(rf, ctx) {
    if (rf & 1) {
      ɵɵtemplate(0, CKEditorComponent_ng_template_0_Template, 0, 0, "ng-template");
    }
  },
  encapsulation: 2
});
(() => {
  (typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(CKEditorComponent, [{
    type: Component,
    args: [{
      selector: "ckeditor",
      template: "<ng-template></ng-template>",
      // Integration with @angular/forms.
      providers: [{
        provide: NG_VALUE_ACCESSOR,
        // eslint-disable-next-line @typescript-eslint/no-use-before-define
        useExisting: forwardRef(() => CKEditorComponent),
        multi: true
      }]
    }]
  }], function() {
    return [{
      type: ElementRef
    }, {
      type: NgZone
    }];
  }, {
    editor: [{
      type: Input
    }],
    config: [{
      type: Input
    }],
    data: [{
      type: Input
    }],
    tagName: [{
      type: Input
    }],
    watchdog: [{
      type: Input
    }],
    editorWatchdogConfig: [{
      type: Input
    }],
    disableTwoWayDataBinding: [{
      type: Input
    }],
    disabled: [{
      type: Input
    }],
    ready: [{
      type: Output
    }],
    change: [{
      type: Output
    }],
    blur: [{
      type: Output
    }],
    focus: [{
      type: Output
    }],
    error: [{
      type: Output
    }]
  });
})();
function hasObservers(emitter) {
  return emitter.observed || emitter.observers.length > 0;
}
var CKEditorModule = class {
};
CKEditorModule.ɵfac = function CKEditorModule_Factory(t) {
  return new (t || CKEditorModule)();
};
CKEditorModule.ɵmod = ɵɵdefineNgModule({
  type: CKEditorModule,
  declarations: [CKEditorComponent],
  imports: [FormsModule, CommonModule],
  exports: [CKEditorComponent]
});
CKEditorModule.ɵinj = ɵɵdefineInjector({
  imports: [[FormsModule, CommonModule]]
});
(() => {
  (typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(CKEditorModule, [{
    type: NgModule,
    args: [{
      imports: [FormsModule, CommonModule],
      declarations: [CKEditorComponent],
      exports: [CKEditorComponent]
    }]
  }], null, null);
})();
export {
  CKEditorComponent,
  CKEditorModule
};
/*! Bundled license information:

@ckeditor/ckeditor5-angular/fesm2020/ckeditor-ckeditor5-angular.mjs:
  (**
   * @license Copyright (c) 2003-2024, CKSource Holding sp. z o.o. All rights reserved.
   * For licensing, see LICENSE.md.
   *)
*/
//# sourceMappingURL=@ckeditor_ckeditor5-angular.js.map
