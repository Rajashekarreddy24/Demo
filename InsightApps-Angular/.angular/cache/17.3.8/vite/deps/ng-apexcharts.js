import {
  require_apexcharts_common
} from "./chunk-FC5EG5OL.js";
import {
  ChangeDetectionStrategy,
  Component,
  EventEmitter,
  Input,
  NgModule,
  NgZone,
  Output,
  ViewChild,
  setClassMetadata,
  ɵɵNgOnChangesFeature,
  ɵɵdefineComponent,
  ɵɵdefineInjector,
  ɵɵdefineNgModule,
  ɵɵdirectiveInject,
  ɵɵelement,
  ɵɵloadQuery,
  ɵɵqueryRefresh,
  ɵɵviewQuery
} from "./chunk-4CQCCVKX.js";
import "./chunk-BS627H3B.js";
import {
  asapScheduler
} from "./chunk-RNFDYRVA.js";
import "./chunk-435DBNDD.js";
import "./chunk-EM7Y34HP.js";
import {
  __toESM
} from "./chunk-CXFNIQQO.js";

// node_modules/ng-apexcharts/fesm2022/ng-apexcharts.mjs
var import_apexcharts = __toESM(require_apexcharts_common(), 1);
var _c0 = ["chart"];
var _ChartComponent = class _ChartComponent {
  constructor(ngZone) {
    this.ngZone = ngZone;
    this.autoUpdateSeries = true;
    this.chartReady = new EventEmitter();
  }
  ngOnChanges(changes) {
    asapScheduler.schedule(() => {
      if (this.autoUpdateSeries && Object.keys(changes).filter((c) => c !== "series").length === 0) {
        this.updateSeries(this.series, true);
        return;
      }
      this.createElement();
    });
  }
  ngOnDestroy() {
    if (this.chartObj) {
      this.chartObj.destroy();
    }
  }
  createElement() {
    const options = {};
    if (this.annotations) {
      options.annotations = this.annotations;
    }
    if (this.chart) {
      options.chart = this.chart;
    }
    if (this.colors) {
      options.colors = this.colors;
    }
    if (this.dataLabels) {
      options.dataLabels = this.dataLabels;
    }
    if (this.series) {
      options.series = this.series;
    }
    if (this.stroke) {
      options.stroke = this.stroke;
    }
    if (this.labels) {
      options.labels = this.labels;
    }
    if (this.legend) {
      options.legend = this.legend;
    }
    if (this.fill) {
      options.fill = this.fill;
    }
    if (this.tooltip) {
      options.tooltip = this.tooltip;
    }
    if (this.plotOptions) {
      options.plotOptions = this.plotOptions;
    }
    if (this.responsive) {
      options.responsive = this.responsive;
    }
    if (this.markers) {
      options.markers = this.markers;
    }
    if (this.noData) {
      options.noData = this.noData;
    }
    if (this.xaxis) {
      options.xaxis = this.xaxis;
    }
    if (this.yaxis) {
      options.yaxis = this.yaxis;
    }
    if (this.forecastDataPoints) {
      options.forecastDataPoints = this.forecastDataPoints;
    }
    if (this.grid) {
      options.grid = this.grid;
    }
    if (this.states) {
      options.states = this.states;
    }
    if (this.title) {
      options.title = this.title;
    }
    if (this.subtitle) {
      options.subtitle = this.subtitle;
    }
    if (this.theme) {
      options.theme = this.theme;
    }
    if (this.chartObj) {
      this.chartObj.destroy();
    }
    this.ngZone.runOutsideAngular(() => {
      this.chartObj = new import_apexcharts.default(this.chartElement.nativeElement, options);
    });
    this.render();
    this.chartReady.emit({
      chartObj: this.chartObj
    });
  }
  render() {
    return this.ngZone.runOutsideAngular(() => this.chartObj.render());
  }
  updateOptions(options, redrawPaths, animate, updateSyncedCharts) {
    return this.ngZone.runOutsideAngular(() => this.chartObj.updateOptions(options, redrawPaths, animate, updateSyncedCharts));
  }
  updateSeries(newSeries, animate) {
    return this.ngZone.runOutsideAngular(() => this.chartObj.updateSeries(newSeries, animate));
  }
  appendSeries(newSeries, animate) {
    this.ngZone.runOutsideAngular(() => this.chartObj.appendSeries(newSeries, animate));
  }
  appendData(newData) {
    this.ngZone.runOutsideAngular(() => this.chartObj.appendData(newData));
  }
  toggleSeries(seriesName) {
    return this.ngZone.runOutsideAngular(() => this.chartObj.toggleSeries(seriesName));
  }
  showSeries(seriesName) {
    this.ngZone.runOutsideAngular(() => this.chartObj.showSeries(seriesName));
  }
  hideSeries(seriesName) {
    this.ngZone.runOutsideAngular(() => this.chartObj.hideSeries(seriesName));
  }
  resetSeries() {
    this.ngZone.runOutsideAngular(() => this.chartObj.resetSeries());
  }
  zoomX(min, max) {
    this.ngZone.runOutsideAngular(() => this.chartObj.zoomX(min, max));
  }
  toggleDataPointSelection(seriesIndex, dataPointIndex) {
    this.ngZone.runOutsideAngular(() => this.chartObj.toggleDataPointSelection(seriesIndex, dataPointIndex));
  }
  destroy() {
    this.chartObj.destroy();
  }
  setLocale(localeName) {
    this.ngZone.runOutsideAngular(() => this.chartObj.setLocale(localeName));
  }
  paper() {
    this.ngZone.runOutsideAngular(() => this.chartObj.paper());
  }
  addXaxisAnnotation(options, pushToMemory, context) {
    this.ngZone.runOutsideAngular(() => this.chartObj.addXaxisAnnotation(options, pushToMemory, context));
  }
  addYaxisAnnotation(options, pushToMemory, context) {
    this.ngZone.runOutsideAngular(() => this.chartObj.addYaxisAnnotation(options, pushToMemory, context));
  }
  addPointAnnotation(options, pushToMemory, context) {
    this.ngZone.runOutsideAngular(() => this.chartObj.addPointAnnotation(options, pushToMemory, context));
  }
  removeAnnotation(id, options) {
    this.ngZone.runOutsideAngular(() => this.chartObj.removeAnnotation(id, options));
  }
  clearAnnotations(options) {
    this.ngZone.runOutsideAngular(() => this.chartObj.clearAnnotations(options));
  }
  dataURI(options) {
    return this.chartObj.dataURI(options);
  }
};
_ChartComponent.ɵfac = function ChartComponent_Factory(t) {
  return new (t || _ChartComponent)(ɵɵdirectiveInject(NgZone));
};
_ChartComponent.ɵcmp = ɵɵdefineComponent({
  type: _ChartComponent,
  selectors: [["apx-chart"]],
  viewQuery: function ChartComponent_Query(rf, ctx) {
    if (rf & 1) {
      ɵɵviewQuery(_c0, 7);
    }
    if (rf & 2) {
      let _t;
      ɵɵqueryRefresh(_t = ɵɵloadQuery()) && (ctx.chartElement = _t.first);
    }
  },
  inputs: {
    chart: "chart",
    annotations: "annotations",
    colors: "colors",
    dataLabels: "dataLabels",
    series: "series",
    stroke: "stroke",
    labels: "labels",
    legend: "legend",
    markers: "markers",
    noData: "noData",
    fill: "fill",
    tooltip: "tooltip",
    plotOptions: "plotOptions",
    responsive: "responsive",
    xaxis: "xaxis",
    yaxis: "yaxis",
    forecastDataPoints: "forecastDataPoints",
    grid: "grid",
    states: "states",
    title: "title",
    subtitle: "subtitle",
    theme: "theme",
    autoUpdateSeries: "autoUpdateSeries"
  },
  outputs: {
    chartReady: "chartReady"
  },
  features: [ɵɵNgOnChangesFeature],
  decls: 2,
  vars: 0,
  consts: [["chart", ""]],
  template: function ChartComponent_Template(rf, ctx) {
    if (rf & 1) {
      ɵɵelement(0, "div", null, 0);
    }
  },
  encapsulation: 2,
  changeDetection: 0
});
var ChartComponent = _ChartComponent;
(() => {
  (typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(ChartComponent, [{
    type: Component,
    args: [{
      selector: "apx-chart",
      template: `<div #chart></div>`,
      changeDetection: ChangeDetectionStrategy.OnPush
    }]
  }], () => [{
    type: NgZone
  }], {
    chart: [{
      type: Input
    }],
    annotations: [{
      type: Input
    }],
    colors: [{
      type: Input
    }],
    dataLabels: [{
      type: Input
    }],
    series: [{
      type: Input
    }],
    stroke: [{
      type: Input
    }],
    labels: [{
      type: Input
    }],
    legend: [{
      type: Input
    }],
    markers: [{
      type: Input
    }],
    noData: [{
      type: Input
    }],
    fill: [{
      type: Input
    }],
    tooltip: [{
      type: Input
    }],
    plotOptions: [{
      type: Input
    }],
    responsive: [{
      type: Input
    }],
    xaxis: [{
      type: Input
    }],
    yaxis: [{
      type: Input
    }],
    forecastDataPoints: [{
      type: Input
    }],
    grid: [{
      type: Input
    }],
    states: [{
      type: Input
    }],
    title: [{
      type: Input
    }],
    subtitle: [{
      type: Input
    }],
    theme: [{
      type: Input
    }],
    autoUpdateSeries: [{
      type: Input
    }],
    chartReady: [{
      type: Output
    }],
    chartElement: [{
      type: ViewChild,
      args: ["chart", {
        static: true
      }]
    }]
  });
})();
window.ApexCharts = import_apexcharts.default;
var declerations = [ChartComponent];
var _NgApexchartsModule = class _NgApexchartsModule {
};
_NgApexchartsModule.ɵfac = function NgApexchartsModule_Factory(t) {
  return new (t || _NgApexchartsModule)();
};
_NgApexchartsModule.ɵmod = ɵɵdefineNgModule({
  type: _NgApexchartsModule,
  declarations: [ChartComponent],
  exports: [ChartComponent]
});
_NgApexchartsModule.ɵinj = ɵɵdefineInjector({});
var NgApexchartsModule = _NgApexchartsModule;
(() => {
  (typeof ngDevMode === "undefined" || ngDevMode) && setClassMetadata(NgApexchartsModule, [{
    type: NgModule,
    args: [{
      declarations: [...declerations],
      imports: [],
      exports: [...declerations]
    }]
  }], null, null);
})();
export {
  ChartComponent,
  NgApexchartsModule
};
//# sourceMappingURL=ng-apexcharts.js.map
