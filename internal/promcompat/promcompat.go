// Package promcompat renders OTel-shaped metrics as the Prometheus series a
// component's upstream exporter used to emit, so a team can move to the OTel
// shape and cut their dashboards and alerts over on their own schedule.
//
// The compat series go into their own instrumentation scope, never alongside
// the OTel ones. That separability is the whole justification for compatibility
// mode: one `filter` processor removes the compat set once a cutover is done,
// and nothing else has to change.
//
// The mapping is not written here. It is generated from each component's Weaver
// registry into a []Entry, so the OTel shape and its compatibility view come
// from the same registry entry in the same pass and cannot drift apart.
package promcompat

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// ScopeSuffix is appended to a component's scope name to hold the compat
// series. Documented, because filtering the compat set out downstream means
// naming this scope.
const ScopeSuffix = "/promcompat"

// TargetInfo is the metric carrying resource attributes as labels, per the
// OTLP-to-Prometheus compatibility specification.
const TargetInfo = "target_info"

// Shape names an output shape a component can emit.
//
// This lives here rather than in each component because it is a program-wide
// contract: every converted receiver offers the same `emit` setting with the
// same values and the same errors, and a per-component copy would drift into
// user-visible inconsistency across one distribution.
type Shape string

const (
	// ShapeOTel is the semconv-aligned shape, and the default.
	ShapeOTel Shape = "otel"
	// ShapePrometheus additionally emits the series the upstream exporter
	// produced, in their own instrumentation scope.
	ShapePrometheus Shape = "prometheus"
)

// Emit is the `emit` configuration field, embedded by each component's Config.
type Emit []Shape

// Validate reports every problem with the emit list at once.
func (e Emit) Validate() error {
	if len(e) == 0 {
		return errors.New("emit must name at least one of otel, prometheus")
	}

	var errs []error
	seen := make(map[Shape]bool, len(e))
	for _, shape := range e {
		switch shape {
		case ShapeOTel, ShapePrometheus:
		default:
			errs = append(errs, fmt.Errorf("emit: unknown shape %q, want otel or prometheus", shape))
			continue
		}
		if seen[shape] {
			errs = append(errs, fmt.Errorf("emit: %q listed twice", shape))
		}
		seen[shape] = true
	}
	return errors.Join(errs...)
}

// Has reports whether the given shape should be produced.
func (e Emit) Has(shape Shape) bool {
	return slices.Contains(e, shape)
}

// Source says how a Prometheus-shaped series is produced.
type Source int

const (
	// SourceDerived means the series is a pure function of the OTel output, so
	// Table.Append reconstructs it with no help from the scraper.
	SourceDerived Source = iota
	// SourceNative means the OTel shape does not carry everything the series
	// needs -- a deliberately dropped unbounded label, usually -- so the
	// scraper writes it into the compat scope itself.
	SourceNative
)

// Series is one upstream Prometheus series produced from an OTel metric.
type Series struct {
	// Name is the upstream series name, verbatim. It is not derived from the
	// OTel name: the registry records what the upstream exporter actually
	// emitted, including any unit or `_total` suffix, because that is the
	// string a user's dashboards refer to.
	Name string
	// When selects the datapoints that become this series, by attribute value.
	// Its keys are not carried as labels -- the series name already encodes
	// them, which is what makes a merge reversible.
	When map[string]string
}

// Entry maps one OTel metric back to the series its upstream exporter emitted.
type Entry struct {
	// Metric is the OTel metric name.
	Metric string
	// Type is the Prometheus metric type of every series in Series.
	Type string
	// Source decides whether Table.Append can build the series itself.
	Source Source
	// Labels maps an OTel attribute key to the Prometheus label it becomes. An
	// attribute absent from this map is not carried into the compat series.
	Labels map[string]string
	// Dropped names upstream labels the OTel shape deliberately does not carry.
	// A non-empty Dropped is why an entry is SourceNative.
	Dropped []string
	// Series is empty when the entry is not emitted into the compat scope.
	Series []Series
}

// labelNames returns every Prometheus label an entry's series carry: the ones
// mapped from OTel attributes, plus the ones only the scraper can supply.
func (e Entry) labelNames() []string {
	names := make([]string, 0, len(e.Labels)+len(e.Dropped))
	for _, label := range e.Labels {
		names = append(names, label)
	}
	names = append(names, e.Dropped...)
	slices.Sort(names)
	return names
}

// Table is a component's compat mapping, indexed for repeated use.
//
// Built once at startup rather than per scrape: the table is generated and
// never changes, so validating it and indexing it on every collection interval
// is work that buys nothing, and a malformed table is a startup error rather
// than something that surfaces as a scrape failure.
type Table struct {
	scopeName    string
	scopeVersion string
	byMetric     map[string]Entry
	// bySeries lets a scraper look a native series up by the upstream name it
	// is about to write, so the name itself stays declared in the registry.
	bySeries map[string]Entry
}

// NewTable indexes a generated compat table.
func NewTable(entries []Entry, scopeName, scopeVersion string) (*Table, error) {
	t := &Table{
		scopeName:    scopeName,
		scopeVersion: scopeVersion,
		byMetric:     make(map[string]Entry, len(entries)),
		bySeries:     make(map[string]Entry, len(entries)),
	}
	for _, entry := range entries {
		if _, dup := t.byMetric[entry.Metric]; dup {
			return nil, fmt.Errorf("promcompat: %s appears twice in the compat table", entry.Metric)
		}
		t.byMetric[entry.Metric] = entry

		// Normalizing here keeps it off the per-datapoint path entirely: these
		// names come from the registry and never change.
		normalized := make(map[string]string, len(entry.Labels))
		for key, label := range entry.Labels {
			normalized[key] = NormalizeLabel(label)
		}
		entry.Labels = normalized

		for _, series := range entry.Series {
			t.bySeries[series.Name] = entry
		}
		t.byMetric[entry.Metric] = entry
	}
	return t, nil
}

// ScopeName is the instrumentation scope the compat series are written to.
func (t *Table) ScopeName() string { return t.scopeName + ScopeSuffix }

// Append writes the Prometheus-compatible view of md into a new scope on every
// resource, leaving the OTel scopes untouched.
//
// SourceNative entries are skipped: their series cannot be rebuilt from output
// that no longer carries the labels they need, so the scraper writes them with
// AppendNative and Append must not duplicate them.
func (t *Table) Append(md pmetric.Metrics) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)

		compat := pmetric.NewScopeMetrics()
		compat.Scope().SetName(t.ScopeName())
		compat.Scope().SetVersion(t.scopeVersion)
		appendTargetInfo(rm.Resource(), compat.Metrics())

		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			if strings.HasSuffix(sm.Scope().Name(), ScopeSuffix) {
				continue
			}
			for k := 0; k < sm.Metrics().Len(); k++ {
				metric := sm.Metrics().At(k)
				entry, ok := t.byMetric[metric.Name()]
				if !ok || entry.Source == SourceNative || len(entry.Series) == 0 {
					continue
				}
				appendEntry(metric, entry, compat.Metrics())
			}
		}

		if compat.Metrics().Len() > 0 {
			compat.MoveTo(rm.ScopeMetrics().AppendEmpty())
		}
	}
}

// Scope returns the compat scope's metrics on rm, creating the scope if it is
// not there yet.
//
// A scraper needs this for SourceNative entries: their series depend on labels
// the OTel shape deliberately drops, so only the scraper can build them, and
// they still have to land in the same scope as everything else for one filter
// processor to remove the whole compat set.
func (t *Table) Scope(rm pmetric.ResourceMetrics) pmetric.MetricSlice {
	name := t.ScopeName()
	sms := rm.ScopeMetrics()
	for i := 0; i < sms.Len(); i++ {
		if sms.At(i).Scope().Name() == name {
			return sms.At(i).Metrics()
		}
	}
	sm := sms.AppendEmpty()
	sm.Scope().SetName(name)
	sm.Scope().SetVersion(t.scopeVersion)
	return sm.Metrics()
}

// AppendNative adds one scraper-built series to a compat metric slice.
//
// The series name is looked up in the table rather than trusted from the
// caller, so a native series is still declared in the registry and renaming one
// stays a registry change. A name the table does not know is a programming
// error and returns one, rather than silently emitting a series no parity run
// accounts for.
func (t *Table) AppendNative(dst pmetric.MetricSlice, name string, value float64, isInt bool, labels map[string]string) error {
	entry, ok := t.bySeries[name]
	if !ok {
		return fmt.Errorf("promcompat: %s is not a series any registry entry declares", name)
	}
	if want := entry.labelNames(); !matchesLabels(labels, want) {
		return fmt.Errorf("promcompat: %s takes labels %v, got %v", name, want, keysOf(labels))
	}

	metric := dst.AppendEmpty()
	metric.SetName(name)

	dp := numberPoints(metric, entry.Type).AppendEmpty()
	if isInt {
		dp.SetIntValue(int64(value))
	} else {
		dp.SetDoubleValue(value)
	}
	for label, v := range labels {
		dp.Attributes().PutStr(NormalizeLabel(label), v)
	}
	return nil
}

// AppendUndeclared adds a series the registry cannot declare, because it has no
// OTel metric to hang off: a target's `up` and `version_info` describe the
// scrape and the process rather than a measurement.
//
// Separate from AppendNative so that "this series is not in the registry" is an
// explicit choice at the call site rather than a lookup that quietly failed.
// Each one belongs in component.yaml's compat_only list with a reason.
func AppendUndeclared(dst pmetric.MetricSlice, name, promType string, value int64, labels map[string]string) {
	metric := dst.AppendEmpty()
	metric.SetName(name)

	dp := numberPoints(metric, promType).AppendEmpty()
	dp.SetIntValue(value)
	for label, v := range labels {
		dp.Attributes().PutStr(NormalizeLabel(label), v)
	}
}

// numberPoints prepares a metric as a counter or a gauge and returns its
// datapoints. A compat entry is only ever one of the two, because the upstream
// exporters this replaces expose nothing else.
func numberPoints(metric pmetric.Metric, promType string) pmetric.NumberDataPointSlice {
	if promType == "counter" {
		sum := metric.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		return sum.DataPoints()
	}
	return metric.SetEmptyGauge().DataPoints()
}

// appendEntry renders one OTel metric as its upstream series.
//
// Every series the entry declares is emitted, even with no matching datapoints:
// an upstream exporter reports a pool state of zero rather than omitting the
// series, and a disappearing series reads as a scrape failure to an alert.
//
// One pass over the datapoints, not one per series: the entries that matter
// most here are the merges, where a per-series rescan is quadratic in the
// state count.
func appendEntry(src pmetric.Metric, entry Entry, dst pmetric.MetricSlice) {
	targets := make([]pmetric.NumberDataPointSlice, len(entry.Series))
	for i, series := range entry.Series {
		metric := dst.AppendEmpty()
		metric.SetName(series.Name)
		metric.SetDescription(src.Description())
		metric.SetUnit(src.Unit())
		targets[i] = numberPoints(metric, entry.Type)
	}

	points := DataPoints(src)
	for i := 0; i < points.Len(); i++ {
		dp := points.At(i)
		for j, series := range entry.Series {
			if !selects(dp.Attributes(), series.When) {
				continue
			}
			out := targets[j].AppendEmpty()
			out.SetStartTimestamp(dp.StartTimestamp())
			out.SetTimestamp(dp.Timestamp())
			switch dp.ValueType() {
			case pmetric.NumberDataPointValueTypeInt:
				out.SetIntValue(dp.IntValue())
			default:
				out.SetDoubleValue(dp.DoubleValue())
			}
			// Only the mapped labels are carried, written straight across
			// rather than copying the whole attribute map and overwriting it.
			for key, label := range entry.Labels {
				if value, ok := dp.Attributes().Get(key); ok {
					out.Attributes().PutStr(label, value.AsString())
				}
			}
			break
		}
	}
}

// DataPoints returns a metric's numeric datapoints regardless of which shape
// carries them.
func DataPoints(metric pmetric.Metric) pmetric.NumberDataPointSlice {
	switch metric.Type() {
	case pmetric.MetricTypeSum:
		return metric.Sum().DataPoints()
	case pmetric.MetricTypeGauge:
		return metric.Gauge().DataPoints()
	default:
		return pmetric.NewNumberDataPointSlice()
	}
}

// selects reports whether a datapoint matches every value in when.
func selects(attrs pcommon.Map, when map[string]string) bool {
	for key, want := range when {
		got, ok := attrs.Get(key)
		if !ok || got.AsString() != want {
			return false
		}
	}
	return true
}

// appendTargetInfo renders the resource as target_info, the compatibility
// specification's carrier for resource attributes.
func appendTargetInfo(resource pcommon.Resource, dst pmetric.MetricSlice) {
	if resource.Attributes().Len() == 0 {
		return
	}
	metric := dst.AppendEmpty()
	metric.SetName(TargetInfo)
	metric.SetDescription("Target metadata")
	dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	for key, value := range resource.Attributes().All() {
		dp.Attributes().PutStr(NormalizeLabel(key), value.AsString())
	}
}

func matchesLabels(got map[string]string, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	return slices.Equal(keysOf(got), want)
}

func keysOf(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// NormalizeLabel makes an attribute key a legal Prometheus label name.
//
// Prometheus label names match [a-zA-Z_][a-zA-Z0-9_]*, so the dots in
// `db.namespace` have to go. A leading digit is prefixed rather than dropped,
// because dropping it would collide two distinct keys onto one label.
func NormalizeLabel(key string) string {
	if key == "" {
		return "_"
	}
	if !needsNormalizing(key) {
		return key
	}

	var b strings.Builder
	b.Grow(len(key) + 1)
	for i, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			if i == 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// needsNormalizing avoids allocating for the common case: most label names are
// already legal, and this runs per label per datapoint.
func needsNormalizing(key string) bool {
	for i, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return true
		}
	}
	return false
}
