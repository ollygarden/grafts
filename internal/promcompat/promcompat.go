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
	"fmt"
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

// Source says how a Prometheus-shaped series is produced.
type Source int

const (
	// SourceDerived means the series is a pure function of the OTel output, so
	// Append reconstructs it with no help from the scraper.
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
	// Source decides whether Append can build the series itself.
	Source Source
	// Disposition records how the upstream metric was treated, for the parity
	// report.
	Disposition string
	// Labels maps an OTel attribute key to the Prometheus label it becomes. An
	// attribute absent from this map is not carried into the compat series.
	Labels map[string]string
	// Dropped names upstream labels the OTel shape deliberately does not carry.
	// A non-empty Dropped is why an entry is SourceNative.
	Dropped []string
	// Series is empty when the entry is not emitted into the compat scope.
	Series []Series
}

// Append writes the Prometheus-compatible view of md into a new scope on every
// resource, leaving the OTel scopes untouched.
//
// SourceNative entries are skipped: their series cannot be rebuilt from output
// that no longer carries the labels they need, so the scraper writes them into
// the compat scope directly and Append must not duplicate them.
//
// scopeName is the component's own scope; the compat scope is that plus
// ScopeSuffix.
func Append(md pmetric.Metrics, table []Entry, scopeName, scopeVersion string) error {
	index := make(map[string]Entry, len(table))
	for _, entry := range table {
		if _, dup := index[entry.Metric]; dup {
			return fmt.Errorf("promcompat: %s appears twice in the compat table", entry.Metric)
		}
		index[entry.Metric] = entry
	}

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)

		compat := pmetric.NewScopeMetrics()
		compat.Scope().SetName(scopeName + ScopeSuffix)
		compat.Scope().SetVersion(scopeVersion)
		appendTargetInfo(rm.Resource(), compat.Metrics())

		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			if strings.HasSuffix(sm.Scope().Name(), ScopeSuffix) {
				continue
			}
			for k := 0; k < sm.Metrics().Len(); k++ {
				metric := sm.Metrics().At(k)
				entry, ok := index[metric.Name()]
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
	return nil
}

// Scope returns the compat scope's metrics on rm, creating the scope if it is
// not there yet.
//
// A scraper needs this for SourceNative entries: their series depend on labels
// the OTel shape deliberately drops, so only the scraper can build them, and
// they still have to land in the same scope as everything else for one filter
// processor to remove the whole compat set.
func Scope(rm pmetric.ResourceMetrics, scopeName, scopeVersion string) pmetric.MetricSlice {
	name := scopeName + ScopeSuffix
	sms := rm.ScopeMetrics()
	for i := 0; i < sms.Len(); i++ {
		if sms.At(i).Scope().Name() == name {
			return sms.At(i).Metrics()
		}
	}
	sm := sms.AppendEmpty()
	sm.Scope().SetName(name)
	sm.Scope().SetVersion(scopeVersion)
	return sm.Metrics()
}

// AppendNative adds one scraper-built series to a compat metric slice.
func AppendNative(dst pmetric.MetricSlice, name, promType string, value int64, labels map[string]string) {
	metric := dst.AppendEmpty()
	metric.SetName(name)

	var points pmetric.NumberDataPointSlice
	if promType == "counter" {
		sum := metric.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		points = sum.DataPoints()
	} else {
		points = metric.SetEmptyGauge().DataPoints()
	}

	dp := points.AppendEmpty()
	dp.SetIntValue(value)
	for label, v := range labels {
		dp.Attributes().PutStr(NormalizeLabel(label), v)
	}
}

// appendEntry renders one OTel metric as its upstream series. Every series the
// entry declares is emitted, even with no matching datapoints: an upstream
// exporter reports a pool state of zero rather than omitting the series, and a
// disappearing series reads as a scrape failure to an alert.
func appendEntry(src pmetric.Metric, entry Entry, dst pmetric.MetricSlice) {
	points := datapoints(src)

	for _, series := range entry.Series {
		out := dst.AppendEmpty()
		out.SetName(series.Name)
		out.SetDescription(src.Description())
		out.SetUnit(src.Unit())

		var target pmetric.NumberDataPointSlice
		if entry.Type == "counter" {
			sum := out.SetEmptySum()
			sum.SetIsMonotonic(true)
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			target = sum.DataPoints()
		} else {
			target = out.SetEmptyGauge().DataPoints()
		}

		for l := 0; l < points.Len(); l++ {
			dp := points.At(l)
			if !selects(dp.Attributes(), series.When) {
				continue
			}
			outDP := target.AppendEmpty()
			dp.CopyTo(outDP)
			relabel(outDP.Attributes(), entry.Labels)
		}
	}
}

// datapoints returns a metric's numeric datapoints regardless of which shape
// carries them. A compat entry is only ever a counter or a gauge, because the
// upstream exporters this replaces expose nothing else.
func datapoints(metric pmetric.Metric) pmetric.NumberDataPointSlice {
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

// relabel replaces a datapoint's OTel attributes with the Prometheus labels the
// entry maps them to. An attribute with no mapping is dropped: it either
// selected the series, in which case the name already carries it, or the OTel
// shape added it and the upstream series never had it.
func relabel(attrs pcommon.Map, labels map[string]string) {
	renamed := pcommon.NewMap()
	for key, label := range labels {
		if value, ok := attrs.Get(key); ok {
			renamed.PutStr(NormalizeLabel(label), value.AsString())
		}
	}
	renamed.CopyTo(attrs)
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

// NormalizeLabel makes an attribute key a legal Prometheus label name.
//
// Prometheus label names match [a-zA-Z_][a-zA-Z0-9_]*, so the dots in
// `db.namespace` have to go. A leading digit is prefixed rather than dropped,
// because dropping it would collide two distinct keys onto one label.
func NormalizeLabel(key string) string {
	if key == "" {
		return "_"
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
