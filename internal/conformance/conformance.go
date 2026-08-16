// Package conformance measures how much of an upstream Prometheus exporter a
// grafts receiver reproduces.
//
// The point is that parity is a number we compute, not a claim we make: an
// integration test runs the real exporter alongside the receiver, scrapes both,
// and diffs them. A metric that is missing is either fixed or given a stated
// reason in the component's component.yaml. There is no third option.
//
// Series are compared on name, type and label-key set. Values are not compared:
// both sides are read from a live target at slightly different moments, so any
// counter would differ for reasons that say nothing about conformance.
package conformance

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"strings"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Series is one Prometheus series, reduced to what conformance compares.
type Series struct {
	Name string
	Type string
	// Labels is the sorted set of label keys, without values. Two datapoints of
	// the same metric differ in values but must agree on keys, and it is the
	// keys a dashboard or an alert is written against.
	Labels []string
}

// Key identifies a series for diffing.
func (s Series) Key() string {
	return s.Name
}

// Shape renders the comparable form, for reporting a mismatch.
func (s Series) Shape() string {
	return fmt.Sprintf("%s{%s} (%s)", s.Name, strings.Join(s.Labels, ","), s.Type)
}

// Report is the outcome of one comparison.
type Report struct {
	// Matched are series present on both sides with the same type and labels.
	Matched []Series
	// Missing are upstream series the receiver does not produce and that
	// component.yaml does not account for.
	Missing []Series
	// Extra are series the receiver adds beyond the upstream exporter. They do
	// not count against parity -- surface upstream lacks is the point.
	Extra []Series
	// ShapeMismatch are series present on both sides that disagree on type or
	// on their label keys. These are worse than missing: a dashboard keeps
	// working and silently means something else.
	ShapeMismatch []Mismatch
	// Dropped are upstream series component.yaml deliberately does not carry,
	// with the stated reason.
	Dropped []string
}

// Mismatch is one series whose shape differs between the two sides.
type Mismatch struct {
	Upstream Series
	Ours     Series
}

// Parity is the share of upstream series, excluding declared drops, that the
// receiver reproduces with the same shape.
func (r Report) Parity() float64 {
	total := len(r.Matched) + len(r.Missing) + len(r.ShapeMismatch)
	if total == 0 {
		return 0
	}
	return float64(len(r.Matched)) / float64(total)
}

// ParsePrometheus reads an exposition-format scrape.
//
// A hand-rolled parser rather than prometheus/common/expfmt: conformance needs
// only names, types and label keys, and that is a small enough surface not to
// justify pulling the Prometheus client libraries into a Collector
// distribution's dependency graph.
func ParsePrometheus(r io.Reader) (map[string]Series, error) {
	types := map[string]string{}
	series := map[string]Series{}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			if fields := strings.Fields(line); len(fields) >= 4 && fields[1] == "TYPE" {
				types[fields[2]] = fields[3]
			}
			continue
		}

		name, labels := splitSample(line)
		if name == "" {
			continue
		}
		// A histogram or summary reports its parts under suffixed names; keep
		// the base name so the type lookup finds them.
		base := name
		for _, suffix := range []string{"_bucket", "_sum", "_count"} {
			if trimmed, ok := strings.CutSuffix(name, suffix); ok {
				if _, known := types[trimmed]; known {
					base = trimmed
				}
			}
		}

		existing, seen := series[name]
		if seen && len(existing.Labels) >= len(labels) {
			continue
		}
		series[name] = Series{Name: name, Type: types[base], Labels: labels}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading the exposition output: %w", err)
	}
	return series, nil
}

// splitSample returns a sample's metric name and its sorted label keys.
func splitSample(line string) (string, []string) {
	open := strings.IndexByte(line, '{')
	if open < 0 {
		name, _, _ := strings.Cut(line, " ")
		return strings.TrimSpace(name), nil
	}
	close := strings.LastIndexByte(line, '}')
	if close < open {
		return "", nil
	}

	name := strings.TrimSpace(line[:open])
	var keys []string
	for _, pair := range splitLabels(line[open+1 : close]) {
		if key, _, ok := strings.Cut(pair, "="); ok {
			keys = append(keys, strings.TrimSpace(key))
		}
	}
	sort.Strings(keys)
	return name, keys
}

// splitLabels splits on commas that are not inside a quoted value.
func splitLabels(s string) []string {
	var out []string
	var current strings.Builder
	var inQuotes, escaped bool

	for _, r := range s {
		switch {
		case escaped:
			escaped = false
		case r == '\\' && inQuotes:
			escaped = true
		case r == '"':
			inQuotes = !inQuotes
		case r == ',' && !inQuotes:
			out = append(out, current.String())
			current.Reset()
			continue
		}
		current.WriteRune(r)
	}
	if current.Len() > 0 {
		out = append(out, current.String())
	}
	return out
}

// FromMetrics reads the receiver's Prometheus-shaped output out of the given
// instrumentation scope.
func FromMetrics(md pmetric.Metrics, scope string) map[string]Series {
	series := map[string]Series{}

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			if sm.Scope().Name() != scope {
				continue
			}
			for k := 0; k < sm.Metrics().Len(); k++ {
				metric := sm.Metrics().At(k)

				promType := "gauge"
				var points pmetric.NumberDataPointSlice
				switch metric.Type() {
				case pmetric.MetricTypeSum:
					points = metric.Sum().DataPoints()
					if metric.Sum().IsMonotonic() {
						promType = "counter"
					}
				case pmetric.MetricTypeGauge:
					points = metric.Gauge().DataPoints()
				default:
					continue
				}

				for l := 0; l < points.Len(); l++ {
					var keys []string
					for key := range points.At(l).Attributes().All() {
						keys = append(keys, key)
					}
					sort.Strings(keys)

					existing, seen := series[metric.Name()]
					if seen && len(existing.Labels) >= len(keys) {
						continue
					}
					series[metric.Name()] = Series{Name: metric.Name(), Type: promType, Labels: keys}
				}
			}
		}
	}
	return series
}

// Options scopes a comparison.
type Options struct {
	// Namespace is the metric-name prefix belonging to the monitored system.
	// Series outside it are ignored rather than counted as missing.
	//
	// A Prometheus exporter mounts the Go runtime and process collectors
	// alongside its own metrics -- `go_*`, `process_*`, `promhttp_*`, some
	// forty series describing the exporter binary. Those are telemetry about a
	// process this component replaces, not about the target, and the Collector
	// reports its own equivalents. Counting them as missing would halve every
	// parity number in this program for no reason.
	Namespace string
	// Dropped names upstream series component.yaml accounts for, with reasons.
	// They are excluded from the denominator rather than counted against us,
	// which is what makes the number honest rather than flattering.
	Dropped map[string]string
}

// Diff compares an upstream scrape against ours.
func Diff(upstream, ours map[string]Series, opts Options) Report {
	report := Report{}

	for name, up := range upstream {
		if !strings.HasPrefix(name, opts.Namespace) {
			continue
		}
		if reason, ok := opts.Dropped[name]; ok {
			report.Dropped = append(report.Dropped, fmt.Sprintf("%s: %s", name, reason))
			continue
		}
		mine, ok := ours[name]
		if !ok {
			report.Missing = append(report.Missing, up)
			continue
		}
		if up.Type != mine.Type || !sameLabels(up.Labels, mine.Labels) {
			report.ShapeMismatch = append(report.ShapeMismatch, Mismatch{Upstream: up, Ours: mine})
			continue
		}
		report.Matched = append(report.Matched, up)
	}

	for name, mine := range ours {
		if _, ok := upstream[name]; ok {
			continue
		}
		report.Extra = append(report.Extra, mine)
	}

	sortSeries(report.Matched)
	sortSeries(report.Missing)
	sortSeries(report.Extra)
	sort.Slice(report.ShapeMismatch, func(i, j int) bool {
		return report.ShapeMismatch[i].Upstream.Name < report.ShapeMismatch[j].Upstream.Name
	})
	sort.Strings(report.Dropped)
	return report
}

func sortSeries(s []Series) {
	sort.Slice(s, func(i, j int) bool { return s[i].Name < s[j].Name })
}

func sameLabels(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Markdown renders the report for committing beside the component.
func (r Report) Markdown(component, upstream string) string {
	var b strings.Builder

	fmt.Fprintf(&b, "<!-- Generated by the conformance test. DO NOT EDIT. -->\n\n")
	fmt.Fprintf(&b, "# Parity: %s vs %s\n\n", component, upstream)
	fmt.Fprintf(&b, "Measured by scraping both against the same PgBouncer, and diffing on\n")
	fmt.Fprintf(&b, "series name, type and label-key set. Values are not compared: the two\n")
	fmt.Fprintf(&b, "scrapes happen at different moments, so any counter would differ for\n")
	fmt.Fprintf(&b, "reasons that say nothing about conformance.\n\n")
	fmt.Fprintf(&b, "**Parity: %.1f%%** (%d matched of %d upstream series, excluding %d declared drops)\n\n",
		r.Parity()*100, len(r.Matched), len(r.Matched)+len(r.Missing)+len(r.ShapeMismatch), len(r.Dropped))

	section(&b, "Matched", len(r.Matched), func() {
		for _, s := range r.Matched {
			fmt.Fprintf(&b, "- `%s`\n", s.Shape())
		}
	})
	section(&b, "Missing", len(r.Missing), func() {
		fmt.Fprintf(&b, "Upstream series this receiver does not produce, and that\n")
		fmt.Fprintf(&b, "`component.yaml` does not account for. Each is either implemented or\n")
		fmt.Fprintf(&b, "given a stated reason.\n\n")
		for _, s := range r.Missing {
			fmt.Fprintf(&b, "- `%s`\n", s.Shape())
		}
	})
	section(&b, "Shape mismatch", len(r.ShapeMismatch), func() {
		fmt.Fprintf(&b, "Present on both sides but disagreeing on type or label keys. Worse\n")
		fmt.Fprintf(&b, "than missing: a dashboard keeps working and quietly means something\n")
		fmt.Fprintf(&b, "else.\n\n")
		for _, m := range r.ShapeMismatch {
			fmt.Fprintf(&b, "- upstream `%s`\n  ours     `%s`\n", m.Upstream.Shape(), m.Ours.Shape())
		}
	})
	section(&b, "Extra", len(r.Extra), func() {
		fmt.Fprintf(&b, "Surface beyond the upstream exporter. These do not count against\n")
		fmt.Fprintf(&b, "parity.\n\n")
		for _, s := range r.Extra {
			fmt.Fprintf(&b, "- `%s`\n", s.Shape())
		}
	})
	section(&b, "Dropped by decision", len(r.Dropped), func() {
		for _, d := range r.Dropped {
			fmt.Fprintf(&b, "- %s\n", d)
		}
	})

	return b.String()
}

func section(b *strings.Builder, title string, count int, body func()) {
	fmt.Fprintf(b, "## %s (%d)\n\n", title, count)
	if count == 0 {
		fmt.Fprintf(b, "None.\n\n")
		return
	}
	body()
	fmt.Fprintf(b, "\n")
}
