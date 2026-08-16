package pgbouncerreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"go.olly.garden/grafts/internal/promcompat"
	"go.olly.garden/grafts/receiver/pgbouncerreceiver/internal/telemetry"
)

// TestDerivedCompatIsAPureFunctionOfOTelOutput is the property the derived and
// native split rests on: a derived entry's Prometheus series can be rebuilt
// from the OTel output alone, with no help from the scraper.
//
// It is checked against every derived entry in the generated table rather than
// a chosen few, so an entry added to the registry is covered the moment it is
// generated.
func TestDerivedCompatIsAPureFunctionOfOTelOutput(t *testing.T) {
	s := newTestScraper(t, &fakeClient{rows: fixtureRows()}, ShapeOTel, ShapePrometheus)

	md, err := s.scrape(t.Context())
	require.NoError(t, err)

	otel := index(t, md, scopeName)
	compat := index(t, md, scopeName+promcompat.ScopeSuffix)

	var checked int
	for _, entry := range telemetry.CompatTable {
		if entry.Source != promcompat.SourceDerived || len(entry.Series) == 0 {
			continue
		}
		source := otel[entry.Metric]
		if len(source) == 0 {
			// The metric produced nothing this scrape, so there is nothing to
			// derive from and nothing to assert.
			continue
		}

		for _, series := range entry.Series {
			t.Run(series.Name, func(t *testing.T) {
				want := selectDataPoints(source, series.When)
				got := compat[series.Name]

				require.Len(t, got, len(want),
					"%s should carry one series per matching %s datapoint", series.Name, entry.Metric)

				for i := range want {
					assert.Equal(t, value(want[i]), value(got[i]),
						"%s must carry the value of the OTel datapoint it was derived from", series.Name)

					// Selecting attributes are encoded in the series name, so
					// they must not also appear as labels -- that is what makes
					// a merge reversible rather than merely documented.
					for key := range series.When {
						_, present := got[i].Attributes().Get(key)
						assert.False(t, present, "%s must not carry its selector %s as a label", series.Name, key)
					}

					// Every label the entry maps must be present, under the
					// upstream name rather than the OTel attribute key.
					for key, label := range entry.Labels {
						if _, ok := want[i].Attributes().Get(key); !ok {
							continue
						}
						_, ok := got[i].Attributes().Get(promcompat.NormalizeLabel(label))
						assert.True(t, ok, "%s should carry label %s", series.Name, label)
					}
				}
				checked++
			})
		}
	}

	// A table that silently stopped producing derived entries would make every
	// assertion above vacuous.
	assert.Positive(t, checked, "no derived entries were exercised")
}

// TestCompatTableCoversEveryDroppedLabelNatively guards the rule the
// prom_annotation policy enforces in the registry: an entry that drops a label
// cannot be derived, because the information the series needs is gone.
func TestCompatTableCoversEveryDroppedLabelNatively(t *testing.T) {
	for _, entry := range telemetry.CompatTable {
		if len(entry.Dropped) > 0 {
			assert.Equal(t, promcompat.SourceNative, entry.Source,
				"%s drops %v, so its compat series cannot be derived", entry.Metric, entry.Dropped)
		}
	}
}

// TestCompatScopeIsSeparable is the whole justification for compatibility mode:
// one filter processor on the scope name has to remove the entire compat set
// and leave the OTel shape untouched.
func TestCompatScopeIsSeparable(t *testing.T) {
	s := newTestScraper(t, &fakeClient{rows: fixtureRows()}, ShapeOTel, ShapePrometheus)

	md, err := s.scrape(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, md.ResourceMetrics().Len())

	sms := md.ResourceMetrics().At(0).ScopeMetrics()
	require.Equal(t, 2, sms.Len())

	var otelNames, compatNames []string
	for i := 0; i < sms.Len(); i++ {
		sm := sms.At(i)
		for j := 0; j < sm.Metrics().Len(); j++ {
			if sm.Scope().Name() == scopeName {
				otelNames = append(otelNames, sm.Metrics().At(j).Name())
			} else {
				compatNames = append(compatNames, sm.Metrics().At(j).Name())
			}
		}
	}

	// Nothing Prometheus-shaped in the OTel scope, and nothing OTel-shaped in
	// the compat scope.
	for _, name := range otelNames {
		assert.NotContains(t, name, "pgbouncer_", "compat series must not reach the OTel scope: %s", name)
	}
	for _, name := range compatNames {
		assert.NotContains(t, name, ".", "OTel names must not reach the compat scope: %s", name)
	}
	assert.Contains(t, compatNames, promcompat.TargetInfo)
}

func selectDataPoints(dps []pmetric.NumberDataPoint, when map[string]string) []pmetric.NumberDataPoint {
	if len(when) == 0 {
		return dps
	}
	var out []pmetric.NumberDataPoint
	for _, dp := range dps {
		matched := true
		for key, want := range when {
			got, ok := dp.Attributes().Get(key)
			if !ok || got.AsString() != want {
				matched = false
				break
			}
		}
		if matched {
			out = append(out, dp)
		}
	}
	return out
}

func value(dp pmetric.NumberDataPoint) float64 {
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		return float64(dp.IntValue())
	}
	return dp.DoubleValue()
}
