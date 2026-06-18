package parquetexporter

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestMetricsToRecordsGaugeAndSum(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	sm := rm.ScopeMetrics().AppendEmpty()

	g := sm.Metrics().AppendEmpty()
	g.SetName("temp")
	gp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	gp.SetDoubleValue(42.0)

	s := sm.Metrics().AppendEmpty()
	s.SetName("reqs")
	sum := s.SetEmptySum()
	sum.SetIsMonotonic(true)
	sp := sum.DataPoints().AppendEmpty()
	sp.SetDoubleValue(7)

	recs := metricsToRecords(md)
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	gr, ok := recs[kindGauge]
	require.True(t, ok, "gauge record present")
	require.Equal(t, int64(1), gr.NumRows())
	vIdx := gr.Schema().FieldIndices("Value")[0]
	assert.Equal(t, 42.0, gr.Column(vIdx).(*array.Float64).Value(0))

	sr, ok := recs[kindSum]
	require.True(t, ok, "sum record present")
	require.Equal(t, int64(1), sr.NumRows())
	mIdx := sr.Schema().FieldIndices("IsMonotonic")[0]
	assert.True(t, sr.Column(mIdx).(*array.Boolean).Value(0), "sum IsMonotonic")

	assert.NotContains(t, recs, kindHistogram, "histogram record should be absent")
}
