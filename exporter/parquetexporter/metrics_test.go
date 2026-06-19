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

func TestMetricsToRecordsHistogram(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	sm := rm.ScopeMetrics().AppendEmpty()

	m := sm.Metrics().AppendEmpty()
	m.SetName("latency")
	h := m.SetEmptyHistogram()
	dp := h.DataPoints().AppendEmpty()
	dp.SetCount(10)
	dp.SetSum(100.0)
	dp.SetMin(1.0)
	dp.SetMax(50.0)
	dp.BucketCounts().FromRaw([]uint64{2, 3, 5})
	dp.ExplicitBounds().FromRaw([]float64{10.0, 20.0})

	recs := metricsToRecords(md)
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	hr, ok := recs[kindHistogram]
	require.True(t, ok, "histogram record present")
	require.Equal(t, int64(1), hr.NumRows())
	cIdx := hr.Schema().FieldIndices("Count")[0]
	assert.Equal(t, int64(10), hr.Column(cIdx).(*array.Int64).Value(0))
}

func TestMetricsToRecordsExpHistogram(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	sm := rm.ScopeMetrics().AppendEmpty()

	m := sm.Metrics().AppendEmpty()
	m.SetName("latency_exp")
	eh := m.SetEmptyExponentialHistogram()
	dp := eh.DataPoints().AppendEmpty()
	dp.SetCount(5)
	dp.SetSum(50.0)
	dp.SetScale(3)
	dp.SetZeroCount(1)
	dp.SetMin(0.5)
	dp.SetMax(25.0)
	dp.Positive().SetOffset(2)
	dp.Positive().BucketCounts().FromRaw([]uint64{1, 2})
	dp.Negative().SetOffset(0)
	dp.Negative().BucketCounts().FromRaw([]uint64{1})

	recs := metricsToRecords(md)
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	er, ok := recs[kindExpHistogram]
	require.True(t, ok, "exp histogram record present")
	require.Equal(t, int64(1), er.NumRows())
	sIdx := er.Schema().FieldIndices("Scale")[0]
	assert.Equal(t, int32(3), er.Column(sIdx).(*array.Int32).Value(0))
}

func TestMetricsToRecordsSummary(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	sm := rm.ScopeMetrics().AppendEmpty()

	m := sm.Metrics().AppendEmpty()
	m.SetName("request_duration")
	sum := m.SetEmptySummary()
	dp := sum.DataPoints().AppendEmpty()
	dp.SetCount(100)
	dp.SetSum(500.0)
	qv := dp.QuantileValues().AppendEmpty()
	qv.SetQuantile(0.99)
	qv.SetValue(12.5)

	recs := metricsToRecords(md)
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	sr, ok := recs[kindSummary]
	require.True(t, ok, "summary record present")
	require.Equal(t, int64(1), sr.NumRows())
	cIdx := sr.Schema().FieldIndices("Count")[0]
	assert.Equal(t, int64(100), sr.Column(cIdx).(*array.Int64).Value(0))
}
