package parquetexporter

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type metricKind int

const (
	kindGauge metricKind = iota
	kindSum
	kindHistogram
	kindExpHistogram
	kindSummary
)

type metricMeta struct {
	resAttrs, resSchemaURL                          string
	scopeName, scopeVer, scopeAttrs, scopeSchemaURL string
	svc                                             string
	name, desc, unit                                string
}

// builderSet bundles a RecordBuilder with helpers and lazy row count.
type builderSet struct {
	schema *arrow.Schema
	rb     *array.RecordBuilder
	rows   int
}

func newBuilderSet(schema *arrow.Schema) *builderSet {
	return &builderSet{schema: schema, rb: array.NewRecordBuilder(memory.DefaultAllocator, schema)}
}

func (bs *builderSet) idx(name string) int { return bs.schema.FieldIndices(name)[0] }
func (bs *builderSet) str(name, v string)  { bs.rb.Field(bs.idx(name)).(*array.StringBuilder).Append(v) }
func (bs *builderSet) i64(name string, v int64) {
	bs.rb.Field(bs.idx(name)).(*array.Int64Builder).Append(v)
}
func (bs *builderSet) i32(name string, v int32) {
	bs.rb.Field(bs.idx(name)).(*array.Int32Builder).Append(v)
}
func (bs *builderSet) f64(name string, v float64) {
	bs.rb.Field(bs.idx(name)).(*array.Float64Builder).Append(v)
}
func (bs *builderSet) boolean(name string, v bool) {
	bs.rb.Field(bs.idx(name)).(*array.BooleanBuilder).Append(v)
}

func (bs *builderSet) f64List(name string, vals []float64) {
	lb := bs.rb.Field(bs.idx(name)).(*array.ListBuilder)
	lb.Append(true)
	vb := lb.ValueBuilder().(*array.Float64Builder)
	for _, v := range vals {
		vb.Append(v)
	}
}

func (bs *builderSet) i64ListFromUint(name string, vals []uint64) {
	lb := bs.rb.Field(bs.idx(name)).(*array.ListBuilder)
	lb.Append(true)
	vb := lb.ValueBuilder().(*array.Int64Builder)
	for _, v := range vals {
		vb.Append(int64(v))
	}
}

func (bs *builderSet) common(m metricMeta, attrs pcommon.Map, start, ts pcommon.Timestamp, flags uint32) {
	bs.str("ResourceAttributes", m.resAttrs)
	bs.str("ResourceSchemaUrl", m.resSchemaURL)
	bs.str("ScopeName", m.scopeName)
	bs.str("ScopeVersion", m.scopeVer)
	bs.str("ScopeAttributes", m.scopeAttrs)
	bs.str("ScopeSchemaUrl", m.scopeSchemaURL)
	bs.str("ServiceName", m.svc)
	bs.str("MetricName", m.name)
	bs.str("MetricDescription", m.desc)
	bs.str("MetricUnit", m.unit)
	bs.str("Attributes", attributesToJSON(attrs))
	bs.i64("StartTimeUnix", int64(start))
	bs.i64("TimeUnix", int64(ts))
	bs.i32("Flags", int32(flags))
}

// exemplars appends a LIST(STRUCT(...)) cell from the data point's exemplars.
func (bs *builderSet) exemplars(name string, exs pmetric.ExemplarSlice) {
	lb := bs.rb.Field(bs.idx(name)).(*array.ListBuilder)
	lb.Append(true)
	st := lb.ValueBuilder().(*array.StructBuilder)
	for i := 0; i < exs.Len(); i++ {
		ex := exs.At(i)
		st.Append(true)
		st.FieldBuilder(0).(*array.StringBuilder).Append(attributesToJSON(ex.FilteredAttributes()))
		st.FieldBuilder(1).(*array.Int64Builder).Append(int64(ex.Timestamp()))
		var v float64
		if ex.ValueType() == pmetric.ExemplarValueTypeInt {
			v = float64(ex.IntValue())
		} else {
			v = ex.DoubleValue()
		}
		st.FieldBuilder(2).(*array.Float64Builder).Append(v)
		st.FieldBuilder(3).(*array.StringBuilder).Append(ex.SpanID().String())
		st.FieldBuilder(4).(*array.StringBuilder).Append(ex.TraceID().String())
	}
}

func numberValue(dp pmetric.NumberDataPoint) float64 {
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		return float64(dp.IntValue())
	}
	return dp.DoubleValue()
}

func metricsToRecords(md pmetric.Metrics) map[metricKind]arrow.RecordBatch {
	sets := map[metricKind]*builderSet{
		kindGauge:        newBuilderSet(metricsGaugeSchema()),
		kindSum:          newBuilderSet(metricsSumSchema()),
		kindHistogram:    newBuilderSet(metricsHistogramSchema()),
		kindExpHistogram: newBuilderSet(metricsExpHistogramSchema()),
		kindSummary:      newBuilderSet(metricsSummarySchema()),
	}

	defer func() {
		for _, bs := range sets {
			bs.rb.Release()
		}
	}()

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		res := rm.Resource()
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			scope := sm.Scope()
			metrics := sm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				m := metrics.At(k)
				meta := metricMeta{
					resAttrs:       attributesToJSON(res.Attributes()),
					resSchemaURL:   rm.SchemaUrl(),
					scopeName:      scope.Name(),
					scopeVer:       scope.Version(),
					scopeAttrs:     attributesToJSON(scope.Attributes()),
					scopeSchemaURL: sm.SchemaUrl(),
					svc:            serviceName(res),
					name:           m.Name(),
					desc:           m.Description(),
					unit:           m.Unit(),
				}
				appendMetric(sets, m, meta)
			}
		}
	}

	out := map[metricKind]arrow.RecordBatch{}
	for kind, bs := range sets {
		if bs.rows > 0 {
			out[kind] = bs.rb.NewRecordBatch()
		}
	}
	return out
}

func appendMetric(sets map[metricKind]*builderSet, m pmetric.Metric, meta metricMeta) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		bs := sets[kindGauge]
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			bs.common(meta, dp.Attributes(), dp.StartTimestamp(), dp.Timestamp(), uint32(dp.Flags()))
			bs.f64("Value", numberValue(dp))
			bs.exemplars("Exemplars", dp.Exemplars())
			bs.rows++
		}
	case pmetric.MetricTypeSum:
		bs := sets[kindSum]
		sum := m.Sum()
		dps := sum.DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			bs.common(meta, dp.Attributes(), dp.StartTimestamp(), dp.Timestamp(), uint32(dp.Flags()))
			bs.f64("Value", numberValue(dp))
			bs.i32("AggregationTemporality", int32(sum.AggregationTemporality()))
			bs.boolean("IsMonotonic", sum.IsMonotonic())
			bs.exemplars("Exemplars", dp.Exemplars())
			bs.rows++
		}
	case pmetric.MetricTypeHistogram:
		bs := sets[kindHistogram]
		h := m.Histogram()
		dps := h.DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			bs.common(meta, dp.Attributes(), dp.StartTimestamp(), dp.Timestamp(), uint32(dp.Flags()))
			bs.i64("Count", int64(dp.Count()))
			bs.f64("Sum", dp.Sum())
			bs.i64ListFromUint("BucketCounts", dp.BucketCounts().AsRaw())
			bs.f64List("ExplicitBounds", dp.ExplicitBounds().AsRaw())
			bs.f64("Min", dp.Min())
			bs.f64("Max", dp.Max())
			bs.i32("AggregationTemporality", int32(h.AggregationTemporality()))
			bs.exemplars("Exemplars", dp.Exemplars())
			bs.rows++
		}
	case pmetric.MetricTypeExponentialHistogram:
		bs := sets[kindExpHistogram]
		eh := m.ExponentialHistogram()
		dps := eh.DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			bs.common(meta, dp.Attributes(), dp.StartTimestamp(), dp.Timestamp(), uint32(dp.Flags()))
			bs.i64("Count", int64(dp.Count()))
			bs.f64("Sum", dp.Sum())
			bs.i32("Scale", dp.Scale())
			bs.i64("ZeroCount", int64(dp.ZeroCount()))
			bs.i32("PositiveOffset", dp.Positive().Offset())
			bs.i64ListFromUint("PositiveBucketCounts", dp.Positive().BucketCounts().AsRaw())
			bs.i32("NegativeOffset", dp.Negative().Offset())
			bs.i64ListFromUint("NegativeBucketCounts", dp.Negative().BucketCounts().AsRaw())
			bs.f64("Min", dp.Min())
			bs.f64("Max", dp.Max())
			bs.i32("AggregationTemporality", int32(eh.AggregationTemporality()))
			bs.exemplars("Exemplars", dp.Exemplars())
			bs.rows++
		}
	case pmetric.MetricTypeSummary:
		bs := sets[kindSummary]
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			bs.common(meta, dp.Attributes(), dp.StartTimestamp(), dp.Timestamp(), uint32(dp.Flags()))
			bs.i64("Count", int64(dp.Count()))
			bs.f64("Sum", dp.Sum())
			lb := bs.rb.Field(bs.idx("ValueAtQuantiles")).(*array.ListBuilder)
			lb.Append(true)
			st := lb.ValueBuilder().(*array.StructBuilder)
			qs := dp.QuantileValues()
			for q := 0; q < qs.Len(); q++ {
				qv := qs.At(q)
				st.Append(true)
				st.FieldBuilder(0).(*array.Float64Builder).Append(qv.Quantile())
				st.FieldBuilder(1).(*array.Float64Builder).Append(qv.Value())
			}
			bs.rows++
		}
	}
}
