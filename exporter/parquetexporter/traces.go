package parquetexporter

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func serviceName(res pcommon.Resource) string {
	if v, ok := res.Attributes().Get("service.name"); ok {
		return v.AsString()
	}
	return ""
}

func tracesToRecord(td ptrace.Traces) arrow.Record {
	schema := tracesSchema()
	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		svc := serviceName(rs.Resource())
		resAttrs := attributesToJSON(rs.Resource().Attributes())
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			scopeName := ss.Scope().Name()
			scopeVer := ss.Scope().Version()
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				appendSpanRow(rb, schema, spans.At(k), svc, resAttrs, scopeName, scopeVer)
			}
		}
	}
	return rb.NewRecord()
}

// appendSpanRow appends exactly one value to every column builder in schema
// order. Every column must be appended once per row or NewRecord panics with a
// length mismatch.
func appendSpanRow(rb *array.RecordBuilder, schema *arrow.Schema, s ptrace.Span, svc, resAttrs, scopeName, scopeVer string) {
	idx := func(name string) int { return schema.FieldIndices(name)[0] }
	str := func(name, v string) { rb.Field(idx(name)).(*array.StringBuilder).Append(v) }
	i64 := func(name string, v int64) { rb.Field(idx(name)).(*array.Int64Builder).Append(v) }

	i64("Timestamp", int64(s.StartTimestamp()))
	str("TraceId", s.TraceID().String())
	str("SpanId", s.SpanID().String())
	str("ParentSpanId", s.ParentSpanID().String())
	str("TraceState", s.TraceState().AsRaw())
	str("SpanName", s.Name())
	str("SpanKind", s.Kind().String())
	str("ServiceName", svc)
	str("ResourceAttributes", resAttrs)
	str("ScopeName", scopeName)
	str("ScopeVersion", scopeVer)
	str("SpanAttributes", attributesToJSON(s.Attributes()))
	i64("Duration", int64(s.EndTimestamp()-s.StartTimestamp()))
	str("StatusCode", s.Status().Code().String())
	str("StatusMessage", s.Status().Message())

	// Events: LIST(STRUCT(Timestamp, Name, Attributes))
	eb := rb.Field(idx("Events")).(*array.ListBuilder)
	eb.Append(true)
	es := eb.ValueBuilder().(*array.StructBuilder)
	for ei := 0; ei < s.Events().Len(); ei++ {
		ev := s.Events().At(ei)
		es.Append(true)
		es.FieldBuilder(0).(*array.Int64Builder).Append(int64(ev.Timestamp()))
		es.FieldBuilder(1).(*array.StringBuilder).Append(ev.Name())
		es.FieldBuilder(2).(*array.StringBuilder).Append(attributesToJSON(ev.Attributes()))
	}

	// Links: LIST(STRUCT(TraceId, SpanId, TraceState, Attributes))
	lb := rb.Field(idx("Links")).(*array.ListBuilder)
	lb.Append(true)
	ls := lb.ValueBuilder().(*array.StructBuilder)
	for li := 0; li < s.Links().Len(); li++ {
		ln := s.Links().At(li)
		ls.Append(true)
		ls.FieldBuilder(0).(*array.StringBuilder).Append(ln.TraceID().String())
		ls.FieldBuilder(1).(*array.StringBuilder).Append(ln.SpanID().String())
		ls.FieldBuilder(2).(*array.StringBuilder).Append(ln.TraceState().AsRaw())
		ls.FieldBuilder(3).(*array.StringBuilder).Append(attributesToJSON(ln.Attributes()))
	}
}
