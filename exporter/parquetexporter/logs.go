package parquetexporter

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/plog"
)

func logsToRecord(ld plog.Logs) arrow.Record {
	schema := logsSchema()
	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()
	idx := func(name string) int { return schema.FieldIndices(name)[0] }
	str := func(name, v string) { rb.Field(idx(name)).(*array.StringBuilder).Append(v) }
	i32 := func(name string, v int32) { rb.Field(idx(name)).(*array.Int32Builder).Append(v) }
	i64 := func(name string, v int64) { rb.Field(idx(name)).(*array.Int64Builder).Append(v) }

	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		svc := serviceName(rl.Resource())
		resAttrs := attributesToJSON(rl.Resource().Attributes())
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sl := sls.At(j)
			scopeName := sl.Scope().Name()
			scopeVer := sl.Scope().Version()
			scopeAttrs := attributesToJSON(sl.Scope().Attributes())
			recs := sl.LogRecords()
			for k := 0; k < recs.Len(); k++ {
				lr := recs.At(k)
				i64("Timestamp", int64(lr.Timestamp()))
				str("TraceId", lr.TraceID().String())
				str("SpanId", lr.SpanID().String())
				i32("TraceFlags", int32(lr.Flags()))
				str("SeverityText", lr.SeverityText())
				i32("SeverityNumber", int32(lr.SeverityNumber()))
				str("ServiceName", svc)
				str("Body", lr.Body().AsString())
				str("ResourceAttributes", resAttrs)
				str("ScopeName", scopeName)
				str("ScopeVersion", scopeVer)
				str("ScopeAttributes", scopeAttrs)
				str("LogAttributes", attributesToJSON(lr.Attributes()))
				str("EventName", lr.EventName())
			}
		}
	}
	return rb.NewRecord()
}
