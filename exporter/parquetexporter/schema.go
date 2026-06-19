package parquetexporter

import "github.com/apache/arrow-go/v18/arrow"

func strField(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
}
func jsonField(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
}
func tsField(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64}
}
func i64Field(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64}
}
func i32Field(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int32}
}
func f64Field(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64}
}
func boolField(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Boolean}
}

func f64ListField(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.ListOf(arrow.PrimitiveTypes.Float64)}
}
func i64ListField(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}
}

// exemplarsType is LIST(STRUCT(FilteredAttributes, TimeUnix, Value, SpanId, TraceId)).
func exemplarsType() arrow.DataType {
	return arrow.ListOf(arrow.StructOf(
		jsonField("FilteredAttributes"),
		tsField("TimeUnix"),
		f64Field("Value"),
		strField("SpanId"),
		strField("TraceId"),
	))
}

func tracesSchema() *arrow.Schema {
	eventsType := arrow.ListOf(arrow.StructOf(tsField("Timestamp"), strField("Name"), jsonField("Attributes")))
	linksType := arrow.ListOf(arrow.StructOf(strField("TraceId"), strField("SpanId"), strField("TraceState"), jsonField("Attributes")))
	return arrow.NewSchema([]arrow.Field{
		tsField("Timestamp"), strField("TraceId"), strField("SpanId"), strField("ParentSpanId"),
		strField("TraceState"), strField("SpanName"), strField("SpanKind"), strField("ServiceName"),
		jsonField("ResourceAttributes"), strField("ScopeName"), strField("ScopeVersion"),
		jsonField("SpanAttributes"), i64Field("Duration"), strField("StatusCode"), strField("StatusMessage"),
		{Name: "Events", Type: eventsType}, {Name: "Links", Type: linksType},
	}, nil)
}

func logsSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		tsField("Timestamp"), strField("TraceId"), strField("SpanId"), i32Field("TraceFlags"),
		strField("SeverityText"), i32Field("SeverityNumber"), strField("ServiceName"), strField("Body"),
		jsonField("ResourceAttributes"), strField("ScopeName"), strField("ScopeVersion"),
		jsonField("ScopeAttributes"), jsonField("LogAttributes"), strField("EventName"),
	}, nil)
}

func metricsCommonFields() []arrow.Field {
	return []arrow.Field{
		jsonField("ResourceAttributes"), strField("ResourceSchemaUrl"),
		strField("ScopeName"), strField("ScopeVersion"), jsonField("ScopeAttributes"), strField("ScopeSchemaUrl"),
		strField("ServiceName"), strField("MetricName"), strField("MetricDescription"), strField("MetricUnit"),
		jsonField("Attributes"), tsField("StartTimeUnix"), tsField("TimeUnix"), i32Field("Flags"),
	}
}

func metricsGaugeSchema() *arrow.Schema {
	f := metricsCommonFields()
	f = append(f, f64Field("Value"), arrow.Field{Name: "Exemplars", Type: exemplarsType()})
	return arrow.NewSchema(f, nil)
}

func metricsSumSchema() *arrow.Schema {
	f := metricsCommonFields()
	f = append(f, f64Field("Value"), i32Field("AggregationTemporality"), boolField("IsMonotonic"),
		arrow.Field{Name: "Exemplars", Type: exemplarsType()})
	return arrow.NewSchema(f, nil)
}

func metricsHistogramSchema() *arrow.Schema {
	f := metricsCommonFields()
	f = append(f, i64Field("Count"), f64Field("Sum"), i64ListField("BucketCounts"), f64ListField("ExplicitBounds"),
		f64Field("Min"), f64Field("Max"), i32Field("AggregationTemporality"),
		arrow.Field{Name: "Exemplars", Type: exemplarsType()})
	return arrow.NewSchema(f, nil)
}

func metricsExpHistogramSchema() *arrow.Schema {
	f := metricsCommonFields()
	f = append(f, i64Field("Count"), f64Field("Sum"), i32Field("Scale"), i64Field("ZeroCount"),
		i32Field("PositiveOffset"), i64ListField("PositiveBucketCounts"),
		i32Field("NegativeOffset"), i64ListField("NegativeBucketCounts"),
		f64Field("Min"), f64Field("Max"), i32Field("AggregationTemporality"),
		arrow.Field{Name: "Exemplars", Type: exemplarsType()})
	return arrow.NewSchema(f, nil)
}

func metricsSummarySchema() *arrow.Schema {
	f := metricsCommonFields()
	quantilesType := arrow.ListOf(arrow.StructOf(f64Field("Quantile"), f64Field("Value")))
	f = append(f, i64Field("Count"), f64Field("Sum"), arrow.Field{Name: "ValueAtQuantiles", Type: quantilesType})
	return arrow.NewSchema(f, nil)
}
