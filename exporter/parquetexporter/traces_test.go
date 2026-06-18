package parquetexporter

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestTracesToRecord(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	span.SetName("GET /cart")
	span.Attributes().PutStr("http.method", "GET")
	ev := span.Events().AppendEmpty()
	ev.SetName("exception")

	rec := tracesToRecord(td)
	defer rec.Release()

	require.Equal(t, int64(1), rec.NumRows())
	svcIdx := rec.Schema().FieldIndices("ServiceName")[0]
	assert.Equal(t, "checkout", rec.Column(svcIdx).(*array.String).Value(0))
	nameIdx := rec.Schema().FieldIndices("SpanName")[0]
	assert.Equal(t, "GET /cart", rec.Column(nameIdx).(*array.String).Value(0))
}

func TestTracesToRecordEmpty(t *testing.T) {
	rec := tracesToRecord(ptrace.NewTraces())
	defer rec.Release()
	assert.Equal(t, int64(0), rec.NumRows())
}

func TestTracesToRecordDurationUnsetEnd(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetStartTimestamp(pcommon.Timestamp(1000))
	// EndTimestamp intentionally left unset (0) — must not underflow

	rec := tracesToRecord(td)
	defer rec.Release()

	require.Equal(t, int64(1), rec.NumRows())
	durIdx := rec.Schema().FieldIndices("Duration")[0]
	assert.Equal(t, int64(0), rec.Column(durIdx).(*array.Int64).Value(0),
		"Duration should be 0 when EndTimestamp is unset")
}

func TestTracesToRecordDurationHappyPath(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetStartTimestamp(pcommon.Timestamp(1000))
	span.SetEndTimestamp(pcommon.Timestamp(5000))

	rec := tracesToRecord(td)
	defer rec.Release()

	require.Equal(t, int64(1), rec.NumRows())
	durIdx := rec.Schema().FieldIndices("Duration")[0]
	assert.Equal(t, int64(4000), rec.Column(durIdx).(*array.Int64).Value(0),
		"Duration should equal end - start")
}
