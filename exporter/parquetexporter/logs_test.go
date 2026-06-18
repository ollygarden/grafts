package parquetexporter

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestLogsToRecord(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "api")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("boom")
	lr.SetSeverityText("ERROR")

	rec := logsToRecord(ld)
	defer rec.Release()
	require.Equal(t, int64(1), rec.NumRows())
	bodyIdx := rec.Schema().FieldIndices("Body")[0]
	assert.Equal(t, "boom", rec.Column(bodyIdx).(*array.String).Value(0))
}

func TestLogsToRecordEmpty(t *testing.T) {
	rec := logsToRecord(plog.NewLogs())
	defer rec.Release()
	assert.Equal(t, int64(0), rec.NumRows())
}
