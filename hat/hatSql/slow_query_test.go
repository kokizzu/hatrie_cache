package hatSql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSQLSlowQueryRecorderRedactsParameters(t *testing.T) {
	recorder := NewSQLSlowQueryRecorder(2)
	result, err := ExecuteSQLQueryParameters(
		context.Background(),
		`SELECT id FROM CACHE('events')`,
		approximateAggregateSource{{"id": 1}},
		[]interface{}{"top-secret"},
		SQLQueryOptions{
			QueryID:            "slow-query",
			SlowQueryThreshold: time.Nanosecond,
			SlowQueryRecorder:  recorder,
		},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("result rows = %#v", result.Rows)
	}

	samples := recorder.Samples()
	if len(samples) != 1 {
		t.Fatalf("Samples() = %#v", samples)
	}
	sample := samples[0]
	if sample.QueryID != "slow-query" || !sample.Slow || !sample.OK || sample.OutputRows != 1 || sample.ResultBytes == 0 || len(sample.Plan) == 0 {
		t.Fatalf("sample = %#v", sample)
	}
	if len(sample.Parameters) != 1 || sample.Parameters[0] != "[redacted]" {
		t.Fatalf("sample parameters = %#v", sample.Parameters)
	}
	encoded, err := json.Marshal(sample)
	if err != nil {
		t.Fatalf("json.Marshal(sample) error = %v", err)
	}
	if strings.Contains(string(encoded), "top-secret") || strings.Contains(string(encoded), "events") {
		t.Fatalf("sample leaked protected input: %s", encoded)
	}
}

func TestSQLSlowQueryRecorderDoesNotRetainErrorText(t *testing.T) {
	recorder := NewSQLSlowQueryRecorder(1)
	recorder.record(SQLQueryEvent{Slow: true, Error: "top-secret"}, `SELECT 'top-secret'`, nil)

	encoded, err := json.Marshal(recorder.Samples())
	if err != nil {
		t.Fatalf("json.Marshal(Samples()) error = %v", err)
	}
	if strings.Contains(string(encoded), "top-secret") {
		t.Fatalf("sample leaked error text: %s", encoded)
	}
}

func TestSQLSlowQueryRecorderUsesKeyedStatementFingerprint(t *testing.T) {
	const source = `SELECT 'top-secret'`
	recorder := NewSQLSlowQueryRecorder(1)
	recorder.record(SQLQueryEvent{Slow: true}, source, nil)

	samples := recorder.Samples()
	plain := sha256.Sum256([]byte(source))
	if len(samples) != 1 || samples[0].StatementFingerprint == "" || samples[0].StatementFingerprint == hex.EncodeToString(plain[:]) {
		t.Fatalf("Samples() = %#v", samples)
	}
}
