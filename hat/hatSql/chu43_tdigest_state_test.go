package hatSql

import (
	"bytes"
	"math"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestSQLTDigestAggregateStateRoundTripAndMerge(t *testing.T) {
	left := make(approximateAggregateSource, 100)
	right := make(approximateAggregateSource, 100)
	all := make(approximateAggregateSource, 0, 200)
	for index := range left {
		left[index] = SQLRow{"latency": float64(index + 1)}
		right[index] = SQLRow{"latency": float64(index + 101)}
	}
	all = append(all, left...)
	all = append(all, right...)

	leftResult, err := ExecuteSQLQuery(`SELECT APPROX_TDIGEST_PERCENTILE_STATE(latency, 100) AS state FROM CACHE('events')`, left)
	if err != nil {
		t.Fatalf("left state query error = %v", err)
	}
	rightResult, err := ExecuteSQLQuery(`SELECT APPROX_TDIGEST_PERCENTILE_STATE(latency, 100) AS state FROM CACHE('events')`, right)
	if err != nil {
		t.Fatalf("right state query error = %v", err)
	}
	leftWire, ok := leftResult.Rows[0]["state"].([]byte)
	if !ok || !bytes.HasPrefix(leftWire, []byte("HAG1")) {
		t.Fatalf("left state = %#v, want HAG1 bytes", leftResult.Rows[0]["state"])
	}
	decoded, err := hatDataStructure.NewTDigestFromAggregateState(leftWire)
	if err != nil {
		t.Fatalf("NewTDigestFromAggregateState() error = %v", err)
	}
	if got, want := decoded.Snapshot().Count, uint64(len(left)); got != want {
		t.Fatalf("decoded state count = %d, want %d", got, want)
	}

	rightWire, ok := rightResult.Rows[0]["state"].([]byte)
	if !ok {
		t.Fatalf("right state = %#v, want bytes", rightResult.Rows[0]["state"])
	}
	merged, err := ExecuteSQLQuery(`SELECT APPROX_TDIGEST_PERCENTILE_MERGE(state, 0.5) AS p50 FROM CACHE('states')`, approximateAggregateSource{
		{"state": leftWire},
		{"state": rightWire},
	})
	if err != nil {
		t.Fatalf("merge query error = %v", err)
	}
	got, ok := merged.Rows[0]["p50"].(float64)
	if !ok || math.IsNaN(got) || got < 80 || got > 120 {
		t.Fatalf("merged p50 = %#v, want a finite value near 100", merged.Rows[0]["p50"])
	}

	direct, err := ExecuteSQLQuery(`SELECT APPROX_TDIGEST_PERCENTILE(latency, 0.5, 100) AS p50 FROM CACHE('events')`, all)
	if err != nil {
		t.Fatalf("direct query error = %v", err)
	}
	directValue, ok := direct.Rows[0]["p50"].(float64)
	if !ok || math.Abs(got-directValue) > 5 {
		t.Fatalf("merged p50 = %v, direct p50 = %#v, want close estimates", got, direct.Rows[0]["p50"])
	}
}

func TestSQLTDigestAggregateStateUsesStreamingPlanner(t *testing.T) {
	for _, query := range []string{
		`SELECT APPROX_TDIGEST_PERCENTILE_STATE(latency, 100) AS state FROM CACHE('events')`,
		`SELECT APPROX_TDIGEST_PERCENTILE_MERGE(state, 0.95) AS p95 FROM CACHE('states')`,
	} {
		parsed, err := parseSQLQuery(query)
		if err != nil {
			t.Fatalf("parseSQLQuery(%q) error = %v", query, err)
		}
		if _, ok := sqlGlobalStreamAggregates(parsed); !ok {
			t.Fatalf("sqlGlobalStreamAggregates(%q) = false, want direct streaming plan", query)
		}
	}

	result, err := ExecuteSQLQuery(`SELECT APPROX_TDIGEST_PERCENTILE_STATE(latency, 100) AS state FROM CACHE('events')`, approximateAggregateStreamSource{rows: []SQLRow{
		{"latency": 1.0},
		{"latency": 2.0},
		{"latency": 3.0},
	}})
	if err != nil {
		t.Fatalf("streaming state query error = %v", err)
	}
	if wire, ok := result.Rows[0]["state"].([]byte); !ok || !bytes.HasPrefix(wire, []byte("HAG1")) {
		t.Fatalf("streaming state = %#v, want HAG1 bytes", result.Rows[0]["state"])
	}
}

func TestSQLTDigestAggregateStateIgnoresNullAndRejectsInvalidStates(t *testing.T) {
	result, err := ExecuteSQLQuery(`SELECT APPROX_TDIGEST_PERCENTILE_STATE(latency, 100) AS state FROM CACHE('events')`, approximateAggregateSource{
		{"latency": nil},
		{"latency": 1.0},
		{"latency": math.NaN()},
	})
	if err != nil {
		t.Fatalf("state query error = %v", err)
	}
	wire := result.Rows[0]["state"].([]byte)
	digest, err := hatDataStructure.NewTDigestFromAggregateState(wire)
	if err != nil {
		t.Fatalf("NewTDigestFromAggregateState() error = %v", err)
	}
	if got, want := digest.Snapshot().Count, uint64(1); got != want {
		t.Fatalf("state count = %d, want %d", got, want)
	}

	for _, query := range []string{
		`SELECT APPROX_TDIGEST_PERCENTILE_STATE(latency, 0) FROM CACHE('events')`,
		`SELECT APPROX_TDIGEST_PERCENTILE_MERGE(state) FROM CACHE('states')`,
		`SELECT APPROX_TDIGEST_PERCENTILE_MERGE(state, 1.1) FROM CACHE('states')`,
		`SELECT APPROX_TDIGEST_PERCENTILE_MERGE(state, 0.5) FROM CACHE('states')`,
	} {
		rows := approximateAggregateSource{{"state": wire}}
		if query == `SELECT APPROX_TDIGEST_PERCENTILE_MERGE(state, 0.5) FROM CACHE('states')` {
			rows = approximateAggregateSource{{"state": 1}}
		}
		if _, err := ExecuteSQLQuery(query, rows); err == nil {
			t.Fatalf("ExecuteSQLQuery(%q) error = nil, want validation error", query)
		}
	}
}

func TestSQLTDigestAggregateStateRejectsCompressionMismatchAndEmptyMergeIsNull(t *testing.T) {
	first, err := ExecuteSQLQuery(`SELECT APPROX_TDIGEST_PERCENTILE_STATE(latency, 100) AS state FROM CACHE('events')`, approximateAggregateSource{{"latency": 1.0}})
	if err != nil {
		t.Fatalf("first state query error = %v", err)
	}
	second, err := ExecuteSQLQuery(`SELECT APPROX_TDIGEST_PERCENTILE_STATE(latency, 200) AS state FROM CACHE('events')`, approximateAggregateSource{{"latency": 2.0}})
	if err != nil {
		t.Fatalf("second state query error = %v", err)
	}
	if _, err := ExecuteSQLQuery(`SELECT APPROX_TDIGEST_PERCENTILE_MERGE(state, 0.5) FROM CACHE('states')`, approximateAggregateSource{
		{"state": first.Rows[0]["state"]},
		{"state": second.Rows[0]["state"]},
	}); err == nil {
		t.Fatal("compression-mismatched states were accepted")
	}

	empty, err := ExecuteSQLQuery(`SELECT APPROX_TDIGEST_PERCENTILE_MERGE(state, 0.5) AS p50 FROM CACHE('states')`, approximateAggregateSource{{"state": nil}})
	if err != nil {
		t.Fatalf("empty merge query error = %v", err)
	}
	if got := empty.Rows[0]["p50"]; got != nil {
		t.Fatalf("empty merge result = %#v, want nil", got)
	}
}

func TestSQLTDigestAggregateStateHonorsFilter(t *testing.T) {
	result, err := ExecuteSQLQuery(`
		SELECT APPROX_TDIGEST_PERCENTILE_STATE(latency, 100) FILTER (WHERE state = 'ok') AS state
		FROM CACHE('events')
	`, approximateAggregateSource{
		{"state": "ok", "latency": 10.0},
		{"state": "skip", "latency": 1000.0},
		{"state": "ok", "latency": 20.0},
	})
	if err != nil {
		t.Fatalf("filtered state query error = %v", err)
	}
	digest, err := hatDataStructure.NewTDigestFromAggregateState(result.Rows[0]["state"].([]byte))
	if err != nil {
		t.Fatalf("NewTDigestFromAggregateState() error = %v", err)
	}
	if got, want := digest.Snapshot().Count, uint64(2); got != want {
		t.Fatalf("filtered state count = %d, want %d", got, want)
	}
}
