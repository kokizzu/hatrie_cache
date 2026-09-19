package hatSql

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func ch042HyperLogLogWire(t *testing.T, precision uint8, values ...string) []byte {
	t.Helper()
	sketch, err := hatDataStructure.NewHyperLogLog(precision)
	if err != nil {
		t.Fatalf("NewHyperLogLog(%d) error = %v", precision, err)
	}
	for _, value := range values {
		sketch.AddJSONString(value)
	}
	wire, err := sketch.MarshalAggregateState()
	if err != nil {
		t.Fatalf("MarshalAggregateState() error = %v", err)
	}
	return wire
}

func TestCH042ApproximateDistinctStateRoundTrip(t *testing.T) {
	result, err := ExecuteSQLQuery(`
		SELECT APPROX_COUNT_DISTINCT_STATE(visitor, 10) AS state
		FROM CACHE('events')
	`, approximateAggregateSource{
		{"visitor": "alice"},
		{"visitor": "bob"},
		{"visitor": "alice"},
		{"visitor": nil},
	})
	if err != nil {
		t.Fatalf("state query error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("state query rows = %d, want one row", len(result.Rows))
	}
	wire, ok := result.Rows[0]["state"].([]byte)
	if !ok || len(wire) < 4 || string(wire[:4]) != "HAG1" {
		t.Fatalf("state value = %#v, want HAG1 []byte", result.Rows[0]["state"])
	}
	sketch, err := hatDataStructure.NewHyperLogLogFromAggregateState(wire)
	if err != nil {
		t.Fatalf("NewHyperLogLogFromAggregateState() error = %v", err)
	}
	if got := sketch.Count(); got != 2 {
		t.Fatalf("decoded approximate distinct count = %d, want 2", got)
	}
}

func TestCH042ApproximateDistinctMergeCombinesStates(t *testing.T) {
	rows := approximateAggregateSource{
		{"state": ch042HyperLogLogWire(t, 10, "alice", "bob")},
		{"state": ch042HyperLogLogWire(t, 10, "bob", "carol")},
	}
	result, err := ExecuteSQLQuery(`
		SELECT APPROX_COUNT_DISTINCT_MERGE(state) AS visitors
		FROM CACHE('states')
	`, rows)
	if err != nil {
		t.Fatalf("merge query error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("merge query rows = %d, want one row", len(result.Rows))
	}
	if got, ok := result.Rows[0]["visitors"].(uint64); !ok || got != 3 {
		t.Fatalf("merged approximate distinct count = %#v, want uint64(3)", result.Rows[0]["visitors"])
	}
}

func TestCH042ApproximateDistinctMergeRejectsMismatchedStates(t *testing.T) {
	rows := approximateAggregateSource{
		{"state": ch042HyperLogLogWire(t, 10, "alice")},
		{"state": ch042HyperLogLogWire(t, 11, "bob")},
	}
	if _, err := ExecuteSQLQuery(`
		SELECT APPROX_COUNT_DISTINCT_MERGE(state)
		FROM CACHE('states')
	`, rows); err == nil {
		t.Fatal("merge query accepted HLL states with different precision")
	}
}

func TestCH042ApproximateDistinctStatesUseStreamingPath(t *testing.T) {
	for _, query := range []string{
		`SELECT APPROX_COUNT_DISTINCT_STATE(visitor, 10) AS state FROM CACHE('events')`,
		`SELECT APPROX_COUNT_DISTINCT_MERGE(state) AS visitors FROM CACHE('states')`,
	} {
		t.Run(query, func(t *testing.T) {
			parsed, err := parseSQLQuery(query)
			if err != nil {
				t.Fatalf("parseSQLQuery() error = %v", err)
			}
			aggregates, ok := sqlGlobalStreamAggregates(parsed)
			if !ok {
				t.Fatal("sqlGlobalStreamAggregates() rejected approximate distinct state")
			}
			if !sqlApproximateDirectSourcePlan(parsed, aggregates) {
				t.Fatal("approximate distinct state did not select direct source streaming")
			}
		})
	}
}

func TestCH042ApproximateDistinctMergeEmptyReturnsZero(t *testing.T) {
	result, err := ExecuteSQLQuery(`
		SELECT APPROX_COUNT_DISTINCT_MERGE(state) AS visitors
		FROM CACHE('states')
	`, approximateAggregateSource{})
	if err != nil {
		t.Fatalf("empty merge query error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("empty merge rows = %d, want one row", len(result.Rows))
	}
	if got, ok := result.Rows[0]["visitors"].(uint64); !ok || got != 0 {
		t.Fatalf("empty merge result = %#v, want uint64(0)", result.Rows[0]["visitors"])
	}
}
