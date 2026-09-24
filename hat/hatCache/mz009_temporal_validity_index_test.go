package hatCache

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSQLJSONValidityIndexAcceleratesAndRefreshesValidAt(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	data := `[
  {"id":1,"valid_from":"2026-01-01T00:00:00Z","valid_to":"2026-01-02T00:00:00Z"},
  {"id":2,"valid_from":"2026-01-02T00:00:00Z","valid_to":null},
  {"id":3,"valid_from":null,"valid_to":"2026-01-01T00:00:00Z"},
  {"id":4,"valid_from":null,"valid_to":null},
  {"id":5,"valid_from":"2026-01-02T00:00:00Z","valid_to":"2026-01-02T00:00:00Z"}
]`
	trie.UpsertString("events", data)
	if err := trie.CreateSQLJSONValidityIndex("events", "valid_from", "valid_to"); err != nil {
		t.Fatalf("CreateSQLJSONValidityIndex() error = %v", err)
	}

	query := "FROM CACHE('events') AS event WHERE VALID_AT(TIMESTAMP '2026-01-01T12:00:00Z', event.valid_from, event.valid_to) SELECT event.id ORDER BY event.id"
	want := []SQLRow{{"id": float64(1)}, {"id": float64(4)}}
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("validity query error = %v", err)
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("validity rows = %#v, want %#v", result.Rows, want)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("validity EXPLAIN error = %v", err)
	}
	if !sqlValidityPlanHasNode(explained.Rows, "VALIDITY INDEX SCAN") {
		t.Fatalf("validity plan = %#v, want VALIDITY INDEX SCAN", explained.Rows)
	}

	trie.UpsertString("events", `[{"id":9,"valid_from":"2026-01-01T00:00:00Z","valid_to":"2026-01-03T00:00:00Z"}]`)
	refreshed, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("refreshed validity query error = %v", err)
	}
	if want := []SQLRow{{"id": float64(9)}}; !reflect.DeepEqual(refreshed.Rows, want) {
		t.Fatalf("refreshed validity rows = %#v, want %#v", refreshed.Rows, want)
	}
}

func TestSQLJSONValidityIndexFallsBackWhenAdmissionDenied(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	data := `[{"id":1,"valid_from":"2026-01-01T00:00:00Z","valid_to":null}]`
	trie.UpsertString("events", data)
	if err := trie.SetSQLJSONIndexAdmissionBudget(SQLJSONIndexAdmissionBudget{MaxSourceBytes: len(data) - 1}); err != nil {
		t.Fatalf("SetSQLJSONIndexAdmissionBudget() error = %v", err)
	}
	if err := trie.CreateSQLJSONValidityIndex("events", "valid_from", "valid_to"); err != nil {
		t.Fatalf("CreateSQLJSONValidityIndex() error = %v", err)
	}
	query := "FROM CACHE('events') WHERE VALID_AT(TIMESTAMP '2026-01-01T12:00:00Z', valid_from, valid_to) SELECT id"
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("admission fallback = %#v, %v", result.Rows, err)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("admission fallback EXPLAIN error = %v", err)
	}
	if sqlValidityPlanHasNode(explained.Rows, "VALIDITY INDEX SCAN") {
		t.Fatalf("admission-denied query unexpectedly used validity index: %#v", explained.Rows)
	}
}

func TestSQLJSONValidityIndexUsesSQLTimestampForms(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("events", `[{"id":1,"valid_from":"2026-01-01 00:00:00","valid_to":null}]`)
	if err := trie.CreateSQLJSONValidityIndex("events", "valid_from", "valid_to"); err != nil {
		t.Fatalf("CreateSQLJSONValidityIndex() error = %v", err)
	}
	rows, available, err := trie.ResolveSQLTemporalValiditySource("CACHE", "events", time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC), "valid_from", "valid_to")
	if err != nil || !available || len(rows) != 1 || rows[0]["id"] != float64(1) {
		t.Fatalf("ResolveSQLTemporalValiditySource() = %#v, %v, %v", rows, available, err)
	}
}

func TestSQLJSONValidityIndexFallsBackForMalformedBounds(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("events", `[
  {"id":1,"valid_from":"not-a-timestamp","valid_to":null},
  {"id":2,"valid_from":"2026-01-01T00:00:00Z","valid_to":null}
]`)
	if err := trie.CreateSQLJSONValidityIndex("events", "valid_from", "valid_to"); err != nil {
		t.Fatalf("CreateSQLJSONValidityIndex() error = %v", err)
	}
	_, err := ExecuteSQLQuery("FROM CACHE('events') WHERE VALID_AT(TIMESTAMP '2026-01-01T12:00:00Z', valid_from, valid_to) SELECT id", trie)
	if err == nil || !strings.Contains(err.Error(), "timestamp must be RFC3339") {
		t.Fatalf("malformed validity query error = %v, want timestamp diagnostic", err)
	}
}

func sqlValidityPlanHasNode(rows []SQLRow, node string) bool {
	for _, row := range rows {
		if value, ok := row["node"].(string); ok && strings.EqualFold(value, node) {
			return true
		}
	}
	return false
}
