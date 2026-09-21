package hatSql_test

import (
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

const mz009ValidityIndexQuery = "SELECT id FROM CACHE('events') WHERE VALID_AT(TIMESTAMP '2026-01-01T01:00:00Z', valid_from, valid_to) ORDER BY id"

type mz009ValidityIndexResolver struct {
	rows           []hatSql.Row
	candidates     []hatSql.Row
	indexAvailable bool
	rangeCalls     int
}

func (resolver *mz009ValidityIndexResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "events" {
		return nil, nil
	}
	return resolver.rows, nil
}

func (resolver *mz009ValidityIndexResolver) ResolveSQLIndexedRangeSource(name, key, field, operator string, value interface{}) ([]hatSql.Row, bool, error) {
	if name != "CACHE" || key != "events" || field != "valid_from" || operator != "<=" || !resolver.indexAvailable {
		return nil, false, nil
	}
	if _, ok := value.(time.Time); !ok {
		return nil, false, nil
	}
	resolver.rangeCalls++
	return resolver.candidates, true, nil
}

func TestMZ009ValidityIndexPushdownPreservesExactResults(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []hatSql.Row{
		{"id": int64(1), "valid_from": base, "valid_to": base.Add(2 * time.Hour)},
		{"id": int64(2), "valid_from": base.Add(24 * time.Hour), "valid_to": nil},
	}
	resolver := &mz009ValidityIndexResolver{
		rows:           rows,
		candidates:     rows[:1],
		indexAvailable: true,
	}
	result, err := hatSql.ExecuteSQLQuery(mz009ValidityIndexQuery, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.rangeCalls != 1 {
		t.Fatalf("range index calls = %d, want 1", resolver.rangeCalls)
	}
	want := []hatSql.Row{{"id": int64(1)}}
	if len(result.Rows) != len(want) || result.Rows[0]["id"] != want[0]["id"] {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestMZ009ValidityIndexUnavailableFallsBack(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []hatSql.Row{
		{"id": int64(1), "valid_from": base, "valid_to": base.Add(2 * time.Hour)},
		{"id": int64(2), "valid_from": base.Add(24 * time.Hour), "valid_to": nil},
	}
	resolver := &mz009ValidityIndexResolver{rows: rows, indexAvailable: false}
	result, err := hatSql.ExecuteSQLQuery(mz009ValidityIndexQuery, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if resolver.rangeCalls != 0 {
		t.Fatalf("range index calls = %d, want 0", resolver.rangeCalls)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != int64(1) {
		t.Fatalf("fallback rows = %#v, want one row with id 1", result.Rows)
	}
}
