package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLIndexedJoinUsesBorrowedIndexPostings(t *testing.T) {
	resolver := &borrowedIndexedJoinResolver{
		borrowedAvailable: true,
		left: []hatSql.SQLRow{{"id": int64(1), "team": "blue"}, {"id": int64(2), "team": "red"}},
		right: map[string][]hatSql.SQLRow{
			"blue": {{"team": "blue", "name": "ocean"}},
			"red":  {{"team": "red", "name": "fire"}},
		},
	}
	result, err := hatSql.ExecuteSQLQuery("FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.team = r.team SELECT l.id, r.name ORDER BY l.id", resolver)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["name"] != "ocean" || result.Rows[1]["name"] != "fire" {
		t.Fatalf("join rows = %#v", result.Rows)
	}
	if resolver.borrowedCalls == 0 {
		t.Fatal("borrowed index postings were not used")
	}
	if resolver.indexedCalls != 0 {
		t.Fatalf("copied index calls = %d, want 0", resolver.indexedCalls)
	}
}

func TestSQLIndexedJoinFallsBackWhenBorrowedPostingsUnavailable(t *testing.T) {
	resolver := &borrowedIndexedJoinResolver{
		left: []hatSql.SQLRow{{"id": int64(1), "team": "blue"}},
		right: map[string][]hatSql.SQLRow{"blue": {{"team": "blue", "name": "ocean"}}},
	}
	result, err := hatSql.ExecuteSQLQuery("FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.team = r.team SELECT l.id, r.name", resolver)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["name"] != "ocean" {
		t.Fatalf("join rows = %#v", result.Rows)
	}
	if resolver.borrowedCalls == 0 || resolver.indexedCalls == 0 {
		t.Fatalf("borrowed/copied calls = %d/%d, want availability probe and fallback", resolver.borrowedCalls, resolver.indexedCalls)
	}
}

type borrowedIndexedJoinResolver struct {
	left         []hatSql.SQLRow
	right        map[string][]hatSql.SQLRow
	borrowedAvailable bool
	indexedCalls int
	borrowedCalls int
}

func (resolver *borrowedIndexedJoinResolver) ResolveSQLSource(_ string, key string) ([]hatSql.SQLRow, error) {
	if key == "left" {
		return resolver.left, nil
	}
	return nil, nil
}

func (resolver *borrowedIndexedJoinResolver) ResolveSQLIndexedSource(_ string, _ string, _ string, value interface{}) ([]hatSql.SQLRow, bool, error) {
	resolver.indexedCalls++
	if value == nil {
		return nil, true, nil
	}
	return resolver.right[value.(string)], true, nil
}

func (resolver *borrowedIndexedJoinResolver) BorrowSQLIndexedSource(_ string, _ string, _ string, value interface{}) ([]hatSql.SQLRow, bool, error) {
	resolver.borrowedCalls++
	if !resolver.borrowedAvailable {
		return nil, false, nil
	}
	if value == nil {
		return nil, true, nil
	}
	return resolver.right[value.(string)], true, nil
}
