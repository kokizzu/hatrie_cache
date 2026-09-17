package hatSql_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCH036AggregateOrNullReturnsNullOnlyWithoutContributingValues(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`
FROM VALUES (1), (NULL), (5) AS events(value)
SELECT COUNT_OR_NULL(events.value) AS counted,
       SUM_OR_NULL(events.value) AS summed,
       AVG_OR_NULL(events.value) AS averaged,
       MIN_OR_NULL(events.value) AS minimum,
       MAX_OR_NULL(events.value) AS maximum`, nil)
	if err != nil {
		t.Fatalf("OrNull query error = %v", err)
	}
	want := []hatSql.SQLRow{{
		"counted":  int64(2),
		"summed":   float64(6),
		"averaged": float64(3),
		"minimum":  float64(1),
		"maximum":  float64(5),
	}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("OrNull rows = %#v, want %#v", result.Rows, want)
	}

	empty, err := hatSql.ExecuteSQLQuery(`
FROM VALUES (1), (NULL) AS events(value)
WHERE events.value IS NULL
SELECT COUNT_OR_NULL(events.value) AS counted,
       SUM_OR_NULL(events.value) AS summed,
       AVG_OR_NULL(events.value) AS averaged,
       MIN_OR_NULL(events.value) AS minimum,
       MAX_OR_NULL(events.value) AS maximum`, nil)
	if err != nil {
		t.Fatalf("empty OrNull query error = %v", err)
	}
	wantEmpty := []hatSql.SQLRow{{
		"counted":  nil,
		"summed":   nil,
		"averaged": nil,
		"minimum":  nil,
		"maximum":  nil,
	}}
	if !reflect.DeepEqual(empty.Rows, wantEmpty) {
		t.Fatalf("empty OrNull rows = %#v, want %#v", empty.Rows, wantEmpty)
	}
}

func TestCH036AggregateOrNullSupportsStarAndFilter(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery(`
FROM VALUES (1, TRUE), (2, FALSE), (3, TRUE) AS events(value, keep)
SELECT COUNT_OR_NULL(*) FILTER (WHERE events.keep) AS counted,
       SUM_OR_NULL(events.value) FILTER (WHERE events.keep) AS summed`, nil)
	if err != nil {
		t.Fatalf("filtered OrNull query error = %v", err)
	}
	want := []hatSql.SQLRow{{"counted": int64(2), "summed": float64(4)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("filtered OrNull rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCH036AggregateOrNullRejectsInvalidArity(t *testing.T) {
	if _, err := hatSql.ExecuteSQLQuery(`
FROM VALUES (1) AS events(value)
SELECT SUM_OR_NULL(events.value, events.value)`, nil); err == nil {
		t.Fatal("SUM_OR_NULL accepted two arguments")
	}
}
