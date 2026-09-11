package hatSql

import (
	"reflect"
	"strings"
	"testing"
)

func TestSQLAsofJoinChoosesLatestPriorMatch(t *testing.T) {
	left := []Row{
		{"symbol": "A", "at": int64(3)},
		{"symbol": "A", "at": int64(12)},
		{"symbol": "B", "at": int64(5)},
	}
	right := []Row{
		{"symbol": "A", "at": int64(1), "price": int64(10)},
		{"symbol": "A", "at": int64(10), "price": int64(20)},
		{"symbol": "A", "at": int64(15), "price": int64(30)},
		{"symbol": "B", "at": int64(7), "price": int64(70)},
	}
	resolver := SourceResolverFunc(func(_, key string) ([]Row, error) {
		switch key {
		case "trades":
			return left, nil
		case "quotes":
			return right, nil
		default:
			t.Fatalf("unexpected source %q", key)
			return nil, nil
		}
	})
	result, err := ExecuteSQLQuery(`FROM CACHE('trades') AS l ASOF JOIN CACHE('quotes') AS r ON l.symbol = r.symbol AND l.at >= r.at SELECT l.at, r.price ORDER BY l.at`, resolver)
	if err != nil {
		t.Fatalf("ASOF JOIN error = %v", err)
	}
	want := []Row{
		{"at": int64(3), "price": int64(10)},
		{"at": int64(12), "price": int64(20)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("ASOF JOIN rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLAsofLeftJoinPreservesUnmatchedRowsAndStrictTime(t *testing.T) {
	left := []Row{
		{"symbol": "A", "at": int64(1)},
		{"symbol": "A", "at": int64(3)},
	}
	right := []Row{
		{"symbol": "A", "at": int64(1), "price": int64(10)},
		{"symbol": "A", "at": int64(2), "price": int64(20)},
	}
	resolver := SourceResolverFunc(func(_, key string) ([]Row, error) {
		if key == "trades" {
			return left, nil
		}
		return right, nil
	})
	result, err := ExecuteSQLQuery(`FROM CACHE('trades') AS l ASOF LEFT JOIN CACHE('quotes') AS r ON l.symbol = r.symbol AND l.at > r.at SELECT l.at, r.price ORDER BY l.at`, resolver)
	if err != nil {
		t.Fatalf("ASOF LEFT JOIN error = %v", err)
	}
	want := []Row{
		{"at": int64(1), "price": nil},
		{"at": int64(3), "price": int64(20)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("ASOF LEFT JOIN rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLAsofJoinRejectsIncompleteTemporalCondition(t *testing.T) {
	_, err := ExecuteSQLQuery(`FROM CACHE('trades') AS l ASOF JOIN CACHE('quotes') AS r ON l.symbol = r.symbol SELECT l.symbol`, SourceResolverFunc(func(string, string) ([]Row, error) {
		return nil, nil
	}))
	if err == nil || !strings.Contains(err.Error(), "ASOF JOIN requires one equality and one temporal inequality") {
		t.Fatalf("incomplete ASOF JOIN error = %v", err)
	}
}
