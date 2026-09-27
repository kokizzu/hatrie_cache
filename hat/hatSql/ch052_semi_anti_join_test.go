package hatSql

import (
	"reflect"
	"testing"
)

func TestCH052SemiAndAntiJoinFilterLeftRows(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []SQLRow
	}{
		{
			name: "semi",
			query: `FROM VALUES
  ('a'), ('b'), ('a'), ('c') AS left_rows(symbol)
LEFT SEMI JOIN VALUES
  ('a'), ('c'), ('a') AS right_rows(symbol)
ON left_rows.symbol = right_rows.symbol
SELECT left_rows.symbol AS symbol
ORDER BY left_rows.symbol`,
			want: []SQLRow{{"symbol": "a"}, {"symbol": "a"}, {"symbol": "c"}},
		},
		{
			name: "anti",
			query: `FROM VALUES
  ('a'), ('b'), ('a'), ('c') AS left_rows(symbol)
LEFT ANTI JOIN VALUES
  ('a'), ('c'), ('a') AS right_rows(symbol)
ON left_rows.symbol = right_rows.symbol
SELECT left_rows.symbol AS symbol
ORDER BY left_rows.symbol`,
			want: []SQLRow{{"symbol": "b"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := ExecuteSQLQuery(test.query, nil)
			if err != nil {
				t.Fatalf("%s join error = %v", test.name, err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("%s rows = %#v, want %#v", test.name, result.Rows, test.want)
			}
		})
	}
}

func TestCH052SemiAndAntiJoinRequireAnEqualityPredicate(t *testing.T) {
	query := `FROM VALUES (1) AS left_rows(value)
LEFT SEMI JOIN VALUES (1) AS right_rows(value)
ON left_rows.value >= right_rows.value
SELECT left_rows.value`
	if _, err := ExecuteSQLQuery(query, nil); err == nil {
		t.Fatal("semi join without an equality predicate unexpectedly succeeded")
	}
}

func TestCH052SemiAndAntiJoinNullKeys(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []SQLRow
	}{
		{
			name: "semi excludes null keys",
			query: `FROM VALUES
  (NULL), ('a'), ('b') AS left_rows(symbol)
LEFT SEMI JOIN VALUES
  (NULL), ('a') AS right_rows(symbol)
ON left_rows.symbol = right_rows.symbol
SELECT left_rows.symbol AS symbol`,
			want: []SQLRow{{"symbol": "a"}},
		},
		{
			name: "anti retains null keys",
			query: `FROM VALUES
  (NULL), ('a'), ('b') AS left_rows(symbol)
LEFT ANTI JOIN VALUES
  (NULL), ('a') AS right_rows(symbol)
ON left_rows.symbol = right_rows.symbol
SELECT left_rows.symbol AS symbol`,
			want: []SQLRow{{"symbol": nil}, {"symbol": "b"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := ExecuteSQLQuery(test.query, nil)
			if err != nil {
				t.Fatalf("%s join error = %v", test.name, err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("%s rows = %#v, want %#v", test.name, result.Rows, test.want)
			}
		})
	}
}

func BenchmarkCH052NaiveSemiJoin(b *testing.B) {
	left := ch052BenchmarkKeys(10000, 0)
	right := ch052BenchmarkKeys(4096, 1)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		matched := 0
		for _, leftKey := range left {
			for _, rightKey := range right {
				if leftKey == rightKey {
					matched++
					break
				}
			}
		}
		if matched == 0 {
			b.Fatal("naive semi join produced no matches")
		}
	}
}

func BenchmarkCH052IndexedSemiJoin(b *testing.B) {
	left := ch052BenchmarkKeys(10000, 0)
	right := ch052BenchmarkKeys(4096, 1)
	index := make(map[int64]struct{}, len(right))
	for _, key := range right {
		index[key] = struct{}{}
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		matched := 0
		for _, leftKey := range left {
			if _, ok := index[leftKey]; ok {
				matched++
			}
		}
		if matched == 0 {
			b.Fatal("indexed semi join produced no matches")
		}
	}
}

func ch052BenchmarkKeys(count, offset int) []int64 {
	keys := make([]int64, count)
	for index := range keys {
		keys[index] = int64((index + offset) % 8192)
	}
	return keys
}
