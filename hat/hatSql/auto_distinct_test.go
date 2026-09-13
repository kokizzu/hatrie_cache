package hatSql

import (
	"fmt"
	"strings"
	"testing"
)

func TestSQLAutoCountDistinctUsesExactAndGroupedPaths(t *testing.T) {
	rows := []SQLRow{
		{"region": "east", "visitor": "a", "active": true},
		{"region": "east", "visitor": "b", "active": true},
		{"region": "east", "visitor": "a", "active": false},
		{"region": "west", "visitor": "c", "active": true},
		{"region": "west", "visitor": "d", "active": true},
	}
	source := approximateAggregateSource(rows)
	global, err := ExecuteSQLQuery(`
		SELECT AUTO_COUNT_DISTINCT(visitor, 16) AS visitors
		FROM CACHE('events')`, source)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := global.Rows[0]["visitors"].(uint64); !ok || got != 4 {
		t.Fatalf("global visitors = %#v, want uint64(4)", global.Rows[0]["visitors"])
	}
	grouped, err := ExecuteSQLQuery(`
		SELECT region,
		       AUTO_COUNT_DISTINCT(visitor, 16) FILTER (WHERE active) AS visitors
		FROM CACHE('events')
		GROUP BY region
		ORDER BY region`, source)
	if err != nil {
		t.Fatal(err)
	}
	if len(grouped.Rows) != 2 || grouped.Rows[0]["visitors"] != uint64(2) || grouped.Rows[1]["visitors"] != uint64(2) {
		t.Fatalf("grouped visitors = %#v, want two groups with uint64(2)", grouped.Rows)
	}
}

func TestSQLAutoCountDistinctSwitchesToApproximation(t *testing.T) {
	rows := make([]SQLRow, 4096)
	for index := range rows {
		rows[index] = SQLRow{"visitor": fmt.Sprintf("visitor-%d", index)}
	}
	result, err := ExecuteSQLQuery(`
		SELECT AUTO_COUNT_DISTINCT(visitor, 8, 12) AS visitors
		FROM CACHE('events')`, approximateAggregateSource(rows))
	if err != nil {
		t.Fatal(err)
	}
	got, ok := result.Rows[0]["visitors"].(uint64)
	if !ok || got < 3700 || got > 4500 {
		t.Fatalf("approximate visitors = %#v, want uint64 in [3700,4500]", result.Rows[0]["visitors"])
	}
}

func TestSQLAutoCountDistinctValidatesOptions(t *testing.T) {
	rows := []SQLRow{{"value": "a"}}
	for _, query := range []string{
		`SELECT AUTO_COUNT_DISTINCT(value, -1) FROM CACHE('events')`,
		`SELECT AUTO_COUNT_DISTINCT(value, 1.5) FROM CACHE('events')`,
		`SELECT AUTO_COUNT_DISTINCT(value, 1, 3) FROM CACHE('events')`,
		`SELECT AUTO_COUNT_DISTINCT(value, 1, 21) FROM CACHE('events')`,
		`SELECT AUTO_COUNT_DISTINCT(value, 1, 14, 2) FROM CACHE('events')`,
	} {
		_, err := ExecuteSQLQuery(query, approximateAggregateSource(rows))
		if err == nil {
			t.Fatalf("query %q unexpectedly succeeded", query)
		}
		if strings.Contains(err.Error(), "unknown SQL function") {
			t.Fatalf("query %q returned unknown function error: %v", query, err)
		}
	}
}

func BenchmarkSQLAutoDistinct(b *testing.B) {
	lowRows := make(approximateAggregateSource, 10000)
	highRows := make(approximateAggregateSource, 10000)
	for index := range lowRows {
		lowRows[index] = SQLRow{"visitor": fmt.Sprintf("visitor-%d", index%64)}
		highRows[index] = SQLRow{"visitor": fmt.Sprintf("visitor-%d", index)}
	}
	queries := []struct {
		name  string
		rows  approximateAggregateSource
		query string
	}{
		{
			name:  "approx-low",
			rows:  lowRows,
			query: `SELECT APPROX_COUNT_DISTINCT(visitor) AS visitors FROM CACHE('events')`,
		},
		{
			name:  "auto-low-exact",
			rows:  lowRows,
			query: `SELECT AUTO_COUNT_DISTINCT(visitor, 1024) AS visitors FROM CACHE('events')`,
		},
		{
			name:  "approx-high",
			rows:  highRows,
			query: `SELECT APPROX_COUNT_DISTINCT(visitor) AS visitors FROM CACHE('events')`,
		},
		{
			name:  "auto-high-hll",
			rows:  highRows,
			query: `SELECT AUTO_COUNT_DISTINCT(visitor, 128) AS visitors FROM CACHE('events')`,
		},
	}
	for _, item := range queries {
		b.Run(item.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := ExecuteSQLQuery(item.query, item.rows); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
