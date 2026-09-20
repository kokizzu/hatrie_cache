package hatSql

import (
	"context"
	"errors"
	"testing"
)

func TestCHG42SQLQueryTracksOperatorWorkingBytes(t *testing.T) {
	tracker, err := NewSQLOperatorMemoryTracker(1 << 20)
	if err != nil {
		t.Fatalf("create tracker: %v", err)
	}
	query := `FROM VALUES ('b', 2), ('a', 1) AS src(group_id, value)
SELECT src.group_id, SUM(src.value) AS total
GROUP BY src.group_id
ORDER BY src.group_id`
	result, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{OperatorMemoryTracker: tracker})
	if err != nil {
		t.Fatalf("execute query: %v", err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["group_id"] != "a" {
		t.Fatalf("query result = %#v, want two ordered groups", result.Rows)
	}

	stats := tracker.Snapshot()
	if len(stats) < 2 {
		t.Fatalf("operator stats = %#v, want GROUP BY and SORT", stats)
	}
	for _, stat := range stats {
		if stat.PeakBytes <= 0 {
			t.Fatalf("operator %q peak bytes = %d, want positive", stat.Operator, stat.PeakBytes)
		}
		if stat.CurrentBytes != 0 {
			t.Fatalf("operator %q current bytes = %d, want released", stat.Operator, stat.CurrentBytes)
		}
	}

	limited, err := NewSQLOperatorMemoryTracker(1)
	if err != nil {
		t.Fatalf("create limited tracker: %v", err)
	}
	if _, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{OperatorMemoryTracker: limited}); !errors.Is(err, ErrSQLOperatorMemoryLimit) {
		t.Fatalf("limited query error = %v, want ErrSQLOperatorMemoryLimit", err)
	}
}

func BenchmarkCHG42SQLQueryMemoryTracking(b *testing.B) {
	query := `FROM VALUES ('b', 2), ('a', 1) AS src(group_id, value)
SELECT src.group_id, SUM(src.value) AS total
GROUP BY src.group_id
ORDER BY src.group_id`
	b.Run("default", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			result, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{})
			if err != nil || len(result.Rows) != 2 {
				b.Fatalf("default query rows=%d err=%v", len(result.Rows), err)
			}
		}
	})
	b.Run("tracked", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			tracker, err := NewSQLOperatorMemoryTracker(1 << 20)
			if err != nil {
				b.Fatal(err)
			}
			result, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{OperatorMemoryTracker: tracker})
			if err != nil || len(result.Rows) != 2 {
				b.Fatalf("tracked query rows=%d err=%v", len(result.Rows), err)
			}
		}
	})
}
