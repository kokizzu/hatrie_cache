package hatSql

import (
	"context"
	"testing"
)

func c230BenchmarkAdmission(b *testing.B) (*SQLClusterAdmission, SQLClusterAdmissionRequest) {
	b.Helper()
	admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{
		Default: SQLClusterAdmissionPolicy{
			Serving: SQLClusterAdmissionPool{
				CPUUnits:    1,
				MemoryBytes: 1 << 20,
				MaxRunning:  1,
				MaxQueued:   64,
			},
			Maintenance: SQLClusterAdmissionPool{
				CPUUnits:    1,
				MemoryBytes: 1 << 20,
				MaxRunning:  1,
				MaxQueued:   64,
			},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	return admission, SQLClusterAdmissionRequest{
		Cluster:     "c230-benchmark",
		Class:       SQLClusterWorkServing,
		CPUUnits:    1,
		MemoryBytes: 64,
	}
}

func BenchmarkC230SQLQueryAdmission(b *testing.B) {
	const query = "FROM VALUES (1), (2), (3), (4) AS src(value) SELECT src.value"
	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			result, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{})
			if err != nil || len(result.Rows) != 4 {
				b.Fatalf("direct query rows=%d err=%v", len(result.Rows), err)
			}
		}
	})
	b.Run("memory_admitted", func(b *testing.B) {
		admission, request := c230BenchmarkAdmission(b)
		options := SQLQueryOptions{
			ClusterAdmission:        admission,
			ClusterAdmissionRequest: request,
		}
		b.ReportAllocs()
		for range b.N {
			result, err := ExecuteSQLQueryContext(context.Background(), query, nil, options)
			if err != nil || len(result.Rows) != 4 {
				b.Fatalf("admitted query rows=%d err=%v", len(result.Rows), err)
			}
		}
	})
}

func BenchmarkC230MemoryAdmissionAcquireRelease(b *testing.B) {
	admission, request := c230BenchmarkAdmission(b)
	ctx := context.Background()
	b.ReportAllocs()
	for range b.N {
		lease, err := admission.Acquire(ctx, request)
		if err != nil {
			b.Fatal(err)
		}
		lease.Release()
	}
}
