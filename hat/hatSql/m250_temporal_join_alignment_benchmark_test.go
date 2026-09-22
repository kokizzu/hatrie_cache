package hatSql

import (
	"context"
	"testing"
)

func benchmarkM250ReadyAlignment(b *testing.B) *SQLTemporalJoinFrontierAlignment {
	b.Helper()
	left, err := NewSQLSourceFrontierBarrierFromPartitions([]SQLSourceFrontierPartition{{Source: "left", Partition: "0"}})
	if err != nil {
		b.Fatal(err)
	}
	right, err := NewSQLSourceFrontierBarrierFromPartitions([]SQLSourceFrontierPartition{{Source: "right", Partition: "0"}})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := left.Observe(SQLSourceFrontier{Source: "left", Partition: "0", Frontier: 7}); err != nil {
		b.Fatal(err)
	}
	if _, err := right.Observe(SQLSourceFrontier{Source: "right", Partition: "0", Frontier: 8}); err != nil {
		b.Fatal(err)
	}
	alignment, err := NewSQLTemporalJoinFrontierAlignment(left, right)
	if err != nil {
		b.Fatal(err)
	}
	return alignment
}

// BenchmarkM250TemporalJoinAlignmentReady measures the opt-in aligned join
// fast path after both inputs have published a frontier.
func BenchmarkM250TemporalJoinAlignmentReady(b *testing.B) {
	alignment := benchmarkM250ReadyAlignment(b)
	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		result, err := alignment.WaitForFrontier(ctx, 7)
		if err != nil || result.CommonFrontier != 7 {
			b.Fatalf("alignment result = %+v, err = %v", result, err)
		}
	}
}

// BenchmarkM250TemporalJoinSequentialReady is the pre-alignment baseline:
// each input barrier is waited on independently.
func BenchmarkM250TemporalJoinSequentialReady(b *testing.B) {
	alignment := benchmarkM250ReadyAlignment(b)
	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		left, err := alignment.left.WaitForFrontier(ctx, 7)
		if err != nil {
			b.Fatal(err)
		}
		right, err := alignment.right.WaitForFrontier(ctx, 7)
		if err != nil || left != 7 || right != 8 {
			b.Fatalf("sequential frontiers = %d/%d, err = %v", left, right, err)
		}
	}
}
