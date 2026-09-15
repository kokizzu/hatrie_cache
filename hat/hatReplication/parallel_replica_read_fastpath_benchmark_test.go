package hatReplication_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

var parallelReplicaReadFastPathSink hatReplication.ParallelReplicaReadResult

func BenchmarkExecuteParallelReplicaReadFastPathC208(b *testing.B) {
	b.Run("single_success", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			parallelReplicaReadFastPathSink, _ = hatReplication.ExecuteParallelReplicaRead(context.Background(), []string{"node-a"}, 0, func(context.Context, string) (any, error) {
				return "ok", nil
			})
		}
	})
	b.Run("three_success_control", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			parallelReplicaReadFastPathSink, _ = hatReplication.ExecuteParallelReplicaRead(context.Background(), []string{"node-a", "node-b", "node-c"}, 0, func(context.Context, string) (any, error) {
				return "ok", nil
			})
		}
	})
	b.Run("single_failure", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			parallelReplicaReadFastPathSink, _ = hatReplication.ExecuteParallelReplicaRead(context.Background(), []string{"node-a"}, 0, func(context.Context, string) (any, error) {
				return nil, context.Canceled
			})
		}
	})
}
