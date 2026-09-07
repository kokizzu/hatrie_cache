package hatReplication_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatReplication"
)

func BenchmarkExecuteParallelReplicaRead(b *testing.B) {
	b.Run("parallel_first_success", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatReplication.ExecuteParallelReplicaRead(context.Background(), []string{"a", "b", "c"}, 0, func(context.Context, string) (any, error) {
				return "ok", nil
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("hedged_success", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatReplication.ExecuteParallelReplicaRead(context.Background(), []string{"slow", "fast"}, time.Microsecond, func(ctx context.Context, node string) (any, error) {
				if node == "slow" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return "ok", nil
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("all_fail", func(b *testing.B) {
		failure := errors.New("unavailable")
		b.ReportAllocs()
		for range b.N {
			if _, err := hatReplication.ExecuteParallelReplicaRead(context.Background(), []string{"a", "b", "c"}, 0, func(context.Context, string) (any, error) {
				return nil, failure
			}); !errors.Is(err, hatReplication.ErrParallelReplicaReadFailed) {
				b.Fatal(err)
			}
		}
	})
}
