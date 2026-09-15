package hatReplication_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

var errReadQuorumBenchmarkFailure = errors.New("unavailable")

func BenchmarkExecuteReadQuorumFastPathC209(b *testing.B) {
	b.Run("single_success", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			result, err := hatReplication.ExecuteReadQuorum(context.Background(), []string{"east"}, 1, func(context.Context, string) (any, error) {
				return "current", nil
			}, nil)
			if err != nil || result.Value != "current" {
				b.Fatalf("read quorum = %#v, %v", result, err)
			}
		}
	})
	b.Run("three_success_control", func(b *testing.B) {
		nodes := []string{"east", "west", "local"}
		b.ReportAllocs()
		for range b.N {
			result, err := hatReplication.ExecuteReadQuorum(context.Background(), nodes, 2, func(_ context.Context, node string) (any, error) {
				if node == "west" {
					return "stale", nil
				}
				return "current", nil
			}, func(left, right any) bool {
				return left.(string) == right.(string)
			})
			if err != nil || result.Value != "current" {
				b.Fatalf("read quorum = %#v, %v", result, err)
			}
		}
	})
	b.Run("single_failure", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			result, err := hatReplication.ExecuteReadQuorum(context.Background(), []string{"east"}, 1, func(context.Context, string) (any, error) {
				return nil, errReadQuorumBenchmarkFailure
			}, nil)
			if err == nil || result.Decision.Acknowledged != 0 {
				b.Fatalf("read quorum = %#v, %v", result, err)
			}
		}
	})
}
