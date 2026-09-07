package hatReplication_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func BenchmarkExecuteReadQuorum(b *testing.B) {
	nodes := []string{"east", "west", "local"}
	read := func(_ context.Context, node string) (any, error) {
		if node == "west" {
			return "stale", nil
		}
		return "current", nil
	}
	b.Run("typed_equal", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			result, err := hatReplication.ExecuteReadQuorum(context.Background(), nodes, 2, read, func(left, right any) bool {
				return left.(string) == right.(string)
			})
			if err != nil || result.Value != "current" {
				b.Fatalf("read quorum = %#v, %v", result, err)
			}
		}
	})
	b.Run("deep_equal", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			result, err := hatReplication.ExecuteReadQuorum(context.Background(), nodes, 2, read, nil)
			if err != nil || result.Value != "current" {
				b.Fatalf("read quorum = %#v, %v", result, err)
			}
		}
	})
}
