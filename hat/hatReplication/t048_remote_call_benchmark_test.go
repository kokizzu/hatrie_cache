package hatReplication

import (
	"context"
	"testing"
)

var benchmarkT048RemoteCallResult int

func BenchmarkT048RetryRemoteCall(b *testing.B) {
	ctx := context.Background()
	call := func(context.Context, RemoteCallRequest) (int, error) {
		return 42, nil
	}
	policy := RemoteCallPolicy{Method: RemoteCallRead, MaxAttempts: 1}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		value, err := RetryRemoteCall(ctx, call, policy)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkT048RemoteCallResult = value
	}
}
