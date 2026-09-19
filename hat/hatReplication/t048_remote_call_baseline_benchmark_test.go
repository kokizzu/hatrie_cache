package hatReplication

import (
	"context"
	"testing"
)

type t048BaselineRequest struct {
	Method         uint8
	IdempotencyKey string
	FencingToken   uint64
	Attempt        int
}

func BenchmarkT048Baseline(b *testing.B) {
	ctx := context.Background()
	request := t048BaselineRequest{Method: 1, IdempotencyKey: "order-42", FencingToken: 9, Attempt: 1}
	call := func(context.Context, t048BaselineRequest) (int, error) {
		return 42, nil
	}
	b.ReportAllocs()
	result := 0
	for i := 0; i < b.N; i++ {
		value, err := call(ctx, request)
		if err != nil {
			b.Fatal(err)
		}
		result += value
	}
	if result == 0 {
		b.Fatal("unexpected zero result")
	}
}
