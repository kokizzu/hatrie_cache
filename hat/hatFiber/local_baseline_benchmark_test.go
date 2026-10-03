package hatFiber

import (
	"context"
	"runtime"
	"testing"
)

func BenchmarkT032ContextWithValueSetGetBaseline(b *testing.B) {
	type key struct{}
	var k key
	b.ReportAllocs()
	b.ResetTimer()
	var got any
	for index := 0; index < b.N; index++ {
		ctx := context.WithValue(context.Background(), k, index)
		got = ctx.Value(k)
	}
	runtime.KeepAlive(got)
}

func BenchmarkT032ContextValueGetBaseline(b *testing.B) {
	type key struct{}
	var k key
	ctx := context.WithValue(context.Background(), k, 1)
	b.ReportAllocs()
	b.ResetTimer()
	var got any
	for index := 0; index < b.N; index++ {
		got = ctx.Value(k)
	}
	runtime.KeepAlive(got)
}
