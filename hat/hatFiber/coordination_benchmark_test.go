package hatFiber_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatFiber"
	"hatrie_cache/hat/hatPipeline"
)

func BenchmarkT031FiberChannelRoundTrip(b *testing.B) {
	channel, err := hatFiber.NewChannel[int](64)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := channel.Send(index); err != nil {
			b.Fatal(err)
		}
		if _, ok, err := channel.Receive(); err != nil || !ok {
			b.Fatalf("Receive() = %t, %v", ok, err)
		}
	}
}

func BenchmarkT031PipelineChannelRoundTrip(b *testing.B) {
	channel, err := hatPipeline.NewChannel[int](64)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := channel.Send(context.Background(), index); err != nil {
			b.Fatal(err)
		}
		if _, ok, err := channel.Receive(context.Background()); err != nil || !ok {
			b.Fatalf("Receive() = %t, %v", ok, err)
		}
	}
}
