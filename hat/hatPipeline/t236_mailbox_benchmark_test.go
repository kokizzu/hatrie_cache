package hatPipeline

import (
	"context"
	"sync"
	"testing"
)

var t236MailboxBenchmarkSink int

func BenchmarkT236NativeChannelMPSC(b *testing.B) {
	channel := make(chan int, 256)
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(4)
	for producer := 0; producer < 4; producer++ {
		first, last := t236MailboxProducerRange(b.N, producer)
		go func(first, last int) {
			defer workers.Done()
			<-start
			for value := first; value < last; value++ {
				channel <- value
			}
		}(first, last)
	}
	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	for value := 0; value < b.N; value++ {
		t236MailboxBenchmarkSink += <-channel
	}
	workers.Wait()
	b.StopTimer()
}

func BenchmarkT236PipelineChannelMPSC(b *testing.B) {
	channel, err := NewChannel[int](256)
	if err != nil {
		b.Fatal(err)
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(4)
	ctx := context.Background()
	for producer := 0; producer < 4; producer++ {
		first, last := t236MailboxProducerRange(b.N, producer)
		go func(first, last int) {
			defer workers.Done()
			<-start
			for value := first; value < last; value++ {
				if err := channel.Send(ctx, value); err != nil {
					panic(err)
				}
			}
		}(first, last)
	}
	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	for value := 0; value < b.N; value++ {
		received, ok, err := channel.Receive(ctx)
		if err != nil || !ok {
			b.Fatalf("receive value=%d ok=%v err=%v", received, ok, err)
		}
		t236MailboxBenchmarkSink += received
	}
	workers.Wait()
	b.StopTimer()
	channel.Close()
}

func t236MailboxProducerRange(total, producer int) (int, int) {
	first := total * producer / 4
	last := total * (producer + 1) / 4
	return first, last
}
