package hatriecache

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

var structuredDirectBatchResponseSink *hatriecachev1.StructuredBatchResponse

func BenchmarkStructuredBatchDirect(b *testing.B) {
	for _, commands := range []int{8, 16, 64} {
		b.Run(fmt.Sprintf("Mixed%d", commands), func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			request := structuredBenchmarkRequest(1, 0, commands)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				response := trie.executeStructuredBatchDirect(context.Background(), request)
				if !response.GetOk() || len(response.GetStatuses()) != commands {
					b.Fatalf("structured response = %#v", response)
				}
				structuredDirectBatchResponseSink = response
			}
			b.ReportMetric(float64(commands), "commands/op")
		})
	}
}

func BenchmarkStructuredBatchReaderPause(b *testing.B) {
	request := structuredBenchmarkRequest(2, 0, maxPublicCommandBatchSize)
	for _, test := range []struct {
		name    string
		execute func(*HatTrie) *hatriecachev1.StructuredBatchResponse
	}{
		{name: "CommandLoop", execute: func(trie *HatTrie) *hatriecachev1.StructuredBatchResponse {
			return trie.executeStructuredBatchCommandLoop(context.Background(), request)
		}},
		{name: "Bounded2", execute: func(trie *HatTrie) *hatriecachev1.StructuredBatchResponse {
			return trie.executeStructuredBatchBoundedWithChunkSize(context.Background(), request, 2)
		}},
		{name: "Bounded4", execute: func(trie *HatTrie) *hatriecachev1.StructuredBatchResponse {
			return trie.executeStructuredBatchBoundedWithChunkSize(context.Background(), request, 4)
		}},
		{name: "Bounded8", execute: func(trie *HatTrie) *hatriecachev1.StructuredBatchResponse {
			return trie.executeStructuredBatchBoundedWithChunkSize(context.Background(), request, 8)
		}},
		{name: "Bounded16", execute: func(trie *HatTrie) *hatriecachev1.StructuredBatchResponse {
			return trie.executeStructuredBatchBoundedWithChunkSize(context.Background(), request, 16)
		}},
	} {
		b.Run(test.name, func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			trie.UpsertString("structured:reader", "value")
			var maxPause atomic.Int64
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				stop := make(chan struct{})
				ready := make(chan struct{})
				done := make(chan struct{})
				go func() {
					defer close(done)
					close(ready)
					for {
						select {
						case <-stop:
							return
						default:
						}
						started := time.Now()
						_ = trie.GetString("structured:reader")
						updateAtomicMax(&maxPause, time.Since(started).Nanoseconds())
					}
				}()
				<-ready
				response := test.execute(trie)
				close(stop)
				<-done
				if !response.GetOk() || len(response.GetStatuses()) != len(request.GetOperations()) {
					b.Fatalf("structured response = %#v", response)
				}
			}
			b.ReportMetric(float64(maxPause.Load()), "max_read_pause_ns/op")
			b.ReportMetric(float64(len(request.GetOperations())), "commands/op")
		})
	}
}

func BenchmarkStructuredBatchCommandLoopReference(b *testing.B) {
	for _, commands := range []int{8, 16, 64} {
		b.Run(fmt.Sprintf("Mixed%d", commands), func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			request := structuredBenchmarkRequest(1, 0, commands)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				response := trie.executeStructuredBatchCommandLoop(context.Background(), request)
				if !response.GetOk() || len(response.GetStatuses()) != commands {
					b.Fatalf("structured response = %#v", response)
				}
				structuredDirectBatchResponseSink = response
			}
			b.ReportMetric(float64(commands), "commands/op")
		})
	}
}
