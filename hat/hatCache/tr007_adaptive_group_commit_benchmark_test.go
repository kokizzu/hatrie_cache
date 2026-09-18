//go:build !tr007baseline

package hatCache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkTR007GroupCommitFixed(b *testing.B) {
	benchmarkTR007GroupCommit(b, false)
}

func BenchmarkTR007GroupCommitAdaptive(b *testing.B) {
	benchmarkTR007GroupCommit(b, true)
}

func benchmarkTR007GroupCommit(b *testing.B, adaptive bool) {
	journal, err := OpenCommandJournalWithOptions(b.TempDir()+"/commands.journal", CommandJournalOptions{
		GroupCommitWindow:   2 * time.Millisecond,
		GroupCommitMaxBatch: 64,
		AdaptiveGroupCommit: adaptive,
	})
	if err != nil {
		b.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()
	var syncs atomic.Int64
	journal.mu.Lock()
	journal.syncHook = func() error {
		syncs.Add(1)
		return nil
	}
	journal.mu.Unlock()
	trie := CreateHatTrie()
	defer trie.Destroy()
	requests := make([]CacheCommandRequest, 16)
	for index := range requests {
		requests[index] = CacheCommandRequest{Command: "SETSTR", Key: "tr007:key", Value: "value"}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		start := make(chan struct{})
		var wait sync.WaitGroup
		wait.Add(len(requests))
		for index := range requests {
			go func(index int) {
				defer wait.Done()
				<-start
				if response := journal.ExecuteCommand(trie, requests[index]); !response.OK {
					b.Errorf("ExecuteCommand() = %#v", response)
				}
			}(index)
		}
		close(start)
		wait.Wait()
	}
	b.StopTimer()
	b.ReportMetric(float64(syncs.Load())/float64(b.N), "syncs/round")
}
