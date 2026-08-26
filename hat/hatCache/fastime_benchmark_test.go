package hatCache

import (
	"strings"
	"testing"
	"time"

	"github.com/kpango/fastime"
)

var (
	clockTimeSink     time.Time
	clockDurationSink time.Duration
	clockUnixSink     int64
)

func TestFastimeClockDocumentsApproximateSemantics(t *testing.T) {
	if !fastime.IsDaemonRunning() {
		t.Fatal("fastime global refresh daemon is not running")
	}

	standard := time.Now()
	approximate := fastime.Now()
	if !strings.Contains(standard.String(), "m=+") {
		t.Fatal("time.Now() did not expose the expected monotonic component")
	}
	if strings.Contains(approximate.String(), "m=+") {
		t.Fatal("fastime.Now() unexpectedly retained a monotonic component")
	}

	foundCachedRead := false
	previous := approximate
	for read := 0; read < 1_000; read++ {
		current := fastime.Now()
		if current.Equal(previous) {
			foundCachedRead = true
			break
		}
		previous = current
	}
	if !foundCachedRead {
		t.Fatal("fastime did not reuse a timestamp across immediate reads")
	}

	deadline := time.Now().Add(time.Second)
	for fastime.Now().Equal(approximate) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if fastime.Now().Equal(approximate) {
		t.Fatal("fastime did not refresh within 1s")
	}
}

func BenchmarkClockSource(b *testing.B) {
	b.Run("TimeNow", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			clockTimeSink = time.Now()
		}
	})
	b.Run("FastimeNow", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			clockTimeSink = fastime.Now()
		}
	})
	b.Run("TimeUnixNano", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			clockUnixSink = time.Now().UnixNano()
		}
	})
	b.Run("FastimeUnixNano", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			clockUnixSink = fastime.UnixNanoNow()
		}
	})
	b.Run("TimeSince", func(b *testing.B) {
		started := time.Now()
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			clockDurationSink = time.Since(started)
		}
	})
	b.Run("FastimeSince", func(b *testing.B) {
		started := fastime.Now()
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			clockDurationSink = fastime.Since(started)
		}
	})
}

func BenchmarkTrieClockSource(b *testing.B) {
	clocks := []struct {
		name     string
		override func() time.Time
	}{
		{name: "Default"},
		{name: "TimeNow", override: time.Now},
	}
	operations := []struct {
		name    string
		setup   func(*HatTrie)
		request CacheCommandRequest
	}{
		{
			name:    "StringSet",
			request: CacheCommandRequest{Command: "SETSTR", Key: "clock:string", Value: "value"},
		},
		{
			name: "StringGet",
			setup: func(trie *HatTrie) {
				trie.UpsertString("clock:string", "value")
			},
			request: CacheCommandRequest{Command: "GET", Key: "clock:string"},
		},
		{
			name: "CounterInc",
			setup: func(trie *HatTrie) {
				trie.UpsertCounter("clock:counter", 0)
			},
			request: CacheCommandRequest{Command: "INC", Key: "clock:counter", Value: "1"},
		},
		{
			name: "TTLRefresh",
			setup: func(trie *HatTrie) {
				trie.UpsertString("clock:ttl", "value")
			},
			request: func() CacheCommandRequest {
				ttl := int64(3600)
				return CacheCommandRequest{Command: "EXPIRE", Key: "clock:ttl", TTLSeconds: &ttl}
			}(),
		},
	}

	for _, operation := range operations {
		b.Run(operation.name, func(b *testing.B) {
			for _, clock := range clocks {
				b.Run(clock.name, func(b *testing.B) {
					trie := CreateHatTrie()
					b.Cleanup(trie.Destroy)
					if clock.override != nil {
						trie.now = clock.override
					}
					if operation.setup != nil {
						operation.setup(trie)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for iteration := 0; iteration < b.N; iteration++ {
						benchmarkCommandResponseSink = trie.ExecuteCommand(operation.request)
					}
				})
			}
		})
	}
}
