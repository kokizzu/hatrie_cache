package hatCache

import (
	"context"
	"testing"
	"time"
)

func TestExpirationCleanerUsesExistingDeadline(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("deadline:existing", "value")
	if !trie.Expire("deadline:existing", 25*time.Millisecond) {
		t.Fatal("Expire(deadline:existing) = false, want true")
	}

	stop := trie.StartExpirationCleanerContext(context.Background(), time.Second)
	defer stop()

	waitForExpirationCleanerSize(t, trie, 0, 400*time.Millisecond)
}

func TestExpirationCleanerWakesForEarlierDeadline(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("deadline:later", "value")
	if !trie.Expire("deadline:later", time.Hour) {
		t.Fatal("Expire(deadline:later) = false, want true")
	}
	trie.UpsertString("deadline:early", "value")
	stop := trie.StartExpirationCleanerContext(context.Background(), time.Second)
	defer stop()

	if !trie.Expire("deadline:early", 25*time.Millisecond) {
		t.Fatal("Expire(deadline:early) = false, want true")
	}

	waitForExpirationCleanerSize(t, trie, 1, 400*time.Millisecond)
}

func TestExpirationCleanerWakesPartitionedChild(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureLocalPartitions(2); err != nil {
		t.Fatalf("ConfigureLocalPartitions(2) error = %v", err)
	}
	trie.UpsertString("deadline:partitioned", "value")
	stop := trie.StartExpirationCleanerContext(context.Background(), time.Second)
	defer stop()

	if !trie.Expire("deadline:partitioned", 25*time.Millisecond) {
		t.Fatal("Expire(deadline:partitioned) = false, want true")
	}

	child := trie.localPartitionForKey("deadline:partitioned")
	waitForExpirationCleanerSize(t, child, 0, 400*time.Millisecond)
}

func waitForExpirationCleanerSize(t *testing.T, trie *HatTrie, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		trie.mu.RLock()
		got := trie.sizeLocked()
		trie.mu.RUnlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("trie size = %d, want %d before %s", got, want, timeout)
		}
		time.Sleep(time.Millisecond)
	}
}

func BenchmarkExpirationCleanerDeadlineScheduling(b *testing.B) {
	fixedNow := time.Unix(1_700_000_000, 0)
	for _, test := range []struct {
		name       string
		start      bool
		partitions int
	}{
		{name: "without_cleaner"},
		{name: "with_cleaner", start: true},
		{name: "with_partitioned_cleaner", start: true, partitions: 2},
	} {
		b.Run(test.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			trie.now = func() time.Time { return fixedNow }
			if test.partitions != 0 {
				if err := trie.ConfigureLocalPartitions(test.partitions); err != nil {
					b.Fatalf("ConfigureLocalPartitions(%d) error = %v", test.partitions, err)
				}
			}
			trie.UpsertString("deadline:benchmark", "value")
			if test.start {
				stop := trie.StartExpirationCleanerContext(context.Background(), time.Hour)
				defer stop()
			}
			if !trie.ExpireAt("deadline:benchmark", fixedNow.Add(time.Hour)) {
				b.Fatal("ExpireAt(deadline:benchmark) = false, want true")
			}

			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				at := fixedNow.Add(time.Hour + time.Duration(index+1))
				if !trie.ExpireAt("deadline:benchmark", at) {
					b.Fatal("ExpireAt(deadline:benchmark) = false, want true")
				}
			}
		})
	}
}
