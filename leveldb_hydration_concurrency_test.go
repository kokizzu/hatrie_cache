package hatriecache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockingReferenceStore struct {
	entry       snapshotEntry
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	calls       atomic.Int64
}

func (store *blockingReferenceStore) Entry(string) (snapshotEntry, bool, error) {
	store.calls.Add(1)
	store.startedOnce.Do(func() { close(store.started) })
	<-store.release
	return store.entry, true, nil
}

func (store *blockingReferenceStore) entryData(string) ([]byte, bool, error) {
	return nil, false, nil
}

func newBlockingColdReference(t *testing.T, key string, value string) (*HatTrie, *blockingReferenceStore) {
	t.Helper()
	store := &blockingReferenceStore{
		entry:   snapshotEntry{Key: key, Type: "string", String: value},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	trie := newTestTrie(t)
	trie.mu.Lock()
	_, err := trie.applyLevelDBReferenceLocked(store, store.entry, []byte("record"))
	trie.mu.Unlock()
	if err != nil {
		t.Fatalf("applyLevelDBReferenceLocked() error = %v", err)
	}
	return trie, store
}

func TestColdReferenceHydrationDoesNotBlockUnrelatedWrite(t *testing.T) {
	trie, store := newBlockingColdReference(t, "cold", "disk-value")
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		_, _, _ = trie.GetStringChecked("cold")
	}()
	<-store.started

	writeDone := make(chan struct{})
	go func() {
		trie.UpsertString("hot", "new")
		close(writeDone)
	}()
	select {
	case <-writeDone:
	case <-time.After(500 * time.Millisecond):
		close(store.release)
		<-readDone
		t.Fatal("unrelated write blocked behind cold-reference I/O")
	}
	close(store.release)
	<-readDone
	if got := trie.GetString("hot"); got != "new" {
		t.Fatalf("hot value = %q, want new", got)
	}
}

func TestColdReferenceHydrationDoesNotOverwriteConcurrentUpdate(t *testing.T) {
	trie, store := newBlockingColdReference(t, "cold", "disk-value")
	result := make(chan string, 1)
	go func() {
		value, _, _ := trie.GetStringChecked("cold")
		result <- value
	}()
	<-store.started

	updated := make(chan struct{})
	go func() {
		trie.UpsertString("cold", "new-value")
		close(updated)
	}()
	select {
	case <-updated:
	case <-time.After(500 * time.Millisecond):
		close(store.release)
		<-result
		t.Fatal("same-key update blocked behind cold-reference I/O")
	}
	close(store.release)
	if got := <-result; got != "new-value" {
		t.Fatalf("concurrent read result = %q, want new-value", got)
	}
	if got := trie.GetString("cold"); got != "new-value" {
		t.Fatalf("cold value after hydration race = %q, want new-value", got)
	}
}

func TestColdReferenceHydrationDoesNotRestoreConcurrentDelete(t *testing.T) {
	trie, store := newBlockingColdReference(t, "cold", "disk-value")
	result := make(chan string, 1)
	go func() {
		value, _, _ := trie.GetStringChecked("cold")
		result <- value
	}()
	<-store.started

	deleted := make(chan bool, 1)
	go func() { deleted <- trie.Delete("cold") }()
	select {
	case ok := <-deleted:
		if !ok {
			t.Fatal("Delete(cold) = false, want true")
		}
	case <-time.After(500 * time.Millisecond):
		close(store.release)
		<-result
		t.Fatal("same-key delete blocked behind cold-reference I/O")
	}
	close(store.release)
	if got := <-result; got != "" {
		t.Fatalf("concurrent read result = %q, want empty after delete", got)
	}
	if trie.Exists("cold") {
		t.Fatal("cold key restored by stale hydration after delete")
	}
}

func TestColdReferenceConcurrentReadsDeduplicateBackendIO(t *testing.T) {
	trie, store := newBlockingColdReference(t, "cold", "disk-value")
	const readers = 32
	start := make(chan struct{})
	results := make(chan string, readers)
	for index := 0; index < readers; index++ {
		go func() {
			<-start
			value, _, _ := trie.GetStringChecked("cold")
			results <- value
		}()
	}
	close(start)
	<-store.started
	time.Sleep(10 * time.Millisecond)
	close(store.release)
	for index := 0; index < readers; index++ {
		if got := <-results; got != "disk-value" {
			t.Fatalf("reader %d value = %q, want disk-value", index, got)
		}
	}
	if got := store.calls.Load(); got != 1 {
		t.Fatalf("backend Entry calls = %d, want 1 for %d readers", got, readers)
	}
}

type delayedReferenceStore struct {
	entries map[string]snapshotEntry
	delay   time.Duration
}

func (store *delayedReferenceStore) Entry(key string) (snapshotEntry, bool, error) {
	time.Sleep(store.delay)
	entry, ok := store.entries[key]
	return entry, ok, nil
}

func (store *delayedReferenceStore) entryData(string) ([]byte, bool, error) {
	return nil, false, nil
}

func BenchmarkColdReferenceParallelHydration32(b *testing.B) {
	const keys = 32
	for _, mode := range []string{"Serialized", "Parallel"} {
		b.Run(mode, func(b *testing.B) {
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				entries := make(map[string]snapshotEntry, keys)
				store := &delayedReferenceStore{entries: entries, delay: 250 * time.Microsecond}
				trie := CreateHatTrie()
				trie.mu.Lock()
				for index := 0; index < keys; index++ {
					key := fmt.Sprintf("key:%02d", index)
					entry := snapshotEntry{Key: key, Type: "string", String: "value"}
					entries[key] = entry
					if _, err := trie.applyLevelDBReferenceLocked(store, entry, []byte("record")); err != nil {
						b.Fatalf("applyLevelDBReferenceLocked() error = %v", err)
					}
				}
				trie.mu.Unlock()
				b.StartTimer()
				if mode == "Serialized" {
					for index := 0; index < keys; index++ {
						trie.GetString(fmt.Sprintf("key:%02d", index))
					}
				} else {
					var wait sync.WaitGroup
					wait.Add(keys)
					for index := 0; index < keys; index++ {
						go func(index int) {
							defer wait.Done()
							trie.GetString(fmt.Sprintf("key:%02d", index))
						}(index)
					}
					wait.Wait()
				}
				b.StopTimer()
				trie.Destroy()
				b.StartTimer()
			}
		})
	}
}
