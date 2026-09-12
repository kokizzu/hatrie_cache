package hatCache

import (
	"strings"
	"testing"
	"time"
)

func TestKeyWatcherPrefixFilterReceivesOnlyMatchingKeys(t *testing.T) {
	ht := CreateHatTrie()
	defer ht.Destroy()

	watcher, err := ht.WatchKeyWithOptions(KeyWatcherOptions{
		Prefix: "user:",
		Buffer: 8,
	})
	if err != nil {
		t.Fatalf("WatchKeyWithOptions() error = %v", err)
	}
	defer watcher.Close()

	ht.UpsertString("order:1", "ignored")
	ht.UpsertString("user:1", "alice")
	if deleted, err := ht.DeleteChecked("user:1"); err != nil || !deleted {
		t.Fatalf("DeleteChecked(user:1) = deleted=%v err=%v", deleted, err)
	}

	first := receiveFilteredKeyChangeEvent(t, watcher.Events())
	second := receiveFilteredKeyChangeEvent(t, watcher.Events())
	if first.Key != "user:1" || first.Operation != KeyChangeSet {
		t.Fatalf("first event = %#v, want user:1 set", first)
	}
	if second.Key != "user:1" || second.Operation != KeyChangeDelete {
		t.Fatalf("second event = %#v, want user:1 delete", second)
	}
	select {
	case event := <-watcher.Events():
		t.Fatalf("prefix watcher received unrelated event: %#v", event)
	default:
	}
}

func TestKeyWatcherCoalescesLatestEventPerKey(t *testing.T) {
	ht := CreateHatTrie()
	defer ht.Destroy()

	watcher, err := ht.WatchKeyWithOptions(KeyWatcherOptions{
		Key:            "user:1",
		Buffer:         8,
		Coalesce:       true,
		CoalesceWindow: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("WatchKeyWithOptions() error = %v", err)
	}
	defer watcher.Close()

	ht.UpsertString("user:1", "alice")
	if deleted, err := ht.DeleteChecked("user:1"); err != nil || !deleted {
		t.Fatalf("DeleteChecked(user:1) = deleted=%v err=%v", deleted, err)
	}

	event := receiveFilteredKeyChangeEvent(t, watcher.Events())
	if event.Key != "user:1" || event.Operation != KeyChangeDelete {
		t.Fatalf("coalesced event = %#v, want latest delete", event)
	}
	select {
	case extra := <-watcher.Events():
		t.Fatalf("coalesced watcher emitted duplicate event: %#v", extra)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestKeyWatcherCoalescesPrefixKeysInFirstSeenOrder(t *testing.T) {
	ht := CreateHatTrie()
	defer ht.Destroy()

	watcher, err := ht.WatchKeyWithOptions(KeyWatcherOptions{
		Prefix:         "user:",
		Buffer:         2,
		Coalesce:       true,
		CoalesceWindow: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("WatchKeyWithOptions() error = %v", err)
	}
	defer watcher.Close()

	ht.UpsertString("user:1", "alice")
	ht.UpsertString("user:2", "bob")
	if deleted, err := ht.DeleteChecked("user:1"); err != nil || !deleted {
		t.Fatalf("DeleteChecked(user:1) = deleted=%v err=%v", deleted, err)
	}

	first := receiveFilteredKeyChangeEvent(t, watcher.Events())
	second := receiveFilteredKeyChangeEvent(t, watcher.Events())
	if first.Key != "user:1" || first.Operation != KeyChangeDelete {
		t.Fatalf("first coalesced event = %#v, want user:1 delete", first)
	}
	if second.Key != "user:2" || second.Operation != KeyChangeSet {
		t.Fatalf("second coalesced event = %#v, want user:2 set", second)
	}
}

func TestKeyWatcherCoalescedClosesWhenTrieIsDestroyed(t *testing.T) {
	ht := CreateHatTrie()
	watcher, err := ht.WatchKeyWithOptions(KeyWatcherOptions{
		Key:            "user:1",
		Coalesce:       true,
		CoalesceWindow: time.Second,
	})
	if err != nil {
		t.Fatalf("WatchKeyWithOptions() error = %v", err)
	}
	ht.UpsertString("user:1", "alice")
	ht.Destroy()
	if _, ok := <-watcher.Events(); ok {
		t.Fatal("destroyed coalesced watcher yielded an event")
	}
	watcher.Close()
}

func TestKeyWatcherOptionsRejectAmbiguousFilters(t *testing.T) {
	ht := CreateHatTrie()
	defer ht.Destroy()

	invalid := []KeyWatcherOptions{
		{},
		{Key: "user:1", Prefix: "user:"},
		{Prefix: ""},
		{Key: "user:1", Buffer: -1},
		{Key: "user:1", CoalesceWindow: -time.Millisecond},
		{Key: strings.Repeat("k", maxHATTrieKeyLength+1)},
	}
	for index, options := range invalid {
		if _, err := ht.WatchKeyWithOptions(options); err == nil {
			t.Fatalf("invalid options %d accepted: %#v", index, options)
		}
	}
}

func receiveFilteredKeyChangeEvent(t *testing.T, events <-chan KeyChangeEvent) KeyChangeEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for filtered key change event")
		return KeyChangeEvent{}
	}
}

func BenchmarkKeyWatcherOptionWritePath(b *testing.B) {
	cases := []struct {
		name    string
		options KeyWatcherOptions
	}{
		{name: "Exact", options: KeyWatcherOptions{Key: "user:1", Buffer: 1024}},
		{name: "Prefix", options: KeyWatcherOptions{Prefix: "user:", Buffer: 1024}},
		{name: "Coalesced", options: KeyWatcherOptions{Key: "user:1", Buffer: 1024, Coalesce: true, CoalesceWindow: time.Nanosecond}},
	}
	for _, test := range cases {
		b.Run(test.name, func(b *testing.B) {
			ht := CreateHatTrie()
			defer ht.Destroy()
			watcher, err := ht.WatchKeyWithOptions(test.options)
			if err != nil {
				b.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				for range watcher.Events() {
				}
				close(done)
			}()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ht.UpsertString("user:1", "value")
			}
			b.StopTimer()
			watcher.Close()
			<-done
		})
	}
}
