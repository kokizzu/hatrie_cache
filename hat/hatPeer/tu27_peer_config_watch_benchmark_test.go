package hatPeer

import (
	"context"
	"testing"
)

func BenchmarkTU27DirectDispatch(b *testing.B) {
	event := PeerConfigWatchEvent{Cursor: 4, Key: "config/db/enabled", Value: []byte("enabled")}
	handler := func(PeerConfigWatchEvent) {}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		copyEvent := event
		copyEvent.Value = append([]byte(nil), event.Value...)
		handler(copyEvent)
	}
}

func BenchmarkTU27PeerWatcherApplyBatch(b *testing.B) {
	watcher := &PeerConfigWatcher{
		prefix:    "config/db/",
		batchSize: 64,
		handler:   func(context.Context, PeerConfigWatchEvent) error { return nil },
	}
	batch := PeerConfigWatchBatch{
		Events:     []PeerConfigWatchEvent{{Cursor: 4, Key: "config/db/enabled", Value: []byte("enabled")}},
		NextCursor: 4,
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		watcher.cursor = 0
		if err := watcher.applyBatch(context.Background(), batch); err != nil {
			b.Fatal(err)
		}
	}
}
