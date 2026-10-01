package hatReplication

import "testing"

var spaceChangefeedBenchmarkSink SpaceChange

func BenchmarkSpaceChangefeedCommitBaseline(b *testing.B) {
	var sequence uint64
	var ring [1024]SpaceChange
	change := SpaceChange{Sequence: 1, SchemaVersion: 1, Operation: SpaceChangeInsert, Key: []byte("id"), After: []byte("row")}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		change.Sequence = sequence + 1
		sequence++
		ring[index%len(ring)] = change
	}
	spaceChangefeedBenchmarkSink = ring[0]
}

func BenchmarkSpaceChangefeedAppend(b *testing.B) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: 1, Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	change := SpaceChange{SchemaVersion: 1, Operation: SpaceChangeInsert, Key: []byte("id"), After: []byte("row")}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		appended, err := feed.Append(change)
		if err != nil {
			b.Fatal(err)
		}
		spaceChangefeedBenchmarkSink = appended
	}
}

func BenchmarkSpaceChangefeedAppendBatch(b *testing.B) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: 1, Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	batch := make([]SpaceChange, 32)
	for index := range batch {
		batch[index] = SpaceChange{SchemaVersion: 1, Operation: SpaceChangeInsert, Key: []byte("id"), After: []byte("row")}
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		appended, err := feed.AppendBatch(batch)
		if err != nil {
			b.Fatal(err)
		}
		spaceChangefeedBenchmarkSink = appended[len(appended)-1]
	}
}

func BenchmarkSpaceChangefeedReadAfter(b *testing.B) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: 1, Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		if _, err := feed.Append(SpaceChange{SchemaVersion: 1, Operation: SpaceChangeInsert, Key: []byte("id"), After: []byte("row")}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		page, err := feed.ReadAfter(512, 16)
		if err != nil {
			b.Fatal(err)
		}
		spaceChangefeedBenchmarkSink = page.Events[0]
	}
}

func BenchmarkSpaceChangefeedCheckpoint(b *testing.B) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{Space: "orders", SchemaVersion: 1})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := feed.Append(SpaceChange{SchemaVersion: 1, Operation: SpaceChangeInsert, Key: []byte("id"), After: []byte("row")}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		checkpoint, err := feed.Checkpoint()
		if err != nil {
			b.Fatal(err)
		}
		spaceChangefeedBenchmarkSink.Sequence = checkpoint.Sequence
	}
}
