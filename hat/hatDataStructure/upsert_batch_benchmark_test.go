package hatDataStructure

import "testing"

type mz014BaselineUpsertEvent struct {
	key     string
	value   int64
	deleted bool
}

var mz014BenchmarkChecksum int64

func mz014BaselineUpsertEvents() []mz014BaselineUpsertEvent {
	events := make([]mz014BaselineUpsertEvent, 0, 10000)
	for index := 0; index < 10000; index++ {
		keyIndex := index % 1000
		events = append(events, mz014BaselineUpsertEvent{
			key:     "key-" + string(rune('a'+keyIndex%26)) + string(rune('a'+keyIndex/26)),
			value:   int64(index),
			deleted: index%7 == 0,
		})
	}
	return events
}

func BenchmarkMZ014BaselineMapLastWrite(b *testing.B) {
	events := mz014BaselineUpsertEvents()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		state := make(map[string]mz014BaselineUpsertEvent, 1000)
		for _, event := range events {
			state[event.key] = event
		}
		if len(state) != 1000 {
			b.Fatalf("state length = %d, want 1000", len(state))
		}
		var checksum int64
		for _, event := range state {
			if event.deleted {
				checksum--
				continue
			}
			checksum += event.value
		}
		mz014BenchmarkChecksum = checksum
	}
}

func BenchmarkMZ014BaselineMapLastWriteReuse(b *testing.B) {
	events := mz014BaselineUpsertEvents()
	state := make(map[string]mz014BaselineUpsertEvent, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for _, event := range events {
			state[event.key] = event
		}
		if len(state) != 1000 {
			b.Fatalf("state length = %d, want 1000", len(state))
		}
		var checksum int64
		for _, event := range state {
			if event.deleted {
				checksum--
				continue
			}
			checksum += event.value
		}
		mz014BenchmarkChecksum = checksum
	}
}

func BenchmarkMZ014UpsertBatchLastWrite(b *testing.B) {
	events := mz014BaselineUpsertEvents()
	batch := NewUpsertBatch[int64](1000)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		batch.Reset()
		for _, event := range events {
			var err error
			if event.deleted {
				err = batch.Delete(event.key)
			} else {
				err = batch.Upsert(event.key, event.value)
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		if batch.Len() != 1000 {
			b.Fatalf("batch length = %d, want 1000", batch.Len())
		}
		var checksum int64
		batch.ForEach(func(record UpsertRecord[int64]) {
			if record.Deleted {
				checksum--
				return
			}
			checksum += record.Value
		})
		mz014BenchmarkChecksum = checksum
	}
}

func BenchmarkMZ014UpsertBatchFresh(b *testing.B) {
	events := mz014BaselineUpsertEvents()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		batch := NewUpsertBatch[int64](1000)
		for _, event := range events {
			var err error
			if event.deleted {
				err = batch.Delete(event.key)
			} else {
				err = batch.Upsert(event.key, event.value)
			}
			if err != nil {
				b.Fatal(err)
			}
		}
		if batch.Len() != 1000 {
			b.Fatalf("batch length = %d, want 1000", batch.Len())
		}
		var checksum int64
		batch.ForEach(func(record UpsertRecord[int64]) {
			if record.Deleted {
				checksum--
				return
			}
			checksum += record.Value
		})
		mz014BenchmarkChecksum = checksum
	}
}
