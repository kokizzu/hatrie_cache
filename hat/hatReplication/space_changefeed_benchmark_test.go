package hatReplication

import "testing"

type naiveSpaceChangefeedRecord struct {
	sequence  uint64
	operation uint8
	key       []byte
	value     []byte
}

type naiveSpaceChangefeed struct {
	records []naiveSpaceChangefeedRecord
}

func (feed *naiveSpaceChangefeed) publish(sequence uint64, operation uint8, key, value []byte) {
	if len(feed.records) >= 8192 {
		feed.records = feed.records[1:]
	}
	feed.records = append(feed.records, naiveSpaceChangefeedRecord{
		sequence:  sequence,
		operation: operation,
		key:       append([]byte(nil), key...),
		value:     append([]byte(nil), value...),
	})
}

func (feed *naiveSpaceChangefeed) readAfter(sequence uint64, limit int) []naiveSpaceChangefeedRecord {
	offset := int(sequence)
	if offset >= len(feed.records) {
		return nil
	}
	available := len(feed.records) - offset
	if available > limit {
		available = limit
	}
	changes := make([]naiveSpaceChangefeedRecord, available)
	for index := range changes {
		record := feed.records[offset+index]
		changes[index] = naiveSpaceChangefeedRecord{
			sequence:  record.sequence,
			operation: record.operation,
			key:       append([]byte(nil), record.key...),
			value:     append([]byte(nil), record.value...),
		}
	}
	return changes
}

func BenchmarkSpaceChangefeedNaivePublish(b *testing.B) {
	feed := &naiveSpaceChangefeed{records: make([]naiveSpaceChangefeedRecord, 0, 8192)}
	key := []byte("order-0000000001")
	value := []byte("status=paid;amount=1200")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		feed.publish(uint64(index+1), 1, key, value)
	}
}

func BenchmarkSpaceChangefeedPublish(b *testing.B) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:     "orders",
		MaxEvents: 8192,
		MaxBytes:  16 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	key := []byte("order-0000000001")
	value := []byte("status=paid;amount=1200")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if index > 0 && index%4096 == 0 {
			if err := feed.CompactThrough(ChangefeedCheckpoint{Source: "orders", Sequence: uint64(index)}); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := feed.Publish(SpaceChangeInsert, key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSpaceChangefeedNaiveRead(b *testing.B) {
	feed := &naiveSpaceChangefeed{records: make([]naiveSpaceChangefeedRecord, 0, 1024)}
	key := []byte("order-0000000001")
	value := []byte("status=paid;amount=1200")
	for index := 0; index < 1024; index++ {
		feed.records = append(feed.records, naiveSpaceChangefeedRecord{
			sequence:  uint64(index + 1),
			operation: 1,
			key:       append([]byte(nil), key...),
			value:     append([]byte(nil), value...),
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		benchmarkSpaceChangefeedSink = feed.readAfter(0, 64)
	}
}

func BenchmarkSpaceChangefeedRead(b *testing.B) {
	feed, err := NewSpaceChangefeed(SpaceChangefeedOptions{
		Space:     "orders",
		MaxEvents: 2048,
		MaxBytes:  16 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	key := []byte("order-0000000001")
	value := []byte("status=paid;amount=1200")
	for index := 0; index < 1024; index++ {
		if _, err := feed.Publish(SpaceChangeInsert, key, value); err != nil {
			b.Fatal(err)
		}
	}
	initial := feed.InitialCheckpoint()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		changes, _, err := feed.ReadAfter(initial, 64)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkSpaceChangefeedSink = changes
	}
}

var benchmarkSpaceChangefeedSink interface{}
