package hatReplication

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	"hatrie_cache/hat/hatCommand"
	"hatrie_cache/hat/hatJournal"
)

type t042ReplayBenchmarkSink struct {
	value uint64
	pad   [7]uint64
}

var t042ReplayBenchmarkSinks [64]t042ReplayBenchmarkSink

func t042ReplayBenchmarkRecords() []hatJournal.Record {
	records := make([]hatJournal.Record, 10000)
	for index := range records {
		records[index] = hatJournal.Record{
			Sequence: uint64(index + 1),
			Request: hatCommand.Request{
				Command: "SET",
				Key:     "key-" + strconv.Itoa(index%64),
				Value:   "value",
			},
		}
	}
	return records
}

func t042ReplayBenchmarkWork(record hatJournal.Record) {
	key := record.Request.Key
	value := record.Sequence
	for iteration := 0; iteration < 128; iteration++ {
		value ^= uint64(key[iteration%len(key)])
		value *= 1099511628211
	}
	atomic.AddUint64(&t042ReplayBenchmarkSinks[(record.Sequence-1)%64].value, value)
}

func t042ReplayBenchmarkSerial(records []hatJournal.Record) {
	for _, record := range records {
		t042ReplayBenchmarkWork(record)
	}
}

func BenchmarkT042SerialReplay(b *testing.B) {
	records := t042ReplayBenchmarkRecords()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		t042ReplayBenchmarkSerial(records)
	}
}

func BenchmarkT042ParallelReplay(b *testing.B) {
	records := t042ReplayBenchmarkRecords()
	options := ParallelReplayOptions{
		Workers: 8,
		Key: func(record hatJournal.Record) string {
			return record.Request.Key
		},
		Apply: func(ctx context.Context, record hatJournal.Record) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			t042ReplayBenchmarkWork(record)
			return nil
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := ReplayJournalRecordsParallel(context.Background(), records, options); err != nil {
			b.Fatal(err)
		}
	}
}
