package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkM206DebeziumBaseline(b *testing.B) {
	initial := QuerySubscriptionDeltaBatch{
		ID:       1,
		Revision: 1,
		Frontier: 1,
		Deltas: []QuerySubscriptionDelta{{
			Row:  Row{"id": int64(1), "name": "name-0", "region": "sg"},
			Diff: 1,
		}},
	}
	batches := m206BenchmarkUpdateBatches()
	feed, err := NewDebeziumChangefeed(DebeziumChangefeedOptions{KeyColumns: []string{"id"}})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := feed.Apply(initial); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := feed.Apply(batches[index%len(batches)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM206UpsertChangefeed(b *testing.B) {
	initial := QuerySubscriptionDeltaBatch{
		ID:       1,
		Revision: 1,
		Frontier: 1,
		Deltas: []QuerySubscriptionDelta{{
			Row:  Row{"id": int64(1), "name": "name-0", "region": "sg"},
			Diff: 1,
		}},
	}
	batches := m206BenchmarkUpdateBatches()
	feed, err := NewUpsertChangefeed(UpsertChangefeedOptions{KeyColumns: []string{"id"}})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := feed.Apply(initial); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := feed.Apply(batches[index%len(batches)]); err != nil {
			b.Fatal(err)
		}
	}
}

func m206BenchmarkUpdateBatches() []QuerySubscriptionDeltaBatch {
	batches := make([]QuerySubscriptionDeltaBatch, 64)
	for index := range batches {
		batches[index] = QuerySubscriptionDeltaBatch{
			ID:       uint64(index + 2),
			Revision: uint64(index + 2),
			Frontier: uint64(index + 2),
			Deltas: []QuerySubscriptionDelta{
				{Row: Row{"id": int64(1), "name": "name-" + strconv.Itoa(index), "region": "sg"}, Diff: -1},
				{Row: Row{"id": int64(1), "name": "name-" + strconv.Itoa(index+1), "region": "sg"}, Diff: 1},
			},
		}
	}
	return batches
}
