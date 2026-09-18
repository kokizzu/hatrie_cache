package hatSql

import (
	"strconv"
	"testing"
)

type ch046BenchmarkRecord struct {
	key       string
	partition string
	offset    uint64
	row       Row
}

func ch046BenchmarkRecords() []ch046BenchmarkRecord {
	records := make([]ch046BenchmarkRecord, 10000)
	for index := range records {
		records[index] = ch046BenchmarkRecord{
			key:       "key-" + strconv.Itoa(index),
			partition: "0",
			offset:    uint64(index),
			row:       Row{"id": index, "value": index * 3},
		}
	}
	return records
}

var ch046BenchmarkSink int

// BenchmarkKafkaTableSourceBaseline measures a naïve adapter that advances
// the checkpoint tracker once per record instead of once per source batch.
func BenchmarkKafkaTableSourceBaseline(b *testing.B) {
	records := ch046BenchmarkRecords()
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		rows := make(map[string]Row, len(records))
		tracker := NewSQLSourceOffsetTracker()
		for _, record := range records {
			advanced, err := tracker.Advance(SQLSourceOffset{
				Source:    "benchmark",
				Partition: record.partition,
				Offset:    record.offset,
			})
			if err != nil {
				b.Fatal(err)
			}
			if advanced {
				row := make(Row, len(record.row))
				for key, value := range record.row {
					row[key] = value
				}
				rows[record.key] = row
			}
		}
		ch046BenchmarkSink = len(rows) + len(tracker.Snapshot())
	}
}

// BenchmarkKafkaTableSourceBatched measures one source transaction using one
// batched checkpoint advance for all 10,000 records.
func BenchmarkKafkaTableSourceBatched(b *testing.B) {
	records := ch046BenchmarkRecords()
	messages := make([]KafkaTableMessage, len(records))
	rows := make(map[string]Row, len(records))
	for index, record := range records {
		messages[index] = KafkaTableMessage{
			Topic:     "benchmark",
			Partition: record.partition,
			Offset:    record.offset,
			Key:       record.key,
		}
		rows[record.key] = record.row
	}
	decoder := func(message KafkaTableMessage) (KafkaTableChange, error) {
		return KafkaTableChange{Key: message.Key, Operation: KafkaTableUpsert, Row: rows[message.Key]}, nil
	}
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		source, err := NewKafkaTableSource(KafkaTableSourceOptions{
			Source:  "benchmark-source",
			Table:   "benchmark",
			Topic:   "benchmark",
			Decoder: decoder,
		})
		if err != nil {
			b.Fatal(err)
		}
		result, err := source.ApplyBatch(KafkaTableBatch{TransactionID: "benchmark-tx", Messages: messages})
		if err != nil {
			b.Fatal(err)
		}
		ch046BenchmarkSink = result.AppliedMessages + source.Stats().Rows
	}
}
