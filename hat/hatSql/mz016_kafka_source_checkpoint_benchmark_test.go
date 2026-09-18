package hatSql

import (
	"context"
	"testing"
)

type mz016BenchmarkCheckpointStore struct{}

func (mz016BenchmarkCheckpointStore) Load(context.Context, string) (KafkaTableSourceSnapshot, bool, error) {
	return KafkaTableSourceSnapshot{}, false, nil
}

func (mz016BenchmarkCheckpointStore) Commit(context.Context, KafkaTableSourceSnapshot) error {
	return nil
}

func BenchmarkMZ016KafkaSourceBatchBaseline(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		source := benchmarkMZ016KafkaSource(b)
		if _, err := source.ApplyBatch(mz016Batch(uint64(index+1), "tx")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ016KafkaSourceBatchWithCheckpoint(b *testing.B) {
	store := mz016BenchmarkCheckpointStore{}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		source := benchmarkMZ016KafkaSource(b)
		if _, err := source.ApplyBatchWithCheckpoint(context.Background(), mz016Batch(uint64(index+1), "tx"), store); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkMZ016KafkaSource(b *testing.B) *KafkaTableSource {
	b.Helper()
	source, err := NewKafkaTableSource(KafkaTableSourceOptions{
		Source: "checkpointed", Table: "orders", Topic: "orders", Decoder: KafkaTableJSONDecoder,
	})
	if err != nil {
		b.Fatal(err)
	}
	return source
}
