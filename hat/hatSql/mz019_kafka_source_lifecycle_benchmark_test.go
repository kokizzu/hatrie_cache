package hatSql

import "testing"

func BenchmarkMZ019KafkaSourceLifecycleToggle(b *testing.B) {
	source := benchmarkMZ019KafkaSource(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := source.Pause("maintenance"); err != nil {
			b.Fatal(err)
		}
		if _, err := source.Resume(); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkMZ019KafkaSource(b *testing.B) *KafkaTableSource {
	b.Helper()
	source, err := NewKafkaTableSource(KafkaTableSourceOptions{
		Source: "lifecycle", Table: "orders", Topic: "orders", Decoder: KafkaTableJSONDecoder,
	})
	if err != nil {
		b.Fatal(err)
	}
	return source
}
