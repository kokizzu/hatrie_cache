package hatSql

import (
	"encoding/json"
	"testing"
)

var mz013DebeziumKafkaDecoderBenchmarkSink KafkaTableChange

func BenchmarkMZ013DebeziumKafkaDecoderBaseline(b *testing.B) {
	message := KafkaTableMessage{
		Key:   "customer-7",
		Value: []byte(`{"before":{"id":7,"name":"Ada"},"after":{"id":7,"name":"Grace"},"op":"u"}`),
	}
	type envelope struct {
		Before Row    `json:"before"`
		After  Row    `json:"after"`
		Op     string `json:"op"`
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var decoded envelope
		if err := json.Unmarshal(message.Value, &decoded); err != nil {
			b.Fatal(err)
		}
		mz013DebeziumKafkaDecoderBenchmarkSink = KafkaTableChange{Key: message.Key, Operation: KafkaTableUpsert, Row: decoded.After}
	}
}

func BenchmarkMZ013DebeziumKafkaDecoder(b *testing.B) {
	decoder, err := NewDebeziumKafkaTableDecoder(DebeziumKafkaTableDecoderOptions{})
	if err != nil {
		b.Fatal(err)
	}
	message := KafkaTableMessage{
		Key:   "customer-7",
		Value: []byte(`{"before":{"id":7,"name":"Ada"},"after":{"id":7,"name":"Grace"},"op":"u"}`),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		change, err := decoder(message)
		if err != nil {
			b.Fatal(err)
		}
		mz013DebeziumKafkaDecoderBenchmarkSink = change
	}
}
