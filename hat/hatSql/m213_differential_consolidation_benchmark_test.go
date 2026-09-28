package hatSql

import (
	"encoding/json"
	"testing"
)

var (
	m213ConsolidatedBatchSink  QuerySubscriptionDeltaBatch
	m213ConsolidatedDeltasSink []QuerySubscriptionDelta
)

func BenchmarkM213ConsolidateDeltaBatchJSON(b *testing.B) {
	input := m213BenchmarkBatch()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		batch, err := ConsolidateQuerySubscriptionDeltaBatch(input)
		if err != nil {
			b.Fatal(err)
		}
		payload, err := json.Marshal(batch)
		if err != nil {
			b.Fatal(err)
		}
		m213ConsolidatedBatchSink = batch
		m213RawJSONSink = payload
	}
	b.StopTimer()
	b.ReportMetric(float64(len(m213RawJSONSink)), "wire-B/op")
}

func BenchmarkM213ConsolidateQuerySubscriptionDeltas(b *testing.B) {
	input := m213BenchmarkBatch().Deltas
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		consolidated, err := ConsolidateQuerySubscriptionDeltas(input)
		if err != nil {
			b.Fatal(err)
		}
		m213ConsolidatedDeltasSink = consolidated
	}
}
