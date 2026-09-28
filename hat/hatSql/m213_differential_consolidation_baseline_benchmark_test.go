package hatSql

import (
	"encoding/json"
	"testing"
)

var m213RawJSONSink []byte

func m213BenchmarkBatch() QuerySubscriptionDeltaBatch {
	const (
		groups            = 128
		updatesPerGroup   = 32
		payloadByteLength = 16
	)

	batch := QuerySubscriptionDeltaBatch{
		ID:       7,
		Revision: 42,
		Frontier: 9001,
		Columns:  []string{"id", "region", "payload"},
		Deltas:   make([]QuerySubscriptionDelta, 0, groups*updatesPerGroup),
	}
	for group := 0; group < groups; group++ {
		row := Row{
			"id":      int64(group),
			"region":  "region-" + string(rune('a'+group%8)),
			"payload": make([]byte, payloadByteLength),
		}
		for update := 0; update < updatesPerGroup; update++ {
			diff := int64(1)
			if update == updatesPerGroup-1 {
				diff = -1
			}
			batch.Deltas = append(batch.Deltas, QuerySubscriptionDelta{Row: row, Diff: diff})
		}
	}
	return batch
}

func BenchmarkM213RawDeltaBatchJSON(b *testing.B) {
	batch := m213BenchmarkBatch()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := json.Marshal(batch)
		if err != nil {
			b.Fatal(err)
		}
		m213RawJSONSink = payload
	}
	b.StopTimer()
	b.ReportMetric(float64(len(m213RawJSONSink)), "wire-B/op")
}
