//go:build mu47baseline

package hatSql

import (
	"encoding/json"
	"testing"
)

func BenchmarkQuerySubscriptionProgressFrameBaselineJSONEncode(b *testing.B) {
	batch := QuerySubscriptionDeltaBatch{ID: 7, Revision: 42, Frontier: 9001, Progress: true, Complete: true}
	payload, err := json.Marshal(batch)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := json.Marshal(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuerySubscriptionProgressFrameBaselineJSONDecode(b *testing.B) {
	payload, err := json.Marshal(QuerySubscriptionDeltaBatch{ID: 7, Revision: 42, Frontier: 9001, Progress: true, Complete: true})
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var batch QuerySubscriptionDeltaBatch
		if err := json.Unmarshal(payload, &batch); err != nil {
			b.Fatal(err)
		}
	}
}
