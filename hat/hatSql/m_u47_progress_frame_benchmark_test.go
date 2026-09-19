//go:build mu47

package hatSql

import "testing"

func BenchmarkQuerySubscriptionProgressFrameEncode(b *testing.B) {
	batch := QuerySubscriptionDeltaBatch{ID: 7, Revision: 42, Frontier: 9001, Progress: true, Complete: true}
	payload, err := EncodeQuerySubscriptionProgressFrame(batch)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := EncodeQuerySubscriptionProgressFrame(batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuerySubscriptionProgressFrameDecode(b *testing.B) {
	payload, err := EncodeQuerySubscriptionProgressFrame(QuerySubscriptionDeltaBatch{ID: 7, Revision: 42, Frontier: 9001, Progress: true, Complete: true})
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := DecodeQuerySubscriptionProgressFrame(payload); err != nil {
			b.Fatal(err)
		}
	}
}
