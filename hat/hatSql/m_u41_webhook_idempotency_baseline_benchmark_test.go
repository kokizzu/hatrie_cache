//go:build mu41baseline

package hatSql

import (
	"crypto/sha256"
	"testing"
)

var mu41BaselineFingerprintSink [sha256.Size]byte

func BenchmarkMU41WebhookFingerprintBaseline(b *testing.B) {
	payload := []byte(`{"event":"payment.created","amount":10,"currency":"USD"}`)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		mu41BaselineFingerprintSink = sha256.Sum256(payload)
	}
	b.StopTimer()
	if mu41BaselineFingerprintSink == ([sha256.Size]byte{}) {
		b.Fatal("baseline fingerprint was empty")
	}
}
