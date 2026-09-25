package hatSql

import "testing"

func BenchmarkMZ010SubscriptionWireKeyringOpenActive(b *testing.B) {
	ring, err := NewSQLSubscriptionWireKeyring([]byte("subscription-key-v2"))
	if err != nil {
		b.Fatal(err)
	}
	wire, err := ring.Seal(SQLSubscriptionWireEnvelope{
		Mode:           SQLSubscriptionModeDifferential,
		SubscriptionID: "people-live",
		Sequence:       42,
		Diff:           1,
		Payload:        []byte(`{"id":7,"name":"Ada","active":true,"score":123.5}`),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for range b.N {
		if _, err := ring.Open(wire); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ010SubscriptionWireKeyringOpenPrevious(b *testing.B) {
	ring, err := NewSQLSubscriptionWireKeyring([]byte("subscription-key-v2"), []byte("subscription-key-v1"))
	if err != nil {
		b.Fatal(err)
	}
	wire, err := SealSQLSubscriptionWireEnvelope([]byte("subscription-key-v1"), SQLSubscriptionWireEnvelope{
		Mode:           SQLSubscriptionModeDifferential,
		SubscriptionID: "people-live",
		Sequence:       42,
		Diff:           1,
		Payload:        []byte(`{"id":7,"name":"Ada","active":true,"score":123.5}`),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for range b.N {
		if _, err := ring.Open(wire); err != nil {
			b.Fatal(err)
		}
	}
}
