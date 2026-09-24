package hatSql

import (
	"encoding/binary"
	"testing"
)

var mz010SubscriptionEnvelopeBenchmarkResult []byte

func BenchmarkSQLSubscriptionWireEnvelopeUnsignedBaseline(b *testing.B) {
	identifier := []byte("people-live")
	payload := []byte(`{"id":7,"name":"Ada","active":true,"score":123.5}`)
	for index := 0; index < b.N; index++ {
		wire := make([]byte, sqlSubscriptionWireHeaderBytes+len(identifier)+len(payload))
		copy(wire[:4], sqlSubscriptionWireMagic)
		wire[4] = SQLSubscriptionWireEnvelopeVersion
		wire[5] = 2
		binary.BigEndian.PutUint16(wire[6:8], uint16(len(identifier)))
		binary.BigEndian.PutUint64(wire[8:16], uint64(index))
		binary.BigEndian.PutUint32(wire[24:28], uint32(len(payload)))
		position := sqlSubscriptionWireHeaderBytes
		position += copy(wire[position:], identifier)
		copy(wire[position:], payload)
		mz010SubscriptionEnvelopeBenchmarkResult = wire
	}
	b.SetBytes(int64(len(mz010SubscriptionEnvelopeBenchmarkResult)))
}

func BenchmarkSQLSubscriptionWireEnvelopeSeal(b *testing.B) {
	envelope := SQLSubscriptionWireEnvelope{
		Mode:           SQLSubscriptionModeDifferential,
		SubscriptionID: "people-live",
		Payload:        []byte(`{"id":7,"name":"Ada","active":true,"score":123.5}`),
	}
	key := []byte("subscription-signing-key")
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		envelope.Sequence = uint64(index)
		wire, err := SealSQLSubscriptionWireEnvelope(key, envelope)
		if err != nil {
			b.Fatal(err)
		}
		mz010SubscriptionEnvelopeBenchmarkResult = wire
	}
	b.SetBytes(int64(len(mz010SubscriptionEnvelopeBenchmarkResult)))
}

func BenchmarkSQLSubscriptionWireEnvelopeOpen(b *testing.B) {
	key := []byte("subscription-signing-key")
	wire, err := SealSQLSubscriptionWireEnvelope(key, SQLSubscriptionWireEnvelope{
		Mode:           SQLSubscriptionModeDifferential,
		SubscriptionID: "people-live",
		Sequence:       42,
		Payload:        []byte(`{"id":7,"name":"Ada","active":true,"score":123.5}`),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		envelope, err := OpenSQLSubscriptionWireEnvelope(key, wire)
		if err != nil {
			b.Fatal(err)
		}
		mz010SubscriptionEnvelopeBenchmarkResult = envelope.Payload
	}
	b.SetBytes(int64(len(wire)))
}
