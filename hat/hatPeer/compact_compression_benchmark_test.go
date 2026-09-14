package hatPeer

import (
	"bytes"
	"testing"
)

var compactCompressionBenchmarkSink []byte

func BenchmarkCompactProtocolPayloadWire(b *testing.B) {
	plain, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		b.Fatalf("NewCompactProtocol(plain) error = %v", err)
	}
	compressed, err := NewCompactProtocol(CompactProtocolOptions{CompressPayloadsAbove: 1})
	if err != nil {
		b.Fatalf("NewCompactProtocol(compressed) error = %v", err)
	}
	frame := CompactFrame{
		Kind:      CompactRequest,
		RequestID: 42,
		Command:   []byte("BATCH"),
		Payload:   bytes.Repeat([]byte("stable-repeated-payload-"), 512),
	}
	for name, protocol := range map[string]CompactProtocol{"plain": plain, "gzip": compressed} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				encoded, err := protocol.Marshal(frame)
				if err != nil {
					b.Fatal(err)
				}
				compactCompressionBenchmarkSink = encoded
			}
			b.ReportMetric(float64(len(compactCompressionBenchmarkSink)), "wire-B")
		})
	}
}

func BenchmarkCompactProtocolPayloadRead(b *testing.B) {
	encoder, err := NewCompactProtocol(CompactProtocolOptions{CompressPayloadsAbove: 1})
	if err != nil {
		b.Fatalf("NewCompactProtocol(encoder) error = %v", err)
	}
	encoded, err := encoder.Marshal(CompactFrame{
		Kind:      CompactResponse,
		RequestID: 42,
		Payload:   bytes.Repeat([]byte("stable-repeated-payload-"), 512),
	})
	if err != nil {
		b.Fatalf("Marshal() error = %v", err)
	}
	plain, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		b.Fatalf("NewCompactProtocol(plain) error = %v", err)
	}
	plainEncoded, err := plain.Marshal(CompactFrame{
		Kind:      CompactResponse,
		RequestID: 42,
		Payload:   bytes.Repeat([]byte("stable-repeated-payload-"), 512),
	})
	if err != nil {
		b.Fatalf("Marshal(plain) error = %v", err)
	}
	for name, protocolData := range map[string]struct {
		protocol CompactProtocol
		data     []byte
	}{"plain": {plain, plainEncoded}, "gzip": {encoder, encoded}} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				decoded, err := protocolData.protocol.Read(bytes.NewReader(protocolData.data))
				if err != nil {
					b.Fatal(err)
				}
				compactCompressionBenchmarkSink = decoded.Payload
			}
			b.ReportMetric(float64(len(protocolData.data)), "wire-B")
		})
	}
}
