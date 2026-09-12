package hatPeer

import (
	"bytes"
	"encoding/json"
	"testing"
)

type benchmarkJSONFrame struct {
	Kind      CompactFrameKind `json:"kind"`
	RequestID uint64           `json:"request_id"`
	Flags     byte             `json:"flags,omitempty"`
	Command   string           `json:"command"`
	Payload   []byte           `json:"payload,omitempty"`
}

func benchmarkCompactProtocolFrame() CompactFrame {
	return CompactFrame{
		Kind:      CompactRequest,
		RequestID: 42,
		Command:   []byte("SETSTR"),
		Payload:   []byte(`{"key":"orders:42","value":"ready","ttl_seconds":3600}`),
	}
}

func TestCompactProtocolWireSize(t *testing.T) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	frame := benchmarkCompactProtocolFrame()
	compact, err := protocol.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	jsonFrame, err := json.Marshal(benchmarkJSONFrame{
		Kind:      frame.Kind,
		RequestID: frame.RequestID,
		Flags:     frame.Flags,
		Command:   string(frame.Command),
		Payload:   frame.Payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("compact_bytes=%d json_bytes=%d compact_ratio=%.3f", len(compact), len(jsonFrame), float64(len(compact))/float64(len(jsonFrame)))
	if len(compact) >= len(jsonFrame) {
		t.Fatalf("compact frame bytes = %d, JSON bytes = %d", len(compact), len(jsonFrame))
	}
}

func BenchmarkCompactProtocolMarshal(b *testing.B) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		b.Fatal(err)
	}
	frame := benchmarkCompactProtocolFrame()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded, err := protocol.Marshal(frame)
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(encoded)))
	}
}

func BenchmarkJSONFrameMarshal(b *testing.B) {
	frame := benchmarkCompactProtocolFrame()
	jsonFrame := benchmarkJSONFrame{
		Kind:      frame.Kind,
		RequestID: frame.RequestID,
		Flags:     frame.Flags,
		Command:   string(frame.Command),
		Payload:   frame.Payload,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded, err := json.Marshal(jsonFrame)
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(encoded)))
	}
}

func BenchmarkCompactProtocolRead(b *testing.B) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := protocol.Marshal(benchmarkCompactProtocolFrame())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := protocol.Read(bytes.NewReader(encoded)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONFrameUnmarshal(b *testing.B) {
	frame := benchmarkCompactProtocolFrame()
	encoded, err := json.Marshal(benchmarkJSONFrame{
		Kind:      frame.Kind,
		RequestID: frame.RequestID,
		Flags:     frame.Flags,
		Command:   string(frame.Command),
		Payload:   frame.Payload,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded benchmarkJSONFrame
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}
