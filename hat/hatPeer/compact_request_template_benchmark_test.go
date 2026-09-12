package hatPeer

import "testing"

var compactRequestTemplateBenchmarkSink []byte

func BenchmarkCompactRequestTemplateMarshal(b *testing.B) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		b.Fatal(err)
	}
	template, err := NewCompactRequestTemplate([]byte("SETSTR"))
	if err != nil {
		b.Fatal(err)
	}
	payload := []byte("key=value")
	warmup, err := protocol.Marshal(CompactFrame{Kind: CompactRequest, RequestID: 1, Command: []byte("SETSTR"), Payload: payload})
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(warmup)))
	b.Run("existing", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			encoded, err := protocol.Marshal(CompactFrame{Kind: CompactRequest, RequestID: uint64(index + 1), Command: []byte("SETSTR"), Payload: payload})
			if err != nil {
				b.Fatal(err)
			}
			compactRequestTemplateBenchmarkSink = encoded
		}
	})
	b.Run("prepared_reused", func(b *testing.B) {
		buffer := make([]byte, 0, len(warmup))
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			buffer, err = template.MarshalInto(protocol, uint64(index+1), payload, buffer[:0])
			if err != nil {
				b.Fatal(err)
			}
		}
		compactRequestTemplateBenchmarkSink = buffer
	})
}
