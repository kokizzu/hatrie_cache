package hatPeer

import (
	"fmt"
	"testing"
)

func BenchmarkT238CompactBatchEncoding(b *testing.B) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		b.Fatal(err)
	}
	requests := make([]CompactBatchRequest, 32)
	for index := range requests {
		requests[index] = CompactBatchRequest{
			Command: []byte("SETSTR"),
			Payload: []byte(fmt.Sprintf(`{"key":"orders:%02d","value":"ready"}`, index)),
		}
	}

	b.Run("individual_frames_baseline", func(b *testing.B) {
		b.ReportAllocs()
		var wireBytes int
		for iteration := 0; iteration < b.N; iteration++ {
			wireBytes = 0
			for index, request := range requests {
				encoded, err := protocol.Marshal(CompactFrame{
					Kind:      CompactRequest,
					RequestID: uint64(index + 1),
					Command:   request.Command,
					Payload:   request.Payload,
				})
				if err != nil {
					b.Fatal(err)
				}
				wireBytes += len(encoded)
			}
		}
		b.ReportMetric(float64(wireBytes), "wire_bytes")
	})

	b.Run("one_batch", func(b *testing.B) {
		b.ReportAllocs()
		var wireBytes int
		for iteration := 0; iteration < b.N; iteration++ {
			encoded, err := protocol.marshalCompactBatchFrameInto(uint64(iteration+1), requests, nil)
			if err != nil {
				b.Fatal(err)
			}
			wireBytes = len(encoded)
		}
		b.ReportMetric(float64(wireBytes), "wire_bytes")
	})
}
