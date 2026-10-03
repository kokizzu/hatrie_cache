package hatPeer

import (
	"testing"
	"time"
)

func BenchmarkTT035RequestEncoding(b *testing.B) {
	legacy, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		b.Fatal(err)
	}
	deadlines, err := NewCompactProtocol(CompactProtocolOptions{EnableRequestDeadlines: true})
	if err != nil {
		b.Fatal(err)
	}
	plainFrame := CompactFrame{
		Kind:      CompactRequest,
		RequestID: 7,
		Command:   []byte("GET"),
		Payload:   []byte("payload"),
	}
	deadlineFrame := plainFrame
	deadlineFrame.Flags = CompactFrameFlagDeadline
	deadlineFrame.DeadlineUnixNano = time.Now().Add(time.Minute).UnixNano()

	b.Run("legacy", func(b *testing.B) {
		buffer := make([]byte, 0, 128)
		encoded, err := legacy.Marshal(plainFrame)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		b.ReportMetric(float64(len(encoded)), "wire-bytes/op")
		for range b.N {
			var err error
			buffer, err = legacy.MarshalInto(plainFrame, buffer[:0])
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("deadline", func(b *testing.B) {
		buffer := make([]byte, 0, 128)
		encoded, err := deadlines.Marshal(deadlineFrame)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		b.ReportMetric(float64(len(encoded)), "wire-bytes/op")
		for range b.N {
			var err error
			buffer, err = deadlines.MarshalInto(deadlineFrame, buffer[:0])
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
