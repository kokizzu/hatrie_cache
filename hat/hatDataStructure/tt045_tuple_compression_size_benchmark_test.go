package hatDataStructure

import (
	"bytes"
	"testing"
)

func BenchmarkTT045TupleCompressionWireSize(b *testing.B) {
	repeated := bytes.Repeat([]byte("hatrie-cache tuple compression benchmark "), 96*1024/39+1)[:96*1024]
	random := make([]byte, len(repeated))
	for i := range random {
		random[i] = byte((i*31 + 17) % 251)
	}

	for _, tc := range []struct {
		name  string
		tuple []byte
	}{
		{name: "repeated", tuple: repeated},
		{name: "random", tuple: random},
	} {
		b.Run(tc.name, func(b *testing.B) {
			compressor, err := NewTupleCompressor(TupleCompressionOptions{})
			if err != nil {
				b.Fatal(err)
			}
			defer compressor.Close()

			encoded, err := compressor.Compress(tc.tuple)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := compressor.Compress(tc.tuple); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(tc.tuple)), "input-bytes")
			b.ReportMetric(float64(len(encoded)), "frame-bytes")
		})
	}
}
