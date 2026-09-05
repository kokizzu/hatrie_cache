package hatCache

import (
	"bytes"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func BenchmarkCommandJournalSegmentCompression(b *testing.B) {
	raw := bytes.Repeat([]byte("sequence=123456789;command=SETSTR;key=region:sg;value=stable-payload\n"), 4096)
	var compressed bytes.Buffer
	encoder, err := zstd.NewWriter(&compressed, zstd.WithEncoderCRC(true))
	if err != nil {
		b.Fatal(err)
	}
	if _, err := encoder.Write(raw); err != nil {
		b.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(raw)), "raw_bytes")
	b.ReportMetric(float64(compressed.Len()), "compressed_bytes")

	b.Run("raw-copy", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))
		for range b.N {
			if _, err := io.Copy(io.Discard, bytes.NewReader(raw)); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("zstd", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(raw)))
		b.ReportMetric(float64(len(raw)), "raw_bytes")
		b.ReportMetric(float64(compressed.Len()), "compressed_bytes")
		b.ReportMetric(float64(compressed.Len())/float64(len(raw)), "compression_ratio")
		for range b.N {
			var output bytes.Buffer
			encoder, err := zstd.NewWriter(&output, zstd.WithEncoderCRC(true))
			if err != nil {
				b.Fatal(err)
			}
			if _, err := encoder.Write(raw); err != nil {
				b.Fatal(err)
			}
			if err := encoder.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
