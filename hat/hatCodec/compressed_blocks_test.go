package hatCodec_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"hatrie_cache/hat/hatCodec"
)

func TestCompressedBlocksRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("independent block payload "), 100)
	encoded, err := hatCodec.EncodeCompressedBlocks(payload, hatCodec.CompressedBlockOptions{BlockSize: 17})
	if err != nil {
		t.Fatalf("encode compressed blocks: %v", err)
	}
	if len(encoded) <= len(payload) {
		t.Logf("encoded payload = %d bytes, source = %d bytes", len(encoded), len(payload))
	}
	decoded, err := hatCodec.DecodeCompressedBlocks(encoded)
	if err != nil {
		t.Fatalf("decode compressed blocks: %v", err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Fatalf("decoded payload differs from source")
	}
}

func TestCompressedBlocksEmptyPayload(t *testing.T) {
	encoded, err := hatCodec.EncodeCompressedBlocks(nil, hatCodec.CompressedBlockOptions{})
	if err != nil {
		t.Fatalf("encode empty payload: %v", err)
	}
	decoded, err := hatCodec.DecodeCompressedBlocks(encoded)
	if err != nil {
		t.Fatalf("decode empty payload: %v", err)
	}
	if len(decoded) != 0 {
		t.Fatalf("decoded empty payload length = %d, want 0", len(decoded))
	}
}

func TestCompressedBlocksRejectCorruptionAndTruncation(t *testing.T) {
	payload := bytes.Repeat([]byte("payload"), 100)
	encoded, err := hatCodec.EncodeCompressedBlocks(payload, hatCodec.CompressedBlockOptions{BlockSize: 32})
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := hatCodec.DecodeCompressedBlocks(corrupt); err == nil {
		t.Fatal("corrupted block decoded successfully")
	}
	if _, err := hatCodec.DecodeCompressedBlocks(encoded[:len(encoded)-1]); err == nil {
		t.Fatal("truncated block decoded successfully")
	}
	if _, err := hatCodec.DecodeCompressedBlocks([]byte("HCB0")); err == nil {
		t.Fatal("invalid header decoded successfully")
	}
}

func TestCompressedBlocksValidateOptions(t *testing.T) {
	for _, options := range []hatCodec.CompressedBlockOptions{
		{BlockSize: 0, Level: 42},
		{BlockSize: 1, Level: -3},
		{BlockSize: (64 << 20) + 1, Level: 1},
	} {
		if _, err := hatCodec.EncodeCompressedBlocks([]byte("payload"), options); err == nil {
			t.Fatalf("options %#v unexpectedly succeeded", options)
		}
	}
}

func benchmarkCompressedBlockPayload() []byte {
	return bytes.Repeat([]byte("a moderately repetitive payload for transfer blocks\n"), 4096)
}

func BenchmarkCompressedBlocksEncode(b *testing.B) {
	payload := benchmarkCompressedBlockPayload()
	b.ReportAllocs()
	for range b.N {
		encoded, err := hatCodec.EncodeCompressedBlocks(payload, hatCodec.CompressedBlockOptions{})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(len(encoded)), "wire-B")
	}
}

func BenchmarkGzipEncodeBaseline(b *testing.B) {
	payload := benchmarkCompressedBlockPayload()
	b.ReportAllocs()
	for range b.N {
		var buffer bytes.Buffer
		writer, err := gzip.NewWriterLevel(&buffer, gzip.BestSpeed)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := writer.Write(payload); err != nil {
			b.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(buffer.Len()), "wire-B")
	}
}

func BenchmarkCompressedBlocksDecode(b *testing.B) {
	payload := benchmarkCompressedBlockPayload()
	encoded, err := hatCodec.EncodeCompressedBlocks(payload, hatCodec.CompressedBlockOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(encoded)), "wire-B")
	for range b.N {
		if _, err := hatCodec.DecodeCompressedBlocks(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGzipDecodeBaseline(b *testing.B) {
	payload := benchmarkCompressedBlockPayload()
	var buffer bytes.Buffer
	writer, err := gzip.NewWriterLevel(&buffer, gzip.BestSpeed)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := writer.Write(payload); err != nil {
		b.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		b.Fatal(err)
	}
	encoded := append([]byte(nil), buffer.Bytes()...)
	b.ReportAllocs()
	b.ReportMetric(float64(len(encoded)), "wire-B")
	for range b.N {
		reader, err := gzip.NewReader(bytes.NewReader(encoded))
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadAll(reader); err != nil {
			b.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
