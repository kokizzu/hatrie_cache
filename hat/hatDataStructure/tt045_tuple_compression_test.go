package hatDataStructure

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

func TestTT045TupleCompressionRoundTripAndAdaptiveFallback(t *testing.T) {
	compressor, err := NewTupleCompressor(DefaultTupleCompressionOptions())
	if err != nil {
		t.Fatal(err)
	}
	repeated := bytes.Repeat([]byte("region=ap-southeast-1;status=ready;"), 4096)
	encoded, err := compressor.Compress(repeated)
	if err != nil {
		t.Fatal(err)
	}
	info, err := InspectTupleCompressionFrame(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if info.Algorithm != TupleCompressionZSTD {
		t.Fatalf("repeated tuple algorithm = %v, want zstd", info.Algorithm)
	}
	decoded, err := compressor.Decompress(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, repeated) {
		t.Fatal("repeated tuple round trip changed bytes")
	}

	randomish := make([]byte, 4096)
	state := uint32(0x9e3779b9)
	for index := range randomish {
		state = state*1664525 + 1013904223
		randomish[index] = byte(state >> 24)
	}
	encoded, err = compressor.Compress(randomish)
	if err != nil {
		t.Fatal(err)
	}
	info, err = InspectTupleCompressionFrame(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if info.Algorithm != TupleCompressionNone {
		t.Fatalf("randomish tuple algorithm = %v, want raw fallback", info.Algorithm)
	}
	decoded, err = compressor.Decompress(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, randomish) {
		t.Fatal("randomish tuple round trip changed bytes")
	}
}

func TestTT045TupleCompressionFrameValidationAndBounds(t *testing.T) {
	options := DefaultTupleCompressionOptions()
	options.MaxTupleSize = 8
	compressor, err := NewTupleCompressor(options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compressor.Compress(bytes.Repeat([]byte{'x'}, 9)); !errors.Is(err, ErrTupleCompressionTooLarge) {
		t.Fatalf("oversized input error = %v", err)
	}

	options = DefaultTupleCompressionOptions()
	options.Algorithm = TupleCompressionNone
	compressor, err = NewTupleCompressor(options)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := compressor.Compress([]byte("small tuple"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := compressor.Decompress(encoded[:len(encoded)-1]); !errors.Is(err, ErrTupleCompressionCorrupt) {
		t.Fatalf("truncated frame error = %v", err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := compressor.Decompress(corrupt); !errors.Is(err, ErrTupleCompressionCorrupt) {
		t.Fatalf("checksum error = %v", err)
	}
	if _, err := InspectTupleCompressionFrame([]byte("HTC0")); !errors.Is(err, ErrTupleCompressionCorrupt) {
		t.Fatalf("bad magic error = %v", err)
	}

	for _, invalid := range []TupleCompressionOptions{
		{Algorithm: TupleCompressionAlgorithm(99)},
		{MinSize: -1},
		{MinSavingsBytes: -1},
		{MaxTupleSize: -1},
	} {
		if _, err := NewTupleCompressor(invalid); !errors.Is(err, ErrTupleCompressionInvalid) {
			t.Fatalf("invalid options %#v error = %v", invalid, err)
		}
	}
}

func TestTT045TupleCompressionConcurrentReuse(t *testing.T) {
	compressor, err := NewTupleCompressor(DefaultTupleCompressionOptions())
	if err != nil {
		t.Fatal(err)
	}
	input := bytes.Repeat([]byte("tuple-value-"), 1024)
	var waitGroup sync.WaitGroup
	for range 8 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			encoded, err := compressor.Compress(input)
			if err != nil {
				t.Error(err)
				return
			}
			decoded, err := compressor.Decompress(encoded)
			if err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(decoded, input) {
				t.Error("concurrent tuple round trip changed bytes")
			}
		}()
	}
	waitGroup.Wait()
}

func TestTT045TupleCompressionExplicitRawMode(t *testing.T) {
	options := DefaultTupleCompressionOptions()
	options.Algorithm = TupleCompressionNone
	compressor, err := NewTupleCompressor(options)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := compressor.Compress(bytes.Repeat([]byte{'a'}, 1024))
	if err != nil {
		t.Fatal(err)
	}
	info, err := InspectTupleCompressionFrame(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if info.Algorithm != TupleCompressionNone || info.OriginalSize != 1024 || info.PayloadSize != 1024 {
		t.Fatalf("raw frame info = %#v", info)
	}
}

func TestTT045TupleCompressionEntropyAdmission(t *testing.T) {
	repeated := bytes.Repeat([]byte("region=ap-southeast-1;status=ready;"), 32)
	if !tupleCompressionLikelyCompressible(repeated) {
		t.Fatal("repeated tuple was rejected by entropy admission")
	}
	randomish := make([]byte, 512)
	state := uint32(0x9e3779b9)
	for index := range randomish {
		state = state*1664525 + 1013904223
		randomish[index] = byte(state >> 24)
	}
	if tupleCompressionLikelyCompressible(randomish) {
		t.Fatal("high-entropy tuple was admitted to compression")
	}
}

func TestTT045TupleCompressionReusesScratch(t *testing.T) {
	compressor, err := NewTupleCompressor(DefaultTupleCompressionOptions())
	if err != nil {
		t.Fatal(err)
	}
	input := bytes.Repeat([]byte("tuple-value-"), 1024)
	if _, err := compressor.Compress(input); err != nil {
		t.Fatal(err)
	}
	var encoded []byte
	allocs := testing.AllocsPerRun(100, func() {
		encoded, err = compressor.Compress(input)
		if err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 1 {
		t.Fatalf("reused compression allocated %.0f times, want at most frame allocation", allocs)
	}
	if len(encoded) == 0 {
		t.Fatal("empty reused frame")
	}
}
