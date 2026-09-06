package hatMerkle

import (
	"bytes"
	"encoding/base64"
	"errors"
	"testing"
)

func TestPartChecksumRoundTripsAndDetectsCorruption(t *testing.T) {
	data := []byte("immutable part payload")
	checksum := ChecksumPart(data)

	if checksum.Size != uint64(len(data)) {
		t.Fatalf("Size = %d, want %d", checksum.Size, len(data))
	}
	if !VerifyPartChecksum(data, checksum) {
		t.Fatal("VerifyPartChecksum() = false for original data")
	}
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)-1]++
	if VerifyPartChecksum(corrupt, checksum) {
		t.Fatal("VerifyPartChecksum() = true for changed data")
	}
	if VerifyPartChecksum(data[:len(data)-1], checksum) {
		t.Fatal("VerifyPartChecksum() = true for changed length")
	}

	encoded := checksum.Encode()
	decoded, err := DecodePartChecksum(encoded)
	if err != nil {
		t.Fatalf("DecodePartChecksum() error = %v", err)
	}
	if decoded != checksum {
		t.Fatalf("decoded checksum = %#v, want %#v", decoded, checksum)
	}
}

func TestPartChecksumEncodingIsCanonicalAndRejectsMalformedInput(t *testing.T) {
	checksum := ChecksumPart(nil)
	encoded := checksum.Encode()
	if len(encoded) != base64.RawStdEncoding.EncodedLen(partChecksumWireBytes) {
		t.Fatalf("encoded length = %d, want %d", len(encoded), base64.RawStdEncoding.EncodedLen(partChecksumWireBytes))
	}
	if stringsIndexByte(encoded, '=') >= 0 {
		t.Fatalf("encoded checksum has padding: %q", encoded)
	}

	tests := []string{
		"",
		encoded[:len(encoded)-1],
		encoded + "A",
		"!" + encoded[1:],
	}
	for _, value := range tests {
		if _, err := DecodePartChecksum(value); err == nil {
			t.Errorf("DecodePartChecksum(%q) error = nil", value)
		}
	}

	badSize := append([]byte(nil), make([]byte, partChecksumWireBytes)...)
	badSize[0] = 1
	badSizeEncoded := base64.RawStdEncoding.EncodeToString(badSize)
	decoded, err := DecodePartChecksum(badSizeEncoded)
	if err != nil {
		t.Fatalf("DecodePartChecksum(badSize) error = %v", err)
	}
	if VerifyPartChecksum(nil, decoded) {
		t.Fatal("VerifyPartChecksum() = true for checksum with wrong size")
	}
}

func TestPartChecksumReturnsSentinelForInvalidEncoding(t *testing.T) {
	if _, err := DecodePartChecksum("invalid"); !errors.Is(err, ErrInvalidPartChecksum) {
		t.Fatalf("DecodePartChecksum() error = %v, want ErrInvalidPartChecksum", err)
	}
}

func BenchmarkChecksumPart(b *testing.B) {
	data := bytes.Repeat([]byte("x"), 1<<20)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for range b.N {
		_ = ChecksumPart(data)
	}
}

func stringsIndexByte(value string, target byte) int {
	for index := range len(value) {
		if value[index] == target {
			return index
		}
	}
	return -1
}
