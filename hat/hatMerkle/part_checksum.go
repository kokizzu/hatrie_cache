package hatMerkle

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
)

const partChecksumWireBytes = 8 + sha256.Size

var ErrInvalidPartChecksum = errors.New("hatriecache: invalid immutable part checksum")

// PartChecksum identifies immutable bytes by their length and SHA-256 digest.
// The length check rejects truncation before hashing and also distinguishes
// malformed transfer boundaries from a valid digest of another payload.
type PartChecksum struct {
	Size   uint64
	Digest [sha256.Size]byte
}

// ChecksumPart computes the integrity checksum for one immutable part.
func ChecksumPart(data []byte) PartChecksum {
	return PartChecksum{
		Size:   uint64(len(data)),
		Digest: sha256.Sum256(data),
	}
}

// VerifyPartChecksum reports whether data exactly matches checksum.
func VerifyPartChecksum(data []byte, checksum PartChecksum) bool {
	return uint64(len(data)) == checksum.Size && sha256.Sum256(data) == checksum.Digest
}

// Encode returns the canonical unpadded Base64 wire representation.
func (checksum PartChecksum) Encode() string {
	var data [partChecksumWireBytes]byte
	binary.LittleEndian.PutUint64(data[:8], checksum.Size)
	copy(data[8:], checksum.Digest[:])
	return base64.RawStdEncoding.EncodeToString(data[:])
}

// DecodePartChecksum decodes exactly one canonical checksum.
func DecodePartChecksum(value string) (PartChecksum, error) {
	var checksum PartChecksum
	if len(value) != base64.RawStdEncoding.EncodedLen(partChecksumWireBytes) {
		return checksum, ErrInvalidPartChecksum
	}
	var data [partChecksumWireBytes]byte
	n, err := base64.RawStdEncoding.Decode(data[:], []byte(value))
	if err != nil || n != len(data) || base64.RawStdEncoding.EncodeToString(data[:]) != value {
		return checksum, ErrInvalidPartChecksum
	}
	checksum.Size = binary.LittleEndian.Uint64(data[:8])
	copy(checksum.Digest[:], data[8:])
	return checksum, nil
}
