// Package hatMerkle provides fixed-size Merkle bucket primitives for
// deterministic replication inventory exchange.
package hatMerkle

import (
	"encoding/base64"
	"encoding/binary"
	"errors"

	"github.com/cespare/xxhash/v2"
)

// BucketCount is the fixed number of Merkle buckets used by the wire format.
const BucketCount = 1024

// BucketMask identifies one or more buckets in a fixed-size Merkle inventory.
type BucketMask [BucketCount / 64]uint64

// Contains reports whether bucket is selected.
func (mask BucketMask) Contains(bucket int) bool {
	return bucket >= 0 && bucket < BucketCount && mask[bucket/64]&(uint64(1)<<uint(bucket%64)) != 0
}

// ContainsKey reports whether the deterministic bucket for key is selected.
func (mask BucketMask) ContainsKey(key string) bool {
	return mask.Contains(BucketForKey(key))
}

// Empty reports whether no buckets are selected.
func (mask BucketMask) Empty() bool {
	for _, word := range mask {
		if word != 0 {
			return false
		}
	}
	return true
}

// BucketForKey returns the deterministic Merkle bucket for key.
func BucketForKey(key string) int {
	return BucketForHash(xxhash.Sum64String(key))
}

// BucketForHash returns the deterministic Merkle bucket for a key hash.
func BucketForHash(keyHash uint64) int {
	return int(keyHash >> (64 - 10))
}

// EncodeBucketMask returns the canonical base64 wire representation.
func EncodeBucketMask(mask BucketMask) string {
	var data [len(mask) * 8]byte
	for index, word := range mask {
		binary.LittleEndian.PutUint64(data[index*8:], word)
	}
	return base64.RawStdEncoding.EncodeToString(data[:])
}

// DecodeBucketMask decodes exactly one canonical fixed-size mask.
func DecodeBucketMask(value string) (BucketMask, error) {
	mask := BucketMask{}
	data := make([]byte, len(mask)*8)
	if len(value) != base64.RawStdEncoding.EncodedLen(len(data)) {
		return mask, errors.New("hatriecache: invalid replication Merkle bucket mask")
	}
	n, err := base64.RawStdEncoding.Decode(data, []byte(value))
	if err != nil || n != len(data) {
		return mask, errors.New("hatriecache: invalid replication Merkle bucket mask")
	}
	for index := range mask {
		mask[index] = binary.LittleEndian.Uint64(data[index*8:])
	}
	return mask, nil
}
