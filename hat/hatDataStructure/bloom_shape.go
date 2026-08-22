package hatDataStructure

import (
	"errors"
	"math"
)

const (
	MinBloomFilterBits   uint64 = 64
	MaxBloomFilterBits   uint64 = 1 << 31
	MaxBloomFilterHashes uint8  = 64
)

// BloomFilterShape returns the bit count and hash count for the requested
// expected item count and false-positive rate.
func BloomFilterShape(expectedItems uint64, falsePositiveRate float64) (uint64, uint8, error) {
	if expectedItems == 0 {
		return 0, 0, errors.New("hatriecache: bloom filter expected items must be positive")
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 || math.IsNaN(falsePositiveRate) {
		return 0, 0, errors.New("hatriecache: bloom filter false positive rate must be between 0 and 1")
	}

	bits := math.Ceil(-float64(expectedItems) * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2))
	if math.IsInf(bits, 0) || bits > float64(MaxBloomFilterBits) {
		return 0, 0, errors.New("hatriecache: bloom filter bit count is too large")
	}
	bitCount := uint64(bits)
	if bitCount < MinBloomFilterBits {
		bitCount = MinBloomFilterBits
	}

	hashes := math.Ceil((float64(bitCount) / float64(expectedItems)) * math.Ln2)
	if math.IsInf(hashes, 0) || hashes < 1 {
		hashes = 1
	}
	if hashes > float64(MaxBloomFilterHashes) {
		return 0, 0, errors.New("hatriecache: bloom filter hash count is too large")
	}
	return bitCount, uint8(hashes), nil
}
