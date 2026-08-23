package hatDataStructure

import (
	"errors"
	"math"
)

const (
	CuckooFilterBucketSize         uint8   = 4
	CuckooFilterTargetLoad         float64 = 0.95
	MinCuckooFilterBuckets         uint64  = 2
	MaxCuckooFilterBuckets         uint64  = 1 << 24
	MinCuckooFilterFingerprintBits uint8   = 4
	MaxCuckooFilterFingerprintBits uint8   = 16
)

// CuckooFilterShape returns the power-of-two bucket count and fingerprint bit
// width required for capacity at the requested false-positive rate.
func CuckooFilterShape(capacity uint64, falsePositiveRate float64) (uint64, uint8, error) {
	if capacity == 0 {
		return 0, 0, errors.New("hatriecache: cuckoo filter capacity must be positive")
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 || math.IsNaN(falsePositiveRate) {
		return 0, 0, errors.New("hatriecache: cuckoo filter false positive rate must be between 0 and 1")
	}
	bitsNeeded := math.Ceil(math.Log2((2 * float64(CuckooFilterBucketSize)) / falsePositiveRate))
	if math.IsInf(bitsNeeded, 0) || bitsNeeded > float64(MaxCuckooFilterFingerprintBits) {
		return 0, 0, errors.New("hatriecache: cuckoo filter false positive rate is too small")
	}
	if bitsNeeded < float64(MinCuckooFilterFingerprintBits) {
		bitsNeeded = float64(MinCuckooFilterFingerprintBits)
	}
	bucketsNeeded := math.Ceil(float64(capacity) / (float64(CuckooFilterBucketSize) * CuckooFilterTargetLoad))
	if math.IsInf(bucketsNeeded, 0) || bucketsNeeded > float64(MaxCuckooFilterBuckets) {
		return 0, 0, errors.New("hatriecache: cuckoo filter bucket count is too large")
	}
	bucketCount := nextPowerOfTwoCuckoo(uint64(bucketsNeeded))
	if bucketCount < MinCuckooFilterBuckets {
		bucketCount = MinCuckooFilterBuckets
	}
	if bucketCount > MaxCuckooFilterBuckets {
		return 0, 0, errors.New("hatriecache: cuckoo filter bucket count is too large")
	}
	return bucketCount, uint8(bitsNeeded), nil
}

func nextPowerOfTwoCuckoo(value uint64) uint64 {
	if value <= 1 {
		return 1
	}
	value--
	value |= value >> 1
	value |= value >> 2
	value |= value >> 4
	value |= value >> 8
	value |= value >> 16
	value |= value >> 32
	return value + 1
}
