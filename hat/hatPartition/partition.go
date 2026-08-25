// Package hatPartition provides deterministic local key partition routing.
package hatPartition

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// MaxCount bounds the number of local partitions. The power-of-two limit
// allows every route to use a bit mask rather than a division.
const MaxCount = 256

// Validate accepts zero (disabled) or a power-of-two partition count from two
// through MaxCount.
func Validate(count int) error {
	if count < 0 || count == 1 || count > MaxCount || (count != 0 && count&(count-1) != 0) {
		return fmt.Errorf("hatriecache: local partitions must be zero or a power of two from 2 through %d", MaxCount)
	}
	return nil
}

// Index returns the stable partition for key. count must have passed Validate
// and must be nonzero.
func Index(key string, count int) int {
	return int(xxhash.Sum64String(key) & uint64(count-1))
}
