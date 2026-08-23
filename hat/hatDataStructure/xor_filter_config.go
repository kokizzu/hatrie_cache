package hatDataStructure

import (
	"errors"
	"strconv"
)

const MaxXorFilterItems uint64 = 1 << 22

// ValidateXorFilterExpectedItems rejects item counts outside the supported
// bounded XOR-filter construction range.
func ValidateXorFilterExpectedItems(expectedItems uint64) error {
	if expectedItems == 0 {
		return errors.New("hatriecache: xor filter expected items must be positive")
	}
	if expectedItems > MaxXorFilterItems {
		return errors.New("hatriecache: xor filter expected items must be <= " + strconv.FormatUint(MaxXorFilterItems, 10))
	}
	return nil
}
