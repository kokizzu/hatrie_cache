package hatDataStructure

import "errors"

const (
	MaxTopKCapacity            uint64 = 1 << 20
	MaxReservoirSampleCapacity uint64 = 1 << 20
)

// ValidateTopKCapacity rejects capacities outside the supported bounded sketch range.
func ValidateTopKCapacity(capacity uint64) error {
	if capacity == 0 {
		return errors.New("hatriecache: top-k capacity must be positive")
	}
	if capacity > MaxTopKCapacity {
		return errors.New("hatriecache: top-k capacity is too large")
	}
	return nil
}

// ValidateReservoirSampleCapacity rejects capacities outside the supported
// fixed-memory sampling range.
func ValidateReservoirSampleCapacity(capacity uint64) error {
	if capacity == 0 {
		return errors.New("hatriecache: reservoir sample capacity must be positive")
	}
	if capacity > MaxReservoirSampleCapacity {
		return errors.New("hatriecache: reservoir sample capacity is too large")
	}
	return nil
}
