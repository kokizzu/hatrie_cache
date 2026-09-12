package hatDataStructure

import (
	"errors"
	"fmt"
	"sync"
)

const (
	// MaxU64AntichainDimensions bounds the width of one timestamp vector.
	MaxU64AntichainDimensions = 64
	// MaxU64AntichainEntries bounds the configured number of incomparable
	// vectors when a caller requests an explicit limit.
	MaxU64AntichainEntries = 1 << 20
)

var (
	// ErrU64AntichainNil indicates that an operation was attempted on a nil antichain.
	ErrU64AntichainNil = errors.New("hatDataStructure: u64 antichain is nil")
	// ErrU64AntichainDimensions indicates an invalid vector width.
	ErrU64AntichainDimensions = errors.New("hatDataStructure: invalid u64 antichain dimensions")
	// ErrU64AntichainPointDimensions indicates a point with the wrong vector width.
	ErrU64AntichainPointDimensions = errors.New("hatDataStructure: invalid u64 antichain point dimensions")
	// ErrU64AntichainMaxEntries indicates an invalid or exceeded entry limit.
	ErrU64AntichainMaxEntries = errors.New("hatDataStructure: u64 antichain entry limit exceeded")
	// ErrU64AntichainInitialEntries indicates an invalid initial capacity.
	ErrU64AntichainInitialEntries = errors.New("hatDataStructure: invalid u64 antichain initial capacity")
)

// U64AntichainOptions configures a U64Antichain. MaxEntries is optional; zero
// means that the caller accepts the default unbounded behavior.
type U64AntichainOptions struct {
	Dimensions int
	MaxEntries int
	// InitialEntries reserves space for this many points. Zero leaves capacity
	// growth lazy.
	InitialEntries int
}

// U64Antichain stores the minimal elements of a coordinate-wise partial order.
// Each point is a fixed-width uint64 vector, and all vectors are kept in one
// flat backing slice to avoid one allocation and one slice header per point.
// The type is safe for concurrent readers and writers.
//
// If an existing point is less than or equal to a new point in every
// dimension, the new point is redundant. Otherwise, existing points dominated
// by the new point are removed before the new point is appended.
type U64Antichain struct {
	mu         sync.RWMutex
	dimensions int
	maxEntries int
	points     []uint64
}

// NewU64Antichain creates an antichain with dimensions fixed-width values per
// point. MaxEntries remains unlimited.
func NewU64Antichain(dimensions int) (*U64Antichain, error) {
	return NewU64AntichainWithOptions(U64AntichainOptions{Dimensions: dimensions})
}

// NewU64AntichainWithOptions creates a bounded antichain. Dimensions must be
// positive and no greater than MaxU64AntichainDimensions. A positive
// MaxEntries protects memory use and cannot exceed MaxU64AntichainEntries.
func NewU64AntichainWithOptions(options U64AntichainOptions) (*U64Antichain, error) {
	if options.Dimensions <= 0 || options.Dimensions > MaxU64AntichainDimensions {
		return nil, fmt.Errorf("%w: got %d, want 1..%d", ErrU64AntichainDimensions, options.Dimensions, MaxU64AntichainDimensions)
	}
	if options.MaxEntries < 0 || options.MaxEntries > MaxU64AntichainEntries {
		return nil, fmt.Errorf("%w: got %d, want 0..%d", ErrU64AntichainMaxEntries, options.MaxEntries, MaxU64AntichainEntries)
	}
	if options.InitialEntries < 0 || options.InitialEntries > MaxU64AntichainEntries || (options.MaxEntries > 0 && options.InitialEntries > options.MaxEntries) {
		return nil, fmt.Errorf("%w: got %d, max entries %d", ErrU64AntichainInitialEntries, options.InitialEntries, options.MaxEntries)
	}
	initialCapacity := options.InitialEntries * options.Dimensions
	return &U64Antichain{
		dimensions: options.Dimensions,
		maxEntries: options.MaxEntries,
		points:     make([]uint64, 0, initialCapacity),
	}, nil
}

// Add inserts point if it is not already covered by an existing point. It
// returns true when the antichain changes. A point is covered when any stored
// point is less than or equal to it in every dimension.
//
// Capacity validation occurs after dominance analysis but before mutation, so
// a rejected insertion leaves the previous antichain unchanged.
func (antichain *U64Antichain) Add(point []uint64) (bool, error) {
	if antichain == nil {
		return false, ErrU64AntichainNil
	}
	if err := antichain.validatePoint(point); err != nil {
		return false, err
	}

	antichain.mu.Lock()
	defer antichain.mu.Unlock()

	pointCount := len(antichain.points) / antichain.dimensions
	removed := 0
	for offset := 0; offset < len(antichain.points); offset += antichain.dimensions {
		existing := antichain.points[offset : offset+antichain.dimensions]
		if u64AntichainPointLessEqual(existing, point) {
			return false, nil
		}
		if u64AntichainPointLessEqual(point, existing) {
			removed++
		}
	}
	if antichain.maxEntries > 0 && pointCount-removed+1 > antichain.maxEntries {
		return false, fmt.Errorf("%w: maximum %d", ErrU64AntichainMaxEntries, antichain.maxEntries)
	}
	if removed == 0 {
		antichain.points = append(antichain.points, point...)
		return true, nil
	}

	writeOffset := 0
	for offset := 0; offset < len(antichain.points); offset += antichain.dimensions {
		existing := antichain.points[offset : offset+antichain.dimensions]
		if u64AntichainPointLessEqual(point, existing) {
			continue
		}
		copy(antichain.points[writeOffset:writeOffset+antichain.dimensions], existing)
		writeOffset += antichain.dimensions
	}
	antichain.points = antichain.points[:writeOffset]
	antichain.points = append(antichain.points, point...)
	return true, nil
}

// Covers reports whether any stored point is less than or equal to point in
// every dimension. It is the usual frontier-read predicate: a point at or
// beyond a known frontier is covered.
func (antichain *U64Antichain) Covers(point []uint64) (bool, error) {
	if antichain == nil {
		return false, ErrU64AntichainNil
	}
	if err := antichain.validatePoint(point); err != nil {
		return false, err
	}
	antichain.mu.RLock()
	defer antichain.mu.RUnlock()
	for offset := 0; offset < len(antichain.points); offset += antichain.dimensions {
		if u64AntichainPointLessEqual(antichain.points[offset:offset+antichain.dimensions], point) {
			return true, nil
		}
	}
	return false, nil
}

// Snapshot copies points into dst in row-major order. Each point occupies
// Dimensions consecutive values. The destination backing array is reused when
// it has enough capacity; callers must treat the returned slice as owned.
func (antichain *U64Antichain) Snapshot(dst []uint64) ([]uint64, error) {
	if antichain == nil {
		return dst[:0], ErrU64AntichainNil
	}
	antichain.mu.RLock()
	defer antichain.mu.RUnlock()
	dst = dst[:0]
	if cap(dst) < len(antichain.points) {
		dst = make([]uint64, 0, len(antichain.points))
	}
	return append(dst, antichain.points...), nil
}

// Len returns the number of incomparable points currently stored.
func (antichain *U64Antichain) Len() int {
	if antichain == nil {
		return 0
	}
	antichain.mu.RLock()
	defer antichain.mu.RUnlock()
	return len(antichain.points) / antichain.dimensions
}

// Dimensions returns the fixed width of each point. A nil antichain reports
// zero.
func (antichain *U64Antichain) Dimensions() int {
	if antichain == nil {
		return 0
	}
	return antichain.dimensions
}

// MaxEntries returns the configured point limit. Zero means unlimited. A nil
// antichain reports zero.
func (antichain *U64Antichain) MaxEntries() int {
	if antichain == nil {
		return 0
	}
	return antichain.maxEntries
}

// Reset removes all points while retaining the allocated flat backing array
// and configuration for reuse.
func (antichain *U64Antichain) Reset() {
	if antichain == nil {
		return
	}
	antichain.mu.Lock()
	antichain.points = antichain.points[:0]
	antichain.mu.Unlock()
}

func (antichain *U64Antichain) validatePoint(point []uint64) error {
	if len(point) != antichain.dimensions {
		return fmt.Errorf("%w: got %d, want %d", ErrU64AntichainPointDimensions, len(point), antichain.dimensions)
	}
	return nil
}

func u64AntichainPointLessEqual(left, right []uint64) bool {
	for index, value := range left {
		if value > right[index] {
			return false
		}
	}
	return true
}
