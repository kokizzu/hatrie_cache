package hatPipeline

import "errors"

var (
	// ErrFrontierAntichainOptionsInvalid indicates invalid dimensions or a
	// point bound.
	ErrFrontierAntichainOptionsInvalid = errors.New("hatPipeline: frontier antichain options are invalid")
	// ErrFrontierAntichainPointInvalid indicates a point with the wrong
	// dimensionality or a nil antichain receiver.
	ErrFrontierAntichainPointInvalid = errors.New("hatPipeline: frontier antichain point is invalid")
	// ErrFrontierAntichainLimit indicates that an incomparable point would
	// exceed the configured antichain bound.
	ErrFrontierAntichainLimit = errors.New("hatPipeline: frontier antichain point limit reached")
)

const (
	// DefaultFrontierAntichainMaxPoints bounds retained incomparable points.
	DefaultFrontierAntichainMaxPoints = 1024
	maxFrontierAntichainDimensions    = 256
	maxFrontierAntichainPoints        = 1 << 20
)

// FrontierAntichainOptions bounds a multi-dimensional frontier antichain.
type FrontierAntichainOptions struct {
	// MaxPoints is the maximum number of incomparable points. Zero uses
	// DefaultFrontierAntichainMaxPoints.
	MaxPoints int
	// InitialPoints reserves flat storage for this many points. Zero keeps
	// construction allocation-free and grows geometrically as needed.
	InitialPoints int
}

// FrontierAntichain stores minimal points under component-wise ordering. It
// uses one flat backing slice instead of one allocation per point and is not
// safe for concurrent mutation.
type FrontierAntichain struct {
	dimensions int
	maxPoints  int
	points     []uint64
}

// FrontierAntichainSnapshot is a detached flat copy of an antichain. Values
// contains Len()*Dimensions() entries in point order.
type FrontierAntichainSnapshot struct {
	Dimensions int
	Values     []uint64
}

// NewFrontierAntichain creates an empty bounded antichain with dimensions
// components per point.
func NewFrontierAntichain(dimensions int, options FrontierAntichainOptions) (*FrontierAntichain, error) {
	maxPoints := options.MaxPoints
	if maxPoints == 0 {
		maxPoints = DefaultFrontierAntichainMaxPoints
	}
	if dimensions < 1 || dimensions > maxFrontierAntichainDimensions || maxPoints < 1 || maxPoints > maxFrontierAntichainPoints || options.InitialPoints < 0 || options.InitialPoints > maxPoints {
		return nil, ErrFrontierAntichainOptionsInvalid
	}
	antichain := &FrontierAntichain{dimensions: dimensions, maxPoints: maxPoints}
	if options.InitialPoints > 0 {
		antichain.points = make([]uint64, 0, options.InitialPoints*dimensions)
	}
	return antichain, nil
}

// Dimensions returns the number of components in each point.
func (antichain *FrontierAntichain) Dimensions() int {
	if antichain == nil {
		return 0
	}
	return antichain.dimensions
}

// Len returns the number of minimal points currently retained.
func (antichain *FrontierAntichain) Len() int {
	if antichain == nil || antichain.dimensions == 0 {
		return 0
	}
	return len(antichain.points) / antichain.dimensions
}

// Insert adds point when it is not dominated by an existing point. Existing
// points dominated by point are removed. The input is copied before return.
// It returns false when point is already covered by the antichain.
func (antichain *FrontierAntichain) Insert(point []uint64) (bool, error) {
	if antichain == nil || len(point) != antichain.dimensions {
		return false, ErrFrontierAntichainPointInvalid
	}
	for offset := 0; offset < len(antichain.points); offset += antichain.dimensions {
		if frontierAntichainLessEqual(antichain.points[offset:offset+antichain.dimensions], point) {
			return false, nil
		}
	}

	survivors := 0
	for offset := 0; offset < len(antichain.points); offset += antichain.dimensions {
		if !frontierAntichainLessEqual(point, antichain.points[offset:offset+antichain.dimensions]) {
			survivors++
		}
	}
	if survivors >= antichain.maxPoints {
		return false, ErrFrontierAntichainLimit
	}

	write := 0
	for read := 0; read < len(antichain.points); read += antichain.dimensions {
		existing := antichain.points[read : read+antichain.dimensions]
		if frontierAntichainLessEqual(point, existing) {
			continue
		}
		if write != read {
			copy(antichain.points[write:write+antichain.dimensions], existing)
		}
		write += antichain.dimensions
	}
	antichain.points = antichain.points[:write]
	antichain.points = append(antichain.points, point...)
	return true, nil
}

// Covers reports whether any retained minimal point is component-wise less
// than or equal to point.
func (antichain *FrontierAntichain) Covers(point []uint64) (bool, error) {
	if antichain == nil || len(point) != antichain.dimensions {
		return false, ErrFrontierAntichainPointInvalid
	}
	for offset := 0; offset < len(antichain.points); offset += antichain.dimensions {
		if frontierAntichainLessEqual(antichain.points[offset:offset+antichain.dimensions], point) {
			return true, nil
		}
	}
	return false, nil
}

// Snapshot returns a detached flat representation of the current antichain.
func (antichain *FrontierAntichain) Snapshot() FrontierAntichainSnapshot {
	if antichain == nil {
		return FrontierAntichainSnapshot{}
	}
	return FrontierAntichainSnapshot{
		Dimensions: antichain.dimensions,
		Values:     append([]uint64(nil), antichain.points...),
	}
}

// Len returns the number of points in the snapshot.
func (snapshot FrontierAntichainSnapshot) Len() int {
	if snapshot.Dimensions < 1 || len(snapshot.Values)%snapshot.Dimensions != 0 {
		return 0
	}
	return len(snapshot.Values) / snapshot.Dimensions
}

// At returns a detached point copy at index.
func (snapshot FrontierAntichainSnapshot) At(index int) ([]uint64, bool) {
	if index < 0 || index >= snapshot.Len() {
		return nil, false
	}
	start := index * snapshot.Dimensions
	point := make([]uint64, snapshot.Dimensions)
	copy(point, snapshot.Values[start:start+snapshot.Dimensions])
	return point, true
}

func frontierAntichainLessEqual(left, right []uint64) bool {
	for index, value := range left {
		if value > right[index] {
			return false
		}
	}
	return true
}
