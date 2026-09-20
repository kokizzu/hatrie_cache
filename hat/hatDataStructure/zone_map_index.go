package hatDataStructure

import "errors"

var (
	ErrZoneMapNil         = errors.New("zone map index is nil")
	ErrZoneMapSegmentSize = errors.New("zone map segment size is invalid")
	ErrZoneMapComparator  = errors.New("zone map comparator is nil")
)

const maxZoneMapSegmentSize = 1 << 20

// ZoneMapSegment describes one caller-owned row block and its inclusive
// minimum and maximum values.
type ZoneMapSegment[K any] struct {
	Min   K
	Max   K
	Start uint64
	End   uint64
}

// ZoneMapIndex stores min/max bounds for fixed-size row segments. It is a
// data-skipping index: callers must still evaluate the actual row predicate.
type ZoneMapIndex[K any] struct {
	segmentSize int
	less        func(K, K) bool
	segments    []ZoneMapSegment[K]
	rows        uint64
}

// NewZoneMapIndex creates an empty min/max data-skipping index.
func NewZoneMapIndex[K any](segmentSize int, less func(K, K) bool) (*ZoneMapIndex[K], error) {
	if segmentSize < 1 || segmentSize > maxZoneMapSegmentSize {
		return nil, ErrZoneMapSegmentSize
	}
	if less == nil {
		return nil, ErrZoneMapComparator
	}
	return &ZoneMapIndex[K]{
		segmentSize: segmentSize,
		less:        less,
	}, nil
}

// Build replaces all bounds after scanning values. Existing bounds remain
// untouched until the new segment slice is complete.
func (z *ZoneMapIndex[K]) Build(values []K) error {
	if z == nil {
		return ErrZoneMapNil
	}
	segmentCount := 0
	if len(values) > 0 {
		segmentCount = 1 + (len(values)-1)/z.segmentSize
	}
	segments := make([]ZoneMapSegment[K], 0, segmentCount)
	for start := 0; start < len(values); start += z.segmentSize {
		end := len(values)
		if remaining := len(values) - start; remaining > z.segmentSize {
			end = start + z.segmentSize
		}
		minimum := values[start]
		maximum := values[start]
		for _, value := range values[start+1 : end] {
			if z.less(value, minimum) {
				minimum = value
			}
			if z.less(maximum, value) {
				maximum = value
			}
		}
		segments = append(segments, ZoneMapSegment[K]{
			Min:   minimum,
			Max:   maximum,
			Start: uint64(start),
			End:   uint64(end),
		})
	}
	z.segments = segments
	z.rows = uint64(len(values))
	return nil
}

// VisitEqual visits segments whose bounds may contain value. It returns the
// number visited and stops when the callback returns false. A nil callback
// performs no scan and returns zero.
func (z *ZoneMapIndex[K]) VisitEqual(value K, visit func(ZoneMapSegment[K]) bool) int {
	if z == nil || visit == nil {
		return 0
	}
	visited := 0
	for _, segment := range z.segments {
		if z.less(value, segment.Min) || z.less(segment.Max, value) {
			continue
		}
		visited++
		if !visit(segment) {
			break
		}
	}
	return visited
}

// VisitRange visits segments whose inclusive bounds overlap [lower, upper].
// It returns the number visited and stops when the callback returns false.
func (z *ZoneMapIndex[K]) VisitRange(lower, upper K, visit func(ZoneMapSegment[K]) bool) int {
	if z == nil || visit == nil || z.less(upper, lower) {
		return 0
	}
	visited := 0
	for _, segment := range z.segments {
		if z.less(segment.Max, lower) || z.less(upper, segment.Min) {
			continue
		}
		visited++
		if !visit(segment) {
			break
		}
	}
	return visited
}

// RowCount returns the number of rows represented by the last build.
func (z *ZoneMapIndex[K]) RowCount() uint64 {
	if z == nil {
		return 0
	}
	return z.rows
}

// SegmentCount returns the number of retained min/max segments.
func (z *ZoneMapIndex[K]) SegmentCount() int {
	if z == nil {
		return 0
	}
	return len(z.segments)
}

// SegmentSize returns the configured maximum rows per segment.
func (z *ZoneMapIndex[K]) SegmentSize() int {
	if z == nil {
		return 0
	}
	return z.segmentSize
}
