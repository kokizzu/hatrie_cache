package hatDataStructure

import (
	"errors"
	"sort"
)

var (
	ErrSparseIndexNil        = errors.New("sparse primary index is nil")
	ErrSparseIndexStride     = errors.New("sparse primary index stride is invalid")
	ErrSparseIndexComparator = errors.New("sparse primary index comparator is nil")
	ErrSparseIndexUnsorted   = errors.New("sparse primary index input is unsorted")
)

const maxSparsePrimaryIndexStride = 1 << 20

type sparsePrimaryIndexAnchor[K any] struct {
	key    K
	offset uint64
}

// SparseIndexWindow identifies the row range containing a lower-bound
// candidate. The caller owns the row storage and scans [Start, End).
// For duplicate keys, callers should continue scanning subsequent windows
// while their row predicate still matches.
type SparseIndexWindow struct {
	Start uint64
	End   uint64
}

// SparsePrimaryIndex stores one sorted-key anchor per stride-sized row block.
// It is useful for ordered immutable runs where a full primary-key index would
// consume more memory than the scan window it saves.
type SparsePrimaryIndex[K any] struct {
	stride  int
	less    func(K, K) bool
	anchors []sparsePrimaryIndexAnchor[K]
	rows    uint64
}

// NewSparsePrimaryIndex creates an empty sparse index. Stride is the maximum
// number of rows in a scan block, and less must define the input ordering.
func NewSparsePrimaryIndex[K any](stride int, less func(K, K) bool) (*SparsePrimaryIndex[K], error) {
	if stride < 1 || stride > maxSparsePrimaryIndexStride {
		return nil, ErrSparseIndexStride
	}
	if less == nil {
		return nil, ErrSparseIndexComparator
	}
	return &SparsePrimaryIndex[K]{
		stride: stride,
		less:   less,
	}, nil
}

// Build replaces the anchors with a copy of every stride-th sorted key. The
// receiver is unchanged when validation fails.
func (s *SparsePrimaryIndex[K]) Build(keys []K) error {
	if s == nil {
		return ErrSparseIndexNil
	}
	for index := 1; index < len(keys); index++ {
		if s.less(keys[index], keys[index-1]) {
			return ErrSparseIndexUnsorted
		}
	}

	anchorCount := 0
	if len(keys) > 0 {
		anchorCount = 1 + (len(keys)-1)/s.stride
	}
	anchors := make([]sparsePrimaryIndexAnchor[K], 0, anchorCount)
	for offset := 0; offset < len(keys); offset += s.stride {
		anchors = append(anchors, sparsePrimaryIndexAnchor[K]{
			key:    keys[offset],
			offset: uint64(offset),
		})
	}
	s.anchors = anchors
	s.rows = uint64(len(keys))
	return nil
}

// Window returns the scan block containing the lower-bound candidate for key.
// It returns false for an empty or nil index. It does not assert that key is
// present; the caller must verify rows in the returned range.
func (s *SparsePrimaryIndex[K]) Window(key K) (SparseIndexWindow, bool) {
	if s == nil || len(s.anchors) == 0 {
		return SparseIndexWindow{}, false
	}
	firstGreater := sort.Search(len(s.anchors), func(index int) bool {
		return s.less(key, s.anchors[index].key)
	})
	anchorIndex := firstGreater - 1
	if anchorIndex < 0 {
		anchorIndex = 0
	}
	start := s.anchors[anchorIndex].offset
	end := s.rows
	if next := anchorIndex + 1; next < len(s.anchors) {
		end = s.anchors[next].offset
	}
	return SparseIndexWindow{Start: start, End: end}, true
}

// RowCount returns the number of rows represented by the last build.
func (s *SparsePrimaryIndex[K]) RowCount() uint64 {
	if s == nil {
		return 0
	}
	return s.rows
}

// AnchorCount returns the number of retained sparse key anchors.
func (s *SparsePrimaryIndex[K]) AnchorCount() int {
	if s == nil {
		return 0
	}
	return len(s.anchors)
}

// Stride returns the configured maximum scan-block size.
func (s *SparsePrimaryIndex[K]) Stride() int {
	if s == nil {
		return 0
	}
	return s.stride
}
