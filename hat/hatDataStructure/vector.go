package hatDataStructure

import (
	"errors"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
)

// VectorMatch is one cosine-similarity result.
type VectorMatch struct {
	ID    string  `json:"id"`
	Score float64 `json:"score"`
}

type vectorEntry struct {
	values []float32
	norm   float64
}

// VectorIndex is a compact exact vector index. It is appropriate for filtered
// candidate sets and moderate namespaces; approximate indexes can be layered
// on without changing this stable API.
type VectorIndex struct {
	mu         sync.RWMutex
	dimensions int
	entries    map[string]vectorEntry
}

// NewVectorIndex creates an exact index with one fixed vector dimension.
func NewVectorIndex(dimensions int) (*VectorIndex, error) {
	if dimensions <= 0 {
		return nil, errors.New("hatriecache: vector dimensions must be positive")
	}
	return &VectorIndex{dimensions: dimensions, entries: make(map[string]vectorEntry)}, nil
}

func (index *VectorIndex) Dimensions() int {
	if index == nil {
		return 0
	}
	return index.dimensions
}

func (index *VectorIndex) Len() int {
	if index == nil {
		return 0
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.entries)
}

// Upsert inserts or replaces one finite, nonzero vector.
func (index *VectorIndex) Upsert(id string, values []float32) error {
	if index == nil {
		return errors.New("hatriecache: vector index is nil")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("hatriecache: vector id is required")
	}
	norm, err := vectorNorm(values, index.dimensions)
	if err != nil {
		return err
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	index.entries[id] = vectorEntry{values: append([]float32(nil), values...), norm: norm}
	return nil
}

func (index *VectorIndex) Delete(id string) bool {
	if index == nil {
		return false
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	_, exists := index.entries[id]
	delete(index.entries, id)
	return exists
}

// Search ranks up to limit vectors by cosine similarity. filter may be nil.
func (index *VectorIndex) Search(query []float32, limit int, filter func(string) bool) ([]VectorMatch, error) {
	if index == nil {
		return nil, errors.New("hatriecache: vector index is nil")
	}
	if limit < 0 {
		return nil, errors.New("hatriecache: vector result limit must be non-negative")
	}
	if limit == 0 {
		return []VectorMatch{}, nil
	}
	norm, err := vectorNorm(query, index.dimensions)
	if err != nil {
		return nil, err
	}
	index.mu.RLock()
	matches := make([]VectorMatch, 0, min(limit, len(index.entries)))
	for id, entry := range index.entries {
		if filter != nil && !filter(id) {
			continue
		}
		dot := 0.0
		for position, value := range query {
			dot += float64(value) * float64(entry.values[position])
		}
		matches = append(matches, VectorMatch{ID: id, Score: dot / (norm * entry.norm)})
	}
	index.mu.RUnlock()
	sort.Slice(matches, func(left, right int) bool {
		if matches[left].Score == matches[right].Score {
			return matches[left].ID < matches[right].ID
		}
		return matches[left].Score > matches[right].Score
	})
	if len(matches) > limit {
		matches = matches[:limit]
	}
	return matches, nil
}

// SearchInto ranks up to limit vectors into dst, reusing its backing array
// when possible. Small limits use a bounded min-heap while scanning, so the
// method does not retain or sort non-winning candidates.
func (index *VectorIndex) SearchInto(dst []VectorMatch, query []float32, limit int, filter func(string) bool) ([]VectorMatch, error) {
	if index == nil {
		return nil, errors.New("hatriecache: vector index is nil")
	}
	if limit < 0 {
		return nil, errors.New("hatriecache: vector result limit must be non-negative")
	}
	dst = dst[:0]
	if limit == 0 {
		return dst, nil
	}
	norm, err := vectorNorm(query, index.dimensions)
	if err != nil {
		return nil, err
	}

	index.mu.RLock()
	maxResults := limit
	if maxResults > len(index.entries) {
		maxResults = len(index.entries)
	}
	if cap(dst) < maxResults {
		dst = make([]VectorMatch, 0, maxResults)
	}
	bounded := limit < len(index.entries)
	for id, entry := range index.entries {
		if filter != nil && !filter(id) {
			continue
		}
		dot := 0.0
		for position, value := range query {
			dot += float64(value) * float64(entry.values[position])
		}
		match := VectorMatch{ID: id, Score: dot / (norm * entry.norm)}
		if !bounded {
			dst = append(dst, match)
			continue
		}
		if len(dst) < limit {
			dst = append(dst, match)
			vectorMatchSiftUp(dst, len(dst)-1)
			continue
		}
		if vectorMatchCompare(match, dst[0]) >= 0 {
			continue
		}
		dst[0] = match
		vectorMatchSiftDown(dst, 0)
	}
	index.mu.RUnlock()

	slices.SortFunc(dst, vectorMatchCompare)
	return dst, nil
}

func vectorMatchCompare(left, right VectorMatch) int {
	if left.Score != right.Score {
		if left.Score > right.Score {
			return -1
		}
		return 1
	}
	if left.ID < right.ID {
		return -1
	}
	if left.ID > right.ID {
		return 1
	}
	return 0
}

func vectorMatchSiftUp(matches []VectorMatch, index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if vectorMatchCompare(matches[index], matches[parent]) <= 0 {
			return
		}
		matches[index], matches[parent] = matches[parent], matches[index]
		index = parent
	}
}

func vectorMatchSiftDown(matches []VectorMatch, index int) {
	for {
		left := index*2 + 1
		if left >= len(matches) {
			return
		}
		worst := left
		right := left + 1
		if right < len(matches) && vectorMatchCompare(matches[right], matches[left]) > 0 {
			worst = right
		}
		if vectorMatchCompare(matches[worst], matches[index]) <= 0 {
			return
		}
		matches[index], matches[worst] = matches[worst], matches[index]
		index = worst
	}
}

func vectorNorm(values []float32, dimensions int) (float64, error) {
	if len(values) != dimensions {
		return 0, errors.New("hatriecache: vector dimension does not match index")
	}
	sum := 0.0
	for _, value := range values {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return 0, errors.New("hatriecache: vector values must be finite")
		}
		sum += float64(value) * float64(value)
	}
	if sum == 0 {
		return 0, errors.New("hatriecache: zero vector is not searchable")
	}
	return math.Sqrt(sum), nil
}
