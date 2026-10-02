package hatStorage

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

var (
	ErrCompactionMergeSelectorNil     = errors.New("hatriecache: compaction merge selector is nil")
	ErrCompactionMergeSelectorInvalid = errors.New("hatriecache: compaction merge selector input is invalid")
)

const (
	DefaultCompactionMergeSelectorMaxParts      = 4
	DefaultCompactionMergeSelectorMaxTotalBytes = 64 << 20
	DefaultCompactionMergeSelectorSizeRatio     = 1.5
	maxCompactionMergeSelectorNameBytes         = 1024
)

// CompactionMergePolicy selects how candidate parts are grouped.
type CompactionMergePolicy uint8

const (
	CompactionMergePolicySizeTiered CompactionMergePolicy = iota + 1
	CompactionMergePolicyTimeAware
)

// CompactionMergeCandidate describes one immutable part eligible for a merge.
// The selector never mutates or retains the input slice.
type CompactionMergeCandidate struct {
	Name      string    `json:"name"`
	SizeBytes uint64    `json:"size_bytes"`
	CreatedAt time.Time `json:"created_at"`
}

// CompactionMergeSelectorOptions bounds one deterministic selection pass.
// Zero values choose conservative size-tiered defaults; MaxTimeSpan is
// optional and only limits time-aware windows when it is positive.
type CompactionMergeSelectorOptions struct {
	Policy        CompactionMergePolicy
	MaxParts      int
	MaxTotalBytes uint64
	SizeRatio     float64
	MaxTimeSpan   time.Duration
}

// CompactionMergeSelection is an independent candidate batch. An empty
// selection is returned without error when no compatible pair exists.
type CompactionMergeSelection struct {
	Candidates []CompactionMergeCandidate `json:"candidates,omitempty"`
	TotalBytes uint64                     `json:"total_bytes,omitempty"`
}

// CompactionMergeSelector implements opt-in size-tiered and time-aware
// candidate grouping. It only plans a batch; callers own execution, locking,
// persistence, and scheduler admission.
type CompactionMergeSelector struct {
	options CompactionMergeSelectorOptions
}

// NewCompactionMergeSelector validates and snapshots selection policy.
func NewCompactionMergeSelector(options CompactionMergeSelectorOptions) (*CompactionMergeSelector, error) {
	if options.Policy == 0 {
		options.Policy = CompactionMergePolicySizeTiered
	}
	if options.Policy != CompactionMergePolicySizeTiered && options.Policy != CompactionMergePolicyTimeAware {
		return nil, ErrCompactionMergeSelectorInvalid
	}
	if options.MaxParts == 0 {
		options.MaxParts = DefaultCompactionMergeSelectorMaxParts
	}
	if options.MaxParts < 2 {
		return nil, ErrCompactionMergeSelectorInvalid
	}
	if options.MaxTotalBytes == 0 {
		options.MaxTotalBytes = DefaultCompactionMergeSelectorMaxTotalBytes
	}
	if options.SizeRatio == 0 {
		options.SizeRatio = DefaultCompactionMergeSelectorSizeRatio
	}
	if math.IsNaN(options.SizeRatio) || math.IsInf(options.SizeRatio, 0) || options.SizeRatio < 1 {
		return nil, ErrCompactionMergeSelectorInvalid
	}
	if options.MaxTimeSpan < 0 {
		return nil, ErrCompactionMergeSelectorInvalid
	}
	return &CompactionMergeSelector{options: options}, nil
}

// Select returns the best deterministic candidate group for the configured
// policy. It returns no selection, rather than an error, when fewer than two
// candidates fit the configured bounds.
func (selector *CompactionMergeSelector) Select(candidates []CompactionMergeCandidate) (CompactionMergeSelection, error) {
	if selector == nil {
		return CompactionMergeSelection{}, ErrCompactionMergeSelectorNil
	}
	if len(candidates) == 0 {
		return CompactionMergeSelection{}, nil
	}
	ordered := make([]CompactionMergeCandidate, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for index, candidate := range candidates {
		candidate.Name = strings.TrimSpace(candidate.Name)
		if candidate.Name == "" || len(candidate.Name) > maxCompactionMergeSelectorNameBytes {
			return CompactionMergeSelection{}, fmt.Errorf("%w: candidate %d name is invalid", ErrCompactionMergeSelectorInvalid, index)
		}
		if _, exists := seen[candidate.Name]; exists {
			return CompactionMergeSelection{}, fmt.Errorf("%w: duplicate candidate %q", ErrCompactionMergeSelectorInvalid, candidate.Name)
		}
		seen[candidate.Name] = struct{}{}
		if selector.options.Policy == CompactionMergePolicyTimeAware && candidate.CreatedAt.IsZero() {
			return CompactionMergeSelection{}, fmt.Errorf("%w: candidate %q has no creation time", ErrCompactionMergeSelectorInvalid, candidate.Name)
		}
		ordered[index] = candidate
	}
	if selector.options.Policy == CompactionMergePolicyTimeAware {
		return selector.selectTimeAware(ordered), nil
	}
	return selector.selectSizeTiered(ordered), nil
}

func (selector *CompactionMergeSelector) selectSizeTiered(candidates []CompactionMergeCandidate) CompactionMergeSelection {
	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].SizeBytes != candidates[right].SizeBytes {
			return candidates[left].SizeBytes < candidates[right].SizeBytes
		}
		return candidates[left].Name < candidates[right].Name
	})
	bestStart, bestCount, bestTotal := -1, 0, uint64(0)
	for start := range candidates {
		total := uint64(0)
		for end := start; end < len(candidates) && end < start+selector.options.MaxParts; end++ {
			if end > start && !compactionMergeSizeCompatible(candidates[start].SizeBytes, candidates[end].SizeBytes, selector.options.SizeRatio) {
				break
			}
			if candidates[end].SizeBytes > selector.options.MaxTotalBytes-total {
				break
			}
			total += candidates[end].SizeBytes
			count := end - start + 1
			if count >= 2 && (count > bestCount || (count == bestCount && total > bestTotal)) {
				bestStart, bestCount, bestTotal = start, count, total
			}
		}
	}
	return compactionMergeSelectionFrom(candidates, bestStart, bestCount, bestTotal)
}

func (selector *CompactionMergeSelector) selectTimeAware(candidates []CompactionMergeCandidate) CompactionMergeSelection {
	sort.Slice(candidates, func(left, right int) bool {
		if !candidates[left].CreatedAt.Equal(candidates[right].CreatedAt) {
			return candidates[left].CreatedAt.Before(candidates[right].CreatedAt)
		}
		return candidates[left].Name < candidates[right].Name
	})
	bestStart, bestCount, bestTotal := -1, 0, uint64(0)
	for start := range candidates {
		total := uint64(0)
		for end := start; end < len(candidates) && end < start+selector.options.MaxParts; end++ {
			if selector.options.MaxTimeSpan > 0 && candidates[end].CreatedAt.Sub(candidates[start].CreatedAt) > selector.options.MaxTimeSpan {
				break
			}
			if candidates[end].SizeBytes > selector.options.MaxTotalBytes-total {
				break
			}
			total += candidates[end].SizeBytes
			count := end - start + 1
			if count >= 2 && (count > bestCount || (count == bestCount && start < bestStart || bestStart < 0)) {
				bestStart, bestCount, bestTotal = start, count, total
			}
		}
	}
	return compactionMergeSelectionFrom(candidates, bestStart, bestCount, bestTotal)
}

func compactionMergeSelectionFrom(candidates []CompactionMergeCandidate, start, count int, total uint64) CompactionMergeSelection {
	if start < 0 || count < 2 {
		return CompactionMergeSelection{}
	}
	selected := make([]CompactionMergeCandidate, count)
	copy(selected, candidates[start:start+count])
	return CompactionMergeSelection{Candidates: selected, TotalBytes: total}
}

func compactionMergeSizeCompatible(minimum, maximum uint64, ratio float64) bool {
	if minimum == 0 {
		return maximum == 0
	}
	return float64(maximum)/float64(minimum) <= ratio
}
