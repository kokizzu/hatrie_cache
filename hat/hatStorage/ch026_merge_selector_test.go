package hatStorage

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestCH026SizeTieredSelectorChoosesLargestCompatibleGroup(t *testing.T) {
	selector, err := NewCompactionMergeSelector(CompactionMergeSelectorOptions{
		Policy:        CompactionMergePolicySizeTiered,
		MaxParts:      3,
		MaxTotalBytes: 8 << 20,
		SizeRatio:     1.5,
	})
	if err != nil {
		t.Fatalf("NewCompactionMergeSelector() error = %v", err)
	}
	input := []CompactionMergeCandidate{
		{Name: "large-b", SizeBytes: 4 << 20},
		{Name: "small-c", SizeBytes: 1200 << 10},
		{Name: "small-a", SizeBytes: 1 << 20},
		{Name: "large-a", SizeBytes: 3 << 20},
		{Name: "small-b", SizeBytes: 1100 << 10},
	}
	selection, err := selector.Select(input)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	want := []string{"small-a", "small-b", "small-c"}
	if got := compactionMergeCandidateNames(selection.Candidates); !reflect.DeepEqual(got, want) {
		t.Fatalf("selected candidates = %v, want %v", got, want)
	}
	if selection.TotalBytes != (1<<20)+(1100<<10)+(1200<<10) {
		t.Fatalf("TotalBytes = %d", selection.TotalBytes)
	}
	input[0].Name = "mutated"
	if selection.Candidates[0].Name == "mutated" {
		t.Fatal("selection aliases input candidates")
	}
}

func TestCH026TimeAwareSelectorChoosesOldestBoundedWindow(t *testing.T) {
	now := time.Unix(1700000000, 0)
	selector, err := NewCompactionMergeSelector(CompactionMergeSelectorOptions{
		Policy:        CompactionMergePolicyTimeAware,
		MaxParts:      3,
		MaxTotalBytes: 10 << 20,
		MaxTimeSpan:   36 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewCompactionMergeSelector() error = %v", err)
	}
	input := []CompactionMergeCandidate{
		{Name: "recent", SizeBytes: 1 << 20, CreatedAt: now.Add(-2 * time.Hour)},
		{Name: "oldest", SizeBytes: 1 << 20, CreatedAt: now.Add(-72 * time.Hour)},
		{Name: "old", SizeBytes: 1 << 20, CreatedAt: now.Add(-48 * time.Hour)},
		{Name: "old-peer", SizeBytes: 1 << 20, CreatedAt: now.Add(-47 * time.Hour)},
	}
	selection, err := selector.Select(input)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}
	want := []string{"oldest", "old", "old-peer"}
	if got := compactionMergeCandidateNames(selection.Candidates); !reflect.DeepEqual(got, want) {
		t.Fatalf("selected candidates = %v, want %v", got, want)
	}
	if selection.Candidates[0].Name != "oldest" || selection.Candidates[1].Name != "old" {
		t.Fatalf("time-aware order = %v", compactionMergeCandidateNames(selection.Candidates))
	}
}

func TestCH026SelectorReturnsNoSelectionForInsufficientOrIncompatibleCandidates(t *testing.T) {
	selector, err := NewCompactionMergeSelector(CompactionMergeSelectorOptions{
		Policy:        CompactionMergePolicySizeTiered,
		MaxParts:      4,
		SizeRatio:     1.01,
		MaxTotalBytes: 100,
	})
	if err != nil {
		t.Fatalf("NewCompactionMergeSelector() error = %v", err)
	}
	for name, candidates := range map[string][]CompactionMergeCandidate{
		"empty": {{Name: "one", SizeBytes: 1}},
		"ratio": {{Name: "one", SizeBytes: 1}, {Name: "far", SizeBytes: 10}},
		"total": {{Name: "one", SizeBytes: 60}, {Name: "two", SizeBytes: 60}},
	} {
		selection, err := selector.Select(candidates)
		if err != nil {
			t.Fatalf("Select(%s) error = %v", name, err)
		}
		if len(selection.Candidates) != 0 || selection.TotalBytes != 0 {
			t.Fatalf("Select(%s) = %#v, want empty selection", name, selection)
		}
	}
}

func TestCH026SelectorValidatesConfigurationAndCandidates(t *testing.T) {
	for name, options := range map[string]CompactionMergeSelectorOptions{
		"unknown policy":  {Policy: CompactionMergePolicy(99)},
		"one max part":    {MaxParts: 1},
		"ratio below one": {SizeRatio: 0.5},
		"negative span":   {MaxTimeSpan: -time.Second},
	} {
		if _, err := NewCompactionMergeSelector(options); !errors.Is(err, ErrCompactionMergeSelectorInvalid) {
			t.Errorf("%s error = %v, want ErrCompactionMergeSelectorInvalid", name, err)
		}
	}
	selector, err := NewCompactionMergeSelector(CompactionMergeSelectorOptions{Policy: CompactionMergePolicyTimeAware})
	if err != nil {
		t.Fatalf("default time-aware selector error = %v", err)
	}
	if _, err := selector.Select([]CompactionMergeCandidate{{Name: "missing-time", SizeBytes: 1}, {Name: "other", SizeBytes: 1}}); !errors.Is(err, ErrCompactionMergeSelectorInvalid) {
		t.Fatalf("zero CreatedAt error = %v, want ErrCompactionMergeSelectorInvalid", err)
	}
	if _, err := selector.Select([]CompactionMergeCandidate{{Name: "duplicate", SizeBytes: 1, CreatedAt: time.Unix(1, 0)}, {Name: " duplicate ", SizeBytes: 1, CreatedAt: time.Unix(2, 0)}}); !errors.Is(err, ErrCompactionMergeSelectorInvalid) {
		t.Fatalf("duplicate name error = %v, want ErrCompactionMergeSelectorInvalid", err)
	}
}

func compactionMergeCandidateNames(candidates []CompactionMergeCandidate) []string {
	names := make([]string, len(candidates))
	for index, candidate := range candidates {
		names[index] = candidate.Name
	}
	return names
}
