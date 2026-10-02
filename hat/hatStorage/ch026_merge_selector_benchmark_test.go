package hatStorage

import (
	"fmt"
	"testing"
	"time"
)

var ch026MergeSelectorSink CompactionMergeSelection

func BenchmarkCH026SizeTieredSelection(b *testing.B) {
	selector, err := NewCompactionMergeSelector(CompactionMergeSelectorOptions{
		Policy:        CompactionMergePolicySizeTiered,
		MaxParts:      4,
		MaxTotalBytes: 64 << 20,
		SizeRatio:     1.5,
	})
	if err != nil {
		b.Fatal(err)
	}
	candidates := benchmarkCH026Candidates()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selection, err := selector.Select(candidates)
		if err != nil {
			b.Fatal(err)
		}
		if len(selection.Candidates) < 2 {
			b.Fatalf("expected at least two candidates, got %d", len(selection.Candidates))
		}
		ch026MergeSelectorSink = selection
	}
}

func BenchmarkCH026TimeAwareSelection(b *testing.B) {
	selector, err := NewCompactionMergeSelector(CompactionMergeSelectorOptions{
		Policy:        CompactionMergePolicyTimeAware,
		MaxParts:      4,
		MaxTotalBytes: 64 << 20,
		MaxTimeSpan:   24 * time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	candidates := benchmarkCH026Candidates()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		selection, err := selector.Select(candidates)
		if err != nil {
			b.Fatal(err)
		}
		if len(selection.Candidates) < 2 {
			b.Fatalf("expected at least two candidates, got %d", len(selection.Candidates))
		}
		ch026MergeSelectorSink = selection
	}
}

func benchmarkCH026Candidates() []CompactionMergeCandidate {
	const candidateCount = 256
	baseTime := time.Unix(1_700_000_000, 0).UTC()
	candidates := make([]CompactionMergeCandidate, candidateCount)
	for i := range candidates {
		candidates[i] = CompactionMergeCandidate{
			Name:      fmt.Sprintf("part-%03d", i),
			SizeBytes: uint64(i%16+1) * 1 << 20,
			CreatedAt: baseTime.Add(-time.Duration(candidateCount-i) * time.Hour),
		}
	}
	return candidates
}
