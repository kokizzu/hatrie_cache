package hatCache

import (
	"errors"
	"reflect"
	"testing"
)

func TestCountMinSketchMergeCombinesCountersExactly(t *testing.T) {
	left, err := NewCountMinSketch(64, 4)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewCountMinSketch(64, 4)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := NewCountMinSketch(64, 4)
	if err != nil {
		t.Fatal(err)
	}
	for _, update := range []struct {
		target *CountMinSketch
		value  string
		count  uint32
	}{
		{target: &left, value: "alpha", count: 5},
		{target: &left, value: "left-only", count: 3},
		{target: &right, value: "alpha", count: 7},
		{target: &right, value: "right-only", count: 11},
	} {
		update.target.Add(update.value, update.count)
		reference.Add(update.value, update.count)
	}
	if err := left.Merge(right); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if !reflect.DeepEqual(left.Snapshot(), reference.Snapshot()) {
		t.Fatalf("merged snapshot = %#v, want replayed snapshot %#v", left.Snapshot(), reference.Snapshot())
	}
}

func TestCountMinSketchMergeAdoptsZeroValueAndCopiesCounters(t *testing.T) {
	part, err := NewCountMinSketch(32, 3)
	if err != nil {
		t.Fatal(err)
	}
	part.Add("stable", 9)
	var merged CountMinSketch
	if err := merged.Merge(part); err != nil {
		t.Fatalf("zero-value Merge() error = %v", err)
	}
	before := merged.Snapshot()
	part.Add("later", 4)
	if !reflect.DeepEqual(merged.Snapshot(), before) {
		t.Fatal("zero-value merge shares mutable counters with the source")
	}
}

func TestCountMinSketchMergeRejectsShapeMismatchWithoutMutation(t *testing.T) {
	receiver, err := NewCountMinSketch(32, 3)
	if err != nil {
		t.Fatal(err)
	}
	receiver.Add("stable", 9)
	before := receiver.Snapshot()
	other, err := NewCountMinSketch(64, 3)
	if err != nil {
		t.Fatal(err)
	}
	other.Add("different", 4)
	if err := receiver.Merge(other); !errors.Is(err, ErrCountMinSketchShapeMismatch) {
		t.Fatalf("shape mismatch error = %v, want ErrCountMinSketchShapeMismatch", err)
	}
	if !reflect.DeepEqual(receiver.Snapshot(), before) {
		t.Fatal("shape mismatch mutated the receiver")
	}
}

func TestCountMinSketchMergeRejectsInvalidStateWithoutMutation(t *testing.T) {
	receiver, err := NewCountMinSketch(32, 3)
	if err != nil {
		t.Fatal(err)
	}
	receiver.Add("stable", 9)
	before := receiver.Snapshot()
	invalid := CountMinSketch{width: 32, depth: 3, counters: []uint32{1}, total: 1}
	if err := receiver.Merge(invalid); !errors.Is(err, ErrCountMinSketchStateInvalid) {
		t.Fatalf("invalid state error = %v, want ErrCountMinSketchStateInvalid", err)
	}
	if !reflect.DeepEqual(receiver.Snapshot(), before) {
		t.Fatal("invalid state mutated the receiver")
	}
}

func TestCountMinSketchSnapshotRoundTripRemainsMergeable(t *testing.T) {
	sketch, err := NewCountMinSketch(32, 3)
	if err != nil {
		t.Fatal(err)
	}
	sketch.Add("alpha", 5)
	snapshot := sketch.Snapshot()
	restored, err := NewCountMinSketchFromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("NewCountMinSketchFromSnapshot() error = %v", err)
	}
	if err := restored.Merge(sketch); err != nil {
		t.Fatalf("restored Merge() error = %v", err)
	}
	if got := restored.Estimate("alpha"); got != 10 {
		t.Fatalf("restored merged estimate = %d, want 10", got)
	}
}

func TestHatTrieMergeCountMinSketchReplacesAndCopiesSource(t *testing.T) {
	part, err := NewCountMinSketch(64, 4)
	if err != nil {
		t.Fatal(err)
	}
	part.Add("alpha", 7)
	trie := newTestTrie(t)
	trie.UpsertString("target", "old")
	if err := trie.MergeCountMinSketch("target", part); err != nil {
		t.Fatalf("MergeCountMinSketch(replace) error = %v", err)
	}
	if got, ok := trie.EstimateCountMinSketch("target", "alpha"); !ok || got != 7 {
		t.Fatalf("replaced estimate = %d/%v, want 7/true", got, ok)
	}
	part.Add("later", 3)
	if got, ok := trie.EstimateCountMinSketch("target", "later"); !ok || got != 0 {
		t.Fatalf("replaced sketch shares source counters: estimate = %d/%v", got, ok)
	}
}

func TestHatTrieMergeCountMinSketchAddsToExistingSketch(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.UpsertCountMinSketch("target", 64, 4); err != nil {
		t.Fatal(err)
	}
	trie.IncrementCountMinSketch("target", "alpha", 5)
	part, err := NewCountMinSketch(64, 4)
	if err != nil {
		t.Fatal(err)
	}
	part.Add("alpha", 7)
	if err := trie.MergeCountMinSketch("target", part); err != nil {
		t.Fatalf("MergeCountMinSketch(existing) error = %v", err)
	}
	if got, ok := trie.EstimateCountMinSketch("target", "alpha"); !ok || got != 12 {
		t.Fatalf("existing estimate = %d/%v, want 12/true", got, ok)
	}
}

func TestCountMinSketchMergeSaturatesCountersAndTotal(t *testing.T) {
	left, err := NewCountMinSketch(16, 2)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewCountMinSketch(16, 2)
	if err != nil {
		t.Fatal(err)
	}
	left.Add("alpha", maxCountMinSketchCounter-1)
	right.Add("alpha", 10)
	if err := left.Merge(right); err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	if got := left.Estimate("alpha"); got != uint64(maxCountMinSketchCounter) {
		t.Fatalf("saturated estimate = %d, want %d", got, maxCountMinSketchCounter)
	}
	if got := left.Info().TotalCount; got != uint64(maxCountMinSketchCounter)+9 {
		t.Fatalf("saturated total = %d, want %d", got, uint64(maxCountMinSketchCounter)+9)
	}
}

func TestHatTrieMergeCountMinSketchRejectsInvalidSourceBeforeReplacing(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("target", "old")
	invalid := CountMinSketch{width: 64, depth: 4, counters: []uint32{1}, total: 1}
	if err := trie.MergeCountMinSketch("target", invalid); !errors.Is(err, ErrCountMinSketchStateInvalid) {
		t.Fatalf("invalid source error = %v, want ErrCountMinSketchStateInvalid", err)
	}
	if value := trie.GetString("target"); value != "old" {
		t.Fatalf("invalid source replaced target with %q", value)
	}
}

func BenchmarkCountMinSketchMergeAgainstReplay(b *testing.B) {
	for _, workload := range []struct {
		name   string
		events int
	}{
		{name: "128Events", events: 128},
		{name: "4096Events", events: 4096},
		{name: "65536Events", events: 65536},
	} {
		b.Run(workload.name+"/MergeState", func(b *testing.B) {
			left, right := benchmarkCountMinSketchParts(b, workload.events)
			values := benchmarkCountMinSketchValues(workload.events)
			b.ReportMetric(float64(len(left.Snapshot().Counters)+len(right.Snapshot().Counters)), "source-base64-bytes/op")
			b.ReportMetric(float64(benchmarkCountMinSketchRawBytes(values)), "raw-value-bytes/op")
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				target, err := NewCountMinSketch(left.width, left.depth)
				if err != nil {
					b.Fatal(err)
				}
				if err := target.Merge(left); err != nil {
					b.Fatal(err)
				}
				if err := target.Merge(right); err != nil {
					b.Fatal(err)
				}
				benchmarkCountMinSketchMergeSink = target.total
			}
		})

		b.Run(workload.name+"/ReplayValues", func(b *testing.B) {
			values := benchmarkCountMinSketchValues(workload.events)
			left, right := benchmarkCountMinSketchParts(b, workload.events)
			b.ReportMetric(float64(len(left.Snapshot().Counters)+len(right.Snapshot().Counters)), "source-base64-bytes/op")
			b.ReportMetric(float64(benchmarkCountMinSketchRawBytes(values)), "raw-value-bytes/op")
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				target, err := NewCountMinSketch(DefaultCountMinSketchWidth, DefaultCountMinSketchDepth)
				if err != nil {
					b.Fatal(err)
				}
				for _, value := range values {
					target.addJSONString(value, 1)
				}
				benchmarkCountMinSketchMergeSink = target.total
			}
		})
	}
}

var benchmarkCountMinSketchMergeSink uint64

func benchmarkCountMinSketchParts(b testing.TB, events int) (CountMinSketch, CountMinSketch) {
	b.Helper()
	values := benchmarkCountMinSketchValues(events)
	left, err := NewCountMinSketch(DefaultCountMinSketchWidth, DefaultCountMinSketchDepth)
	if err != nil {
		b.Fatal(err)
	}
	right, err := NewCountMinSketch(DefaultCountMinSketchWidth, DefaultCountMinSketchDepth)
	if err != nil {
		b.Fatal(err)
	}
	for index, value := range values {
		if index%2 == 0 {
			left.addJSONString(value, 1)
		} else {
			right.addJSONString(value, 1)
		}
	}
	return left, right
}

func benchmarkCountMinSketchValues(events int) []string {
	values := make([]string, events)
	for index := range values {
		values[index] = "value-" + string(rune('a'+index%26)) + "-" + string(rune('0'+index%10))
	}
	return values
}

func benchmarkCountMinSketchRawBytes(values []string) int {
	bytes := 0
	for _, value := range values {
		bytes += len(value)
	}
	return bytes
}
