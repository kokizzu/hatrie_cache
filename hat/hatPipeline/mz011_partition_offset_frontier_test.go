package hatPipeline

import (
	"fmt"
	"testing"
)

func ExamplePartitionOffsetFrontier() {
	frontier, _ := NewPartitionOffsetFrontier([]int32{0, 1}, PartitionOffsetFrontierOptions{})
	_ = frontier.Advance(0, 10, 100)
	_ = frontier.Advance(1, 12, 120)
	offset, watermark, ready := frontier.Common()
	fmt.Println(offset, watermark, ready)
	// Output: 10 100 true
}

func TestMZ011PartitionOffsetFrontierTracksExactCompleteness(t *testing.T) {
	frontier, err := NewPartitionOffsetFrontier([]int32{3, 1, 2}, PartitionOffsetFrontierOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if frontier.Ready() {
		t.Fatal("new frontier is ready")
	}
	if _, _, ok := frontier.Common(); ok {
		t.Fatal("incomplete frontier returned a common point")
	}

	if err := frontier.Advance(1, 10, 100); err != nil {
		t.Fatal(err)
	}
	if err := frontier.Advance(2, 8, 80); err != nil {
		t.Fatal(err)
	}
	if frontier.Ready() {
		t.Fatal("partially initialized frontier is ready")
	}
	if err := frontier.Advance(3, 12, 120); err != nil {
		t.Fatal(err)
	}
	offset, watermark, ok := frontier.Common()
	if !ok || offset != 8 || watermark != 80 {
		t.Fatalf("common frontier = (%d, %d, %v), want (8, 80, true)", offset, watermark, ok)
	}

	snapshot := frontier.Snapshot()
	if !snapshot.Ready || snapshot.CommonOffset != 8 || snapshot.CommonWatermark != 80 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if len(snapshot.Entries) != 3 || snapshot.Entries[0].Partition != 1 || snapshot.Entries[1].Partition != 2 || snapshot.Entries[2].Partition != 3 {
		t.Fatalf("snapshot entries = %+v, want sorted partitions", snapshot.Entries)
	}
	snapshot.Entries[0].Offset = 999
	if got, _, _ := frontier.Common(); got != 8 {
		t.Fatalf("snapshot mutation changed frontier offset to %d", got)
	}

	if err := frontier.Advance(1, 10, 100); err != nil {
		t.Fatalf("equal advance = %v, want idempotent success", err)
	}
	if err := frontier.Advance(1, 9, 101); err != ErrPartitionOffsetRegression {
		t.Fatalf("offset regression = %v, want %v", err, ErrPartitionOffsetRegression)
	}
	if err := frontier.Advance(1, 11, 99); err != ErrPartitionOffsetRegression {
		t.Fatalf("watermark regression = %v, want %v", err, ErrPartitionOffsetRegression)
	}
}

func TestMZ011PartitionOffsetFrontierValidatesPartitionsAndBounds(t *testing.T) {
	if _, err := NewPartitionOffsetFrontier(nil, PartitionOffsetFrontierOptions{}); err != ErrPartitionOffsetFrontierOptionsInvalid {
		t.Fatalf("empty partitions = %v, want %v", err, ErrPartitionOffsetFrontierOptionsInvalid)
	}
	if _, err := NewPartitionOffsetFrontier([]int32{1, 1}, PartitionOffsetFrontierOptions{}); err != ErrPartitionOffsetPartitionDuplicate {
		t.Fatalf("duplicate partition = %v, want %v", err, ErrPartitionOffsetPartitionDuplicate)
	}
	if _, err := NewPartitionOffsetFrontier([]int32{-1}, PartitionOffsetFrontierOptions{}); err != ErrPartitionOffsetPartitionInvalid {
		t.Fatalf("negative partition = %v, want %v", err, ErrPartitionOffsetPartitionInvalid)
	}
	if _, err := NewPartitionOffsetFrontier([]int32{1}, PartitionOffsetFrontierOptions{MaxPartitions: -1}); err != ErrPartitionOffsetFrontierOptionsInvalid {
		t.Fatalf("negative max = %v, want %v", err, ErrPartitionOffsetFrontierOptionsInvalid)
	}
	if _, err := NewPartitionOffsetFrontier([]int32{1, 2}, PartitionOffsetFrontierOptions{MaxPartitions: 1}); err != ErrPartitionOffsetFrontierOptionsInvalid {
		t.Fatalf("partition limit = %v, want %v", err, ErrPartitionOffsetFrontierOptionsInvalid)
	}

	frontier, err := NewPartitionOffsetFrontier([]int32{1}, PartitionOffsetFrontierOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := frontier.Advance(2, 1, 1); err != ErrPartitionOffsetPartitionUnknown {
		t.Fatalf("unknown partition = %v, want %v", err, ErrPartitionOffsetPartitionUnknown)
	}
	if err := (*PartitionOffsetFrontier)(nil).Advance(1, 1, 1); err != ErrPartitionOffsetFrontierNil {
		t.Fatalf("nil advance = %v, want %v", err, ErrPartitionOffsetFrontierNil)
	}
}

func TestMZ011PartitionOffsetFrontierUsesDenseIndexForContiguousPartitions(t *testing.T) {
	dense, err := NewPartitionOffsetFrontier([]int32{2, 0, 1}, PartitionOffsetFrontierOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !dense.direct || dense.indexes != nil {
		t.Fatalf("dense layout = direct=%v indexes=%v, want direct with no map", dense.direct, dense.indexes)
	}
	sparse, err := NewPartitionOffsetFrontier([]int32{1, 3}, PartitionOffsetFrontierOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if sparse.direct || sparse.indexes == nil {
		t.Fatalf("sparse layout = direct=%v indexes=%v, want indexed map", sparse.direct, sparse.indexes)
	}
}
