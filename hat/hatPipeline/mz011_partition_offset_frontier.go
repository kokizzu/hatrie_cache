package hatPipeline

import (
	"errors"
	"sort"
)

const (
	// DefaultPartitionOffsetFrontierMaxPartitions bounds the default number of
	// Kafka-style partitions tracked by one frontier.
	DefaultPartitionOffsetFrontierMaxPartitions = 256
	maxPartitionOffsetFrontierPartitions        = 1 << 20
)

var (
	// ErrPartitionOffsetFrontierNil indicates a method call on a nil frontier.
	ErrPartitionOffsetFrontierNil = errors.New("hatPipeline: partition offset frontier is nil")
	// ErrPartitionOffsetFrontierOptionsInvalid indicates an invalid partition
	// set or bound.
	ErrPartitionOffsetFrontierOptionsInvalid = errors.New("hatPipeline: partition offset frontier options are invalid")
	// ErrPartitionOffsetPartitionInvalid indicates a negative partition ID.
	ErrPartitionOffsetPartitionInvalid = errors.New("hatPipeline: partition offset frontier partition is invalid")
	// ErrPartitionOffsetPartitionDuplicate indicates a repeated partition ID.
	ErrPartitionOffsetPartitionDuplicate = errors.New("hatPipeline: partition offset frontier partition is duplicated")
	// ErrPartitionOffsetPartitionUnknown indicates an update for an unregistered
	// partition.
	ErrPartitionOffsetPartitionUnknown = errors.New("hatPipeline: partition offset frontier partition is unknown")
	// ErrPartitionOffsetRegression indicates a lower offset or watermark.
	ErrPartitionOffsetRegression = errors.New("hatPipeline: partition offset frontier regressed")
)

// PartitionOffsetFrontierOptions bounds one partition-offset frontier.
type PartitionOffsetFrontierOptions struct {
	// MaxPartitions is the maximum number of registered partitions. Zero uses
	// DefaultPartitionOffsetFrontierMaxPartitions.
	MaxPartitions int
}

// PartitionOffsetFrontierEntry is one partition's completed offset and
// monotone event-time watermark. Offset is the next offset to consume: an
// offset of N means records below N are complete for this partition.
type PartitionOffsetFrontierEntry struct {
	Partition   int32
	Offset      uint64
	Watermark   uint64
	Initialized bool
}

// PartitionOffsetFrontierSnapshot is a detached, partition-sorted view. The
// common values are valid only when Ready is true and are the component-wise
// minimum across all partitions.
type PartitionOffsetFrontierSnapshot struct {
	Entries         []PartitionOffsetFrontierEntry
	Ready           bool
	CommonOffset    uint64
	CommonWatermark uint64
}

// PartitionOffsetFrontier tracks exact per-partition offsets and watermarks.
// It is intentionally caller-owned, like FrontierAntichain: callers that
// update it from multiple goroutines must provide external synchronization.
// The dense entry slice keeps the hot update and readiness path allocation-free
// while the index map handles sparse Kafka partition IDs.
type PartitionOffsetFrontier struct {
	partitions  []PartitionOffsetFrontierEntry
	indexes     map[int32]int
	initialized int
	direct      bool
}

// NewPartitionOffsetFrontier creates a bounded, sorted partition frontier.
func NewPartitionOffsetFrontier(partitions []int32, options PartitionOffsetFrontierOptions) (*PartitionOffsetFrontier, error) {
	maxPartitions := options.MaxPartitions
	if maxPartitions == 0 {
		maxPartitions = DefaultPartitionOffsetFrontierMaxPartitions
	}
	if maxPartitions < 1 || maxPartitions > maxPartitionOffsetFrontierPartitions || len(partitions) == 0 || len(partitions) > maxPartitions {
		return nil, ErrPartitionOffsetFrontierOptionsInvalid
	}
	partitionIDs := append([]int32(nil), partitions...)
	sort.Slice(partitionIDs, func(left, right int) bool { return partitionIDs[left] < partitionIDs[right] })
	entries := make([]PartitionOffsetFrontierEntry, len(partitionIDs))
	direct := true
	for index, partition := range partitionIDs {
		if partition < 0 {
			return nil, ErrPartitionOffsetPartitionInvalid
		}
		if index > 0 && partition == partitionIDs[index-1] {
			return nil, ErrPartitionOffsetPartitionDuplicate
		}
		if partition != int32(index) {
			direct = false
		}
		entries[index].Partition = partition
	}
	var indexes map[int32]int
	if !direct {
		indexes = make(map[int32]int, len(partitionIDs))
		for index, partition := range partitionIDs {
			indexes[partition] = index
		}
	}
	return &PartitionOffsetFrontier{
		partitions: entries,
		indexes:    indexes,
		direct:     direct,
	}, nil
}

// Advance publishes a monotone offset and watermark for one registered
// partition. Equal values are idempotent. It does not allocate.
func (frontier *PartitionOffsetFrontier) Advance(partition int32, offset, watermark uint64) error {
	if frontier == nil {
		return ErrPartitionOffsetFrontierNil
	}
	if partition < 0 {
		return ErrPartitionOffsetPartitionInvalid
	}
	index, ok := frontier.partitionIndex(partition)
	if !ok {
		return ErrPartitionOffsetPartitionUnknown
	}
	entry := &frontier.partitions[index]
	if entry.Initialized && (offset < entry.Offset || watermark < entry.Watermark) {
		return ErrPartitionOffsetRegression
	}
	if !entry.Initialized {
		frontier.initialized++
		entry.Initialized = true
	}
	entry.Offset = offset
	entry.Watermark = watermark
	return nil
}

func (frontier *PartitionOffsetFrontier) partitionIndex(partition int32) (int, bool) {
	if frontier.direct {
		index := int(partition)
		return index, index >= 0 && index < len(frontier.partitions)
	}
	index, ok := frontier.indexes[partition]
	return index, ok
}

// Ready reports whether every registered partition has published at least one
// offset and watermark.
func (frontier *PartitionOffsetFrontier) Ready() bool {
	return frontier != nil && frontier.initialized == len(frontier.partitions)
}

// Common returns the component-wise minimum offset and watermark when every
// partition is initialized. It scans the compact entry slice and allocates
// nothing.
func (frontier *PartitionOffsetFrontier) Common() (offset, watermark uint64, ready bool) {
	if !frontier.Ready() {
		return 0, 0, false
	}
	offset = frontier.partitions[0].Offset
	watermark = frontier.partitions[0].Watermark
	for _, entry := range frontier.partitions[1:] {
		if entry.Offset < offset {
			offset = entry.Offset
		}
		if entry.Watermark < watermark {
			watermark = entry.Watermark
		}
	}
	return offset, watermark, true
}

// Snapshot returns a detached partition-sorted view and computes its common
// frontier once. It is the inspection/export path, not the hot update path.
func (frontier *PartitionOffsetFrontier) Snapshot() PartitionOffsetFrontierSnapshot {
	if frontier == nil {
		return PartitionOffsetFrontierSnapshot{}
	}
	snapshot := PartitionOffsetFrontierSnapshot{
		Entries: append([]PartitionOffsetFrontierEntry(nil), frontier.partitions...),
		Ready:   frontier.Ready(),
	}
	if snapshot.Ready {
		snapshot.CommonOffset, snapshot.CommonWatermark, _ = frontier.Common()
	}
	return snapshot
}
