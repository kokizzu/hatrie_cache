package hatDataStructure

import (
	"errors"
	"sort"
)

var (
	ErrTopKInvalidCapacity = errors.New("hatriecache: top-k capacity must be positive")
	ErrTopKInvalidSnapshot = errors.New("hatriecache: top-k snapshot is invalid")
)

// TopKEntry is an approximate frequency entry. The true frequency is between
// Count-Error and Count, inclusive.
type TopKEntry[K comparable] struct {
	Key   K      `json:"key"`
	Count uint64 `json:"count"`
	Error uint64 `json:"error"`
}

// TopKSnapshot is a portable bounded top-k state representation.
type TopKSnapshot[K comparable] struct {
	Capacity int            `json:"capacity"`
	Total    uint64         `json:"total"`
	Entries  []TopKEntry[K] `json:"entries"`
}

// TopK tracks frequent keys with a bounded Space-Saving summary. It is not
// safe for concurrent use; protect one instance when updates are concurrent.
type TopK[K comparable] struct {
	capacity  int
	total     uint64
	counters  []topKCounter[K]
	positions map[K]int
	nextOrder uint64
}

type topKCounter[K comparable] struct {
	key   K
	count uint64
	err   uint64
	order uint64
}

type topKOutput[K comparable] struct {
	entry TopKEntry[K]
	order uint64
}

// NewTopK creates a bounded approximate top-k counter.
func NewTopK[K comparable](capacity int) (TopK[K], error) {
	if capacity <= 0 {
		return TopK[K]{}, ErrTopKInvalidCapacity
	}
	return TopK[K]{
		capacity:  capacity,
		counters:  make([]topKCounter[K], 0, capacity),
		positions: make(map[K]int, capacity),
	}, nil
}

// NewTopKFromSnapshot validates and restores a bounded top-k counter.
func NewTopKFromSnapshot[K comparable](snapshot TopKSnapshot[K]) (TopK[K], error) {
	if snapshot.Capacity <= 0 || len(snapshot.Entries) > snapshot.Capacity {
		return TopK[K]{}, ErrTopKInvalidSnapshot
	}
	top, err := NewTopK[K](snapshot.Capacity)
	if err != nil {
		return TopK[K]{}, ErrTopKInvalidSnapshot
	}
	top.total = snapshot.Total
	for index, entry := range snapshot.Entries {
		if entry.Count == 0 || entry.Error > entry.Count {
			return TopK[K]{}, ErrTopKInvalidSnapshot
		}
		if _, exists := top.positions[entry.Key]; exists {
			return TopK[K]{}, ErrTopKInvalidSnapshot
		}
		top.positions[entry.Key] = index
		top.counters = append(top.counters, topKCounter[K]{
			key:   entry.Key,
			count: entry.Count,
			err:   entry.Error,
			order: uint64(index),
		})
	}
	if top.total < top.lowerBoundTotal() {
		return TopK[K]{}, ErrTopKInvalidSnapshot
	}
	top.nextOrder = uint64(len(top.counters))
	top.rebuildHeap()
	return top, nil
}

// Add records one occurrence of key.
func (top *TopK[K]) Add(key K) {
	if top == nil || top.capacity <= 0 {
		return
	}
	if top.total != ^uint64(0) {
		top.total++
	}
	if index, exists := top.positions[key]; exists {
		if top.counters[index].count != ^uint64(0) {
			top.counters[index].count++
		}
		top.siftDown(index)
		return
	}
	top.insertMissing(key, 1, 0)
}

// AddN records count occurrences of key without iterating once per occurrence.
func (top *TopK[K]) AddN(key K, count uint64) {
	if top == nil || top.capacity <= 0 || count == 0 {
		return
	}
	top.total = topKAddSaturating(top.total, count)
	if index, exists := top.positions[key]; exists {
		top.counters[index].count = topKAddSaturating(top.counters[index].count, count)
		top.siftDown(index)
		return
	}
	top.insertMissing(key, count, 0)
}

// Merge combines another top-k summary into top. The receiver capacity wins;
// entries that do not fit remain represented by the error bound of the
// replacement counter.
func (top *TopK[K]) Merge(other TopK[K]) error {
	if top == nil || top.capacity <= 0 || other.capacity <= 0 {
		return ErrTopKInvalidSnapshot
	}
	top.total = topKAddSaturating(top.total, other.total)
	for _, counter := range other.counters {
		top.mergeCounter(counter)
	}
	return nil
}

// Entries returns a copy sorted by descending estimated frequency. Ties use
// lower error first and then insertion order, so output is deterministic.
func (top *TopK[K]) Entries() []TopKEntry[K] {
	if top == nil || len(top.counters) == 0 {
		return []TopKEntry[K]{}
	}
	output := make([]topKOutput[K], len(top.counters))
	for index, counter := range top.counters {
		output[index] = topKOutput[K]{
			entry: TopKEntry[K]{Key: counter.key, Count: counter.count, Error: counter.err},
			order: counter.order,
		}
	}
	sort.Slice(output, func(left, right int) bool {
		if output[left].entry.Count != output[right].entry.Count {
			return output[left].entry.Count > output[right].entry.Count
		}
		if output[left].entry.Error != output[right].entry.Error {
			return output[left].entry.Error < output[right].entry.Error
		}
		return output[left].order < output[right].order
	})
	entries := make([]TopKEntry[K], len(output))
	for index, item := range output {
		entries[index] = item.entry
	}
	return entries
}

// Snapshot returns a portable copy of the current summary.
func (top *TopK[K]) Snapshot() TopKSnapshot[K] {
	if top == nil {
		return TopKSnapshot[K]{}
	}
	return TopKSnapshot[K]{
		Capacity: top.capacity,
		Total:    top.total,
		Entries:  top.Entries(),
	}
}

// Capacity reports the configured maximum number of tracked keys.
func (top *TopK[K]) Capacity() int {
	if top == nil {
		return 0
	}
	return top.capacity
}

// Total reports the number of observations recorded by the summary.
func (top *TopK[K]) Total() uint64 {
	if top == nil {
		return 0
	}
	return top.total
}

func (top *TopK[K]) insertMissing(key K, count, err uint64) {
	if len(top.counters) < top.capacity {
		index := len(top.counters)
		top.counters = append(top.counters, topKCounter[K]{
			key:   key,
			count: count,
			err:   err,
			order: top.newOrder(),
		})
		top.positions[key] = index
		top.siftUp(index)
		return
	}
	minimum := top.counters[0]
	delete(top.positions, minimum.key)
	top.counters[0] = topKCounter[K]{
		key:   key,
		count: topKAddSaturating(minimum.count, count),
		err:   topKAddSaturating(minimum.count, err),
		order: top.newOrder(),
	}
	top.positions[key] = 0
	top.siftDown(0)
}

func (top *TopK[K]) mergeCounter(counter topKCounter[K]) {
	if counter.count == 0 {
		return
	}
	if index, exists := top.positions[counter.key]; exists {
		top.counters[index].count = topKAddSaturating(top.counters[index].count, counter.count)
		top.counters[index].err = topKAddSaturating(top.counters[index].err, counter.err)
		top.siftDown(index)
		return
	}
	if len(top.counters) < top.capacity || counter.count > top.counters[0].count {
		top.insertMissing(counter.key, counter.count, counter.err)
	}
}

func (top *TopK[K]) newOrder() uint64 {
	order := top.nextOrder
	if top.nextOrder != ^uint64(0) {
		top.nextOrder++
	}
	return order
}

func (top *TopK[K]) lowerBoundTotal() uint64 {
	var lower uint64
	for _, counter := range top.counters {
		lower = topKAddSaturating(lower, counter.count-counter.err)
	}
	return lower
}

func (top *TopK[K]) less(left, right int) bool {
	if top.counters[left].count != top.counters[right].count {
		return top.counters[left].count < top.counters[right].count
	}
	return top.counters[left].order < top.counters[right].order
}

func (top *TopK[K]) swap(left, right int) {
	top.counters[left], top.counters[right] = top.counters[right], top.counters[left]
	top.positions[top.counters[left].key] = left
	top.positions[top.counters[right].key] = right
}

func (top *TopK[K]) siftUp(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if !top.less(index, parent) {
			return
		}
		top.swap(index, parent)
		index = parent
	}
}

func (top *TopK[K]) siftDown(index int) {
	for {
		left := index*2 + 1
		if left >= len(top.counters) {
			return
		}
		smallest := left
		right := left + 1
		if right < len(top.counters) && top.less(right, left) {
			smallest = right
		}
		if !top.less(smallest, index) {
			return
		}
		top.swap(index, smallest)
		index = smallest
	}
}

func (top *TopK[K]) rebuildHeap() {
	for index := len(top.counters)/2 - 1; index >= 0; index-- {
		top.siftDown(index)
	}
}

func topKAddSaturating(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
