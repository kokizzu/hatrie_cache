package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
)

var (
	ErrIncrementalTopKNil                  = errors.New("incremental top-k maintainer is nil")
	ErrIncrementalTopKInvalidLimit         = errors.New("incremental top-k limit must not be negative")
	ErrIncrementalTopKOrderKeyRequired     = errors.New("incremental top-k order key is required")
	ErrIncrementalTopKNegativeMultiplicity = errors.New("incremental top-k multiplicity became negative")
	ErrIncrementalTopKOverflow             = errors.New("incremental top-k multiplicity overflowed")
	ErrIncrementalTopKRowConflict          = errors.New("incremental top-k row conflicts with existing key")
)

const incrementalTopKSelectionScratchLimit = 4096

// IncrementalTopKDefinition configures an exact weighted Top-K maintainer.
// DifferentialRow.Key is the stable row identity. Positive updates add row
// multiplicity and negative updates retract it. Descending selects larger SQL
// order values first; ties are resolved by key in ascending byte order.
type IncrementalTopKDefinition struct {
	K          int
	OrderKey   IncrementalWindowOrderKeyFunc
	Descending bool
}

// IncrementalTopK maintains the first K weighted rows in SQL order under
// signed differential updates. Apply validates a complete batch before
// changing the published state. The maintainer is single-writer; callers that
// share it across goroutines must provide synchronization.
type IncrementalTopK struct {
	k          int
	orderKey   IncrementalWindowOrderKeyFunc
	descending bool
	root       *incrementalTopKNode
	entries    map[string]*incrementalTopKNode
	priority   uint64
	scratch    [2][]incrementalTopKSelection
}

type incrementalTopKNode struct {
	key      string
	time     uint64
	row      Row
	order    interface{}
	count    uint64
	priority uint64
	left     *incrementalTopKNode
	right    *incrementalTopKNode
}

type incrementalTopKPendingEntry struct {
	active bool
	time   uint64
	row    Row
	order  interface{}
	count  uint64
}

type incrementalTopKPreparedUpdate struct {
	update DifferentialRow
	row    Row
	order  interface{}
}

type incrementalTopKSelection struct {
	node  *incrementalTopKNode
	count uint64
}

// IncrementalTopKRankChange describes one row whose bounded Top-K rank or
// selected multiplicity changed. Ranks are one-based first occupied positions;
// zero means that the row is outside the bounded result. Weighted rows occupy
// one position per selected multiplicity, while Diff is the selected-count
// change rather than the full relation-count change.
type IncrementalTopKRankChange struct {
	Key         string
	Time        uint64
	Row         Row
	Diff        int64
	BeforeRank  int
	AfterRank   int
	BeforeCount int64
	AfterCount  int64
}

// NewIncrementalTopK creates an exact weighted Top-K maintainer.
func NewIncrementalTopK(definition IncrementalTopKDefinition) (*IncrementalTopK, error) {
	if definition.K < 0 {
		return nil, ErrIncrementalTopKInvalidLimit
	}
	if definition.OrderKey == nil {
		return nil, ErrIncrementalTopKOrderKeyRequired
	}
	return &IncrementalTopK{
		k:          definition.K,
		orderKey:   definition.OrderKey,
		descending: definition.Descending,
		entries:    make(map[string]*incrementalTopKNode),
		priority:   0x9e3779b97f4a7c15,
	}, nil
}

// Apply validates and applies signed row updates, returning only the change
// in the maintained Top-K result. The returned updates are ordered as
// removals from the previous result followed by additions to the new result.
// A row can be incremented without a payload; when a payload is supplied for
// an existing key it must be the same row and order value as the original.
func (topK *IncrementalTopK) Apply(updates []DifferentialRow) ([]DifferentialRow, error) {
	if topK == nil {
		return nil, ErrIncrementalTopKNil
	}
	before, after, err := topK.apply(updates)
	if err != nil {
		return nil, err
	}
	return topK.selectionChanges(before, after), nil
}

func (topK *IncrementalTopK) apply(updates []DifferentialRow) ([]incrementalTopKSelection, []incrementalTopKSelection, error) {
	if len(updates) == 0 {
		return nil, nil, nil
	}
	if len(updates) == 2 &&
		updates[0].Key == updates[1].Key &&
		updates[0].Diff < 0 && updates[1].Diff > 0 {
		if _, exists := topK.entries[updates[0].Key]; exists {
			return topK.applyReplacement(updates)
		}
	}

	pending := make(map[string]incrementalTopKPendingEntry, len(updates))
	prepared := make([]incrementalTopKPreparedUpdate, 0, len(updates))
	for index, update := range updates {
		if update.Key == "" {
			return nil, nil, fmt.Errorf("incremental top-k update %d: differential row key is required", index)
		}
		if update.Diff == 0 {
			continue
		}

		entry, ok := pending[update.Key]
		if !ok {
			if current, exists := topK.entries[update.Key]; exists {
				entry = incrementalTopKPendingEntry{
					active: true,
					time:   current.time,
					row:    current.row,
					order:  current.order,
					count:  current.count,
				}
			}
		}

		preparedUpdate := incrementalTopKPreparedUpdate{update: update}
		if update.Diff > 0 {
			if !entry.active {
				order, err := topK.orderKey(update.Row)
				if err != nil {
					return nil, nil, fmt.Errorf("incremental top-k update %d key %q order key: %w", index, update.Key, err)
				}
				entry = incrementalTopKPendingEntry{
					active: true,
					time:   update.Time,
					row:    cloneDifferentialRow(update.Row),
					order:  cloneIncrementalTopKOrder(order),
					count:  0,
				}
				preparedUpdate.row = entry.row
				preparedUpdate.order = entry.order
			} else if update.Row != nil {
				order, err := topK.orderKey(update.Row)
				if err != nil {
					return nil, nil, fmt.Errorf("incremental top-k update %d key %q order key: %w", index, update.Key, err)
				}
				if Compare(order, entry.order) != 0 || !reflect.DeepEqual(update.Row, entry.row) {
					return nil, nil, fmt.Errorf("incremental top-k update %d key %q: %w", index, update.Key, ErrIncrementalTopKRowConflict)
				}
			}
			next, ok := incrementalTopKAddMultiplicity(entry.count, update.Diff)
			if !ok {
				return nil, nil, fmt.Errorf("incremental top-k update %d key %q: %w", index, update.Key, ErrIncrementalTopKOverflow)
			}
			entry.count = next
		} else {
			if !entry.active {
				return nil, nil, fmt.Errorf("incremental top-k update %d key %q: %w", index, update.Key, ErrIncrementalTopKNegativeMultiplicity)
			}
			decrement := incrementalTopKMagnitude(update.Diff)
			if decrement > entry.count {
				return nil, nil, fmt.Errorf("incremental top-k update %d key %q: %w", index, update.Key, ErrIncrementalTopKNegativeMultiplicity)
			}
			entry.count -= decrement
			if entry.count == 0 {
				entry.active = false
			}
		}
		pending[update.Key] = entry
		prepared = append(prepared, preparedUpdate)
	}
	if len(prepared) == 0 {
		return nil, nil, nil
	}

	before := topK.selectedRowsInto(&topK.scratch[0])
	for _, preparedUpdate := range prepared {
		update := preparedUpdate.update
		if update.Diff > 0 {
			increment := uint64(update.Diff)
			if current, exists := topK.entries[update.Key]; exists {
				current.count += increment
				topK.root = incrementalTopKSetCount(topK.root, current, current.count, topK.descending)
				continue
			}
			current := &incrementalTopKNode{
				key:      update.Key,
				time:     update.Time,
				row:      preparedUpdate.row,
				order:    preparedUpdate.order,
				count:    increment,
				priority: topK.nextPriority(),
			}
			topK.entries[update.Key] = current
			topK.root = incrementalTopKInsert(topK.root, current, topK.descending)
			continue
		}

		current := topK.entries[update.Key]
		current.count -= incrementalTopKMagnitude(update.Diff)
		if current.count == 0 {
			topK.root = incrementalTopKErase(topK.root, current, topK.descending)
			delete(topK.entries, update.Key)
		} else {
			topK.root = incrementalTopKSetCount(topK.root, current, current.count, topK.descending)
		}
	}

	after := topK.selectedRowsInto(&topK.scratch[1])
	return before, after, nil
}

// applyReplacement handles a delete-then-insert transition for one retained
// key without constructing the generic pending map and prepared slice. It
// returns the same before/after selections as apply so both public APIs keep
// their existing change semantics.
func (topK *IncrementalTopK) applyReplacement(updates []DifferentialRow) ([]incrementalTopKSelection, []incrementalTopKSelection, error) {
	remove := updates[0]
	insert := updates[1]
	if remove.Key == "" {
		return nil, nil, fmt.Errorf("incremental top-k update 0: differential row key is required")
	}
	if insert.Key == "" {
		return nil, nil, fmt.Errorf("incremental top-k update 1: differential row key is required")
	}
	current := topK.entries[remove.Key]
	if current == nil || current.count == 0 {
		return nil, nil, fmt.Errorf("incremental top-k update 0 key %q: %w", remove.Key, ErrIncrementalTopKNegativeMultiplicity)
	}
	decrement := incrementalTopKMagnitude(remove.Diff)
	if decrement > current.count {
		return nil, nil, fmt.Errorf("incremental top-k update 0 key %q: %w", remove.Key, ErrIncrementalTopKNegativeMultiplicity)
	}
	remaining := current.count - decrement

	row := current.row
	order := current.order
	time := current.time
	if remaining > 0 {
		if insert.Row != nil {
			candidateOrder, err := topK.orderKey(insert.Row)
			if err != nil {
				return nil, nil, fmt.Errorf("incremental top-k update 1 key %q order key: %w", insert.Key, err)
			}
			if Compare(candidateOrder, current.order) != 0 || !reflect.DeepEqual(insert.Row, current.row) {
				return nil, nil, fmt.Errorf("incremental top-k update 1 key %q: %w", insert.Key, ErrIncrementalTopKRowConflict)
			}
		}
	} else {
		candidateOrder, err := topK.orderKey(insert.Row)
		if err != nil {
			return nil, nil, fmt.Errorf("incremental top-k update 1 key %q order key: %w", insert.Key, err)
		}
		row = cloneDifferentialRow(insert.Row)
		order = cloneIncrementalTopKOrder(candidateOrder)
		time = insert.Time
	}
	nextCount, ok := incrementalTopKAddMultiplicity(remaining, insert.Diff)
	if !ok {
		return nil, nil, fmt.Errorf("incremental top-k update 1 key %q: %w", insert.Key, ErrIncrementalTopKOverflow)
	}

	before := topK.selectedRowsInto(&topK.scratch[0])
	if remaining > 0 {
		current.count = nextCount
		topK.root = incrementalTopKSetCount(topK.root, current, nextCount, topK.descending)
	} else {
		topK.root = incrementalTopKErase(topK.root, current, topK.descending)
		delete(topK.entries, remove.Key)
		replacement := &incrementalTopKNode{
			key:      insert.Key,
			time:     time,
			row:      row,
			order:    order,
			count:    nextCount,
			priority: topK.nextPriority(),
		}
		topK.entries[insert.Key] = replacement
		topK.root = incrementalTopKInsert(topK.root, replacement, topK.descending)
	}
	after := topK.selectedRowsInto(&topK.scratch[1])
	return before, after, nil
}

// ApplyWithRankChanges validates and applies signed row updates, returning
// every row whose bounded Top-K rank or selected multiplicity changed. Unlike
// Apply, this reports rank-only movement with Diff equal to zero. The update is
// atomic: an error leaves the maintainer unchanged and returns no changes.
func (topK *IncrementalTopK) ApplyWithRankChanges(updates []DifferentialRow) ([]IncrementalTopKRankChange, error) {
	if topK == nil {
		return nil, ErrIncrementalTopKNil
	}
	before, after, err := topK.apply(updates)
	if err != nil {
		return nil, err
	}
	return topK.rankChanges(before, after), nil
}

// Snapshot returns the current Top-K result in SQL order. A weighted row is
// returned once with Diff equal to its selected multiplicity.
func (topK *IncrementalTopK) Snapshot() []DifferentialRow {
	if topK == nil {
		return nil
	}
	selected := topK.selectedRows()
	if len(selected) == 0 {
		return nil
	}
	result := make([]DifferentialRow, 0, len(selected))
	for _, selection := range selected {
		result = append(result, DifferentialRow{
			Key:  selection.node.key,
			Time: selection.node.time,
			Diff: int64(selection.count),
			Row:  cloneDifferentialRow(selection.node.row),
		})
	}
	return result
}

// AllRows returns every active logical row in SQL order. Diff is the full
// stored multiplicity, unlike Snapshot which is capped by K.
func (topK *IncrementalTopK) AllRows() []DifferentialRow {
	if topK == nil || topK.root == nil {
		return nil
	}
	result := make([]DifferentialRow, 0, len(topK.entries))
	incrementalTopKAppendAll(topK.root, &result)
	return result
}

func (topK *IncrementalTopK) selectedRows() []incrementalTopKSelection {
	return topK.selectedRowsInto(nil)
}

func (topK *IncrementalTopK) selectedRowsInto(scratch *[]incrementalTopKSelection) []incrementalTopKSelection {
	if topK.k == 0 || topK.root == nil {
		return nil
	}
	capacity := len(topK.entries)
	if capacity > topK.k {
		capacity = topK.k
	}
	var result []incrementalTopKSelection
	if scratch != nil && capacity <= incrementalTopKSelectionScratchLimit {
		result = (*scratch)[:0]
		if cap(result) < capacity {
			result = make([]incrementalTopKSelection, 0, capacity)
		}
		*scratch = result
	} else {
		result = make([]incrementalTopKSelection, 0, capacity)
	}
	incrementalTopKAppendSelected(topK.root, uint64(topK.k), &result)
	if scratch != nil && capacity <= incrementalTopKSelectionScratchLimit {
		*scratch = result
	}
	return result
}

func incrementalTopKFindRank(selections []incrementalTopKSelection, key string) (int, uint64, *incrementalTopKNode, bool) {
	rank := 1
	for _, selection := range selections {
		if selection.node.key == key {
			return rank, selection.count, selection.node, true
		}
		rank += int(selection.count)
	}
	return 0, 0, nil, false
}

func (topK *IncrementalTopK) rankChanges(before, after []incrementalTopKSelection) []IncrementalTopKRankChange {
	if len(before) == 0 && len(after) == 0 {
		return nil
	}
	capacity := len(before)
	if capacity < len(after) {
		capacity = len(after)
	}
	var changes []IncrementalTopKRankChange
	rank := 1
	for _, previous := range before {
		afterRank, afterCount, currentNode, hasCurrent := incrementalTopKFindRank(after, previous.node.key)
		if rank != afterRank || previous.count != afterCount {
			node := previous.node
			if hasCurrent {
				node = currentNode
			}
			changes = incrementalTopKAppendRankChange(changes, capacity, IncrementalTopKRankChange{
				Key:         previous.node.key,
				Time:        node.time,
				Row:         cloneDifferentialRow(node.row),
				Diff:        int64(afterCount) - int64(previous.count),
				BeforeRank:  rank,
				AfterRank:   afterRank,
				BeforeCount: int64(previous.count),
				AfterCount:  int64(afterCount),
			})
		}
		rank += int(previous.count)
	}
	rank = 1
	for _, current := range after {
		if _, _, _, hadPrevious := incrementalTopKFindRank(before, current.node.key); hadPrevious {
			rank += int(current.count)
			continue
		}
		changes = incrementalTopKAppendRankChange(changes, capacity, IncrementalTopKRankChange{
			Key:         current.node.key,
			Time:        current.node.time,
			Row:         cloneDifferentialRow(current.node.row),
			Diff:        int64(current.count),
			BeforeRank:  0,
			AfterRank:   rank,
			BeforeCount: 0,
			AfterCount:  int64(current.count),
		})
		rank += int(current.count)
	}
	if len(changes) == 0 {
		return nil
	}
	return changes
}

func incrementalTopKAppendRankChange(changes []IncrementalTopKRankChange, capacity int, change IncrementalTopKRankChange) []IncrementalTopKRankChange {
	if changes == nil {
		changes = make([]IncrementalTopKRankChange, 0, capacity)
	}
	return append(changes, change)
}

func (topK *IncrementalTopK) selectionChanges(before, after []incrementalTopKSelection) []DifferentialRow {
	if len(before) == 0 && len(after) == 0 {
		return nil
	}
	if len(before) == len(after) {
		unchanged := true
		for index := range before {
			if before[index].node != after[index].node || before[index].count != after[index].count {
				unchanged = false
				break
			}
		}
		if unchanged {
			return nil
		}
	}
	beforeByKey := make(map[string]incrementalTopKSelection, len(before))
	for _, selection := range before {
		beforeByKey[selection.node.key] = selection
	}
	afterByKey := make(map[string]incrementalTopKSelection, len(after))
	for _, selection := range after {
		afterByKey[selection.node.key] = selection
	}

	changes := make([]DifferentialRow, 0, len(before)+len(after))
	for _, previous := range before {
		current, exists := afterByKey[previous.node.key]
		if !exists || current.node != previous.node {
			changes = append(changes, DifferentialRow{
				Key:  previous.node.key,
				Time: previous.node.time,
				Diff: -int64(previous.count),
				Row:  cloneDifferentialRow(previous.node.row),
			})
			continue
		}
		if previous.count > current.count {
			changes = append(changes, DifferentialRow{
				Key:  previous.node.key,
				Time: previous.node.time,
				Diff: -int64(previous.count - current.count),
				Row:  cloneDifferentialRow(previous.node.row),
			})
		}
	}
	for _, current := range after {
		previous, exists := beforeByKey[current.node.key]
		if !exists || previous.node != current.node {
			changes = append(changes, DifferentialRow{
				Key:  current.node.key,
				Time: current.node.time,
				Diff: int64(current.count),
				Row:  cloneDifferentialRow(current.node.row),
			})
			continue
		}
		if current.count > previous.count {
			changes = append(changes, DifferentialRow{
				Key:  current.node.key,
				Time: current.node.time,
				Diff: int64(current.count - previous.count),
				Row:  cloneDifferentialRow(current.node.row),
			})
		}
	}
	if len(changes) == 0 {
		return nil
	}
	return changes
}

func (topK *IncrementalTopK) nextPriority() uint64 {
	x := topK.priority
	x ^= x << 7
	x ^= x >> 9
	x ^= x << 8
	topK.priority = x
	return x
}

func incrementalTopKAddMultiplicity(current uint64, diff int64) (uint64, bool) {
	increment := uint64(diff)
	if current > uint64(math.MaxInt64)-increment {
		return 0, false
	}
	return current + increment, true
}

func incrementalTopKMagnitude(diff int64) uint64 {
	return uint64(-(diff + 1)) + 1
}

func cloneIncrementalTopKOrder(value interface{}) interface{} {
	if bytes, ok := value.([]byte); ok {
		clone := make([]byte, len(bytes))
		copy(clone, bytes)
		return clone
	}
	return value
}

func incrementalTopKBefore(left, right *incrementalTopKNode, descending bool) bool {
	comparison := Compare(left.order, right.order)
	if comparison != 0 {
		if descending {
			return comparison > 0
		}
		return comparison < 0
	}
	return left.key < right.key
}

func incrementalTopKInsert(root, node *incrementalTopKNode, descending bool) *incrementalTopKNode {
	if root == nil {
		return node
	}
	if incrementalTopKBefore(node, root, descending) {
		root.left = incrementalTopKInsert(root.left, node, descending)
		if root.left.priority > root.priority {
			root = incrementalTopKRotateRight(root)
		}
	} else {
		root.right = incrementalTopKInsert(root.right, node, descending)
		if root.right.priority > root.priority {
			root = incrementalTopKRotateLeft(root)
		}
	}
	return root
}

func incrementalTopKErase(root, target *incrementalTopKNode, descending bool) *incrementalTopKNode {
	if root == nil {
		return nil
	}
	if root == target {
		return incrementalTopKMerge(root.left, root.right)
	}
	if incrementalTopKBefore(target, root, descending) {
		root.left = incrementalTopKErase(root.left, target, descending)
	} else {
		root.right = incrementalTopKErase(root.right, target, descending)
	}
	return root
}

func incrementalTopKSetCount(root, target *incrementalTopKNode, count uint64, descending bool) *incrementalTopKNode {
	if root == nil {
		return nil
	}
	if root == target {
		root.count = count
		return root
	}
	if incrementalTopKBefore(target, root, descending) {
		root.left = incrementalTopKSetCount(root.left, target, count, descending)
	} else {
		root.right = incrementalTopKSetCount(root.right, target, count, descending)
	}
	return root
}

func incrementalTopKMerge(left, right *incrementalTopKNode) *incrementalTopKNode {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if left.priority > right.priority {
		left.right = incrementalTopKMerge(left.right, right)
		return left
	}
	right.left = incrementalTopKMerge(left, right.left)
	return right
}

func incrementalTopKRotateRight(root *incrementalTopKNode) *incrementalTopKNode {
	left := root.left
	root.left = left.right
	left.right = root
	return left
}

func incrementalTopKRotateLeft(root *incrementalTopKNode) *incrementalTopKNode {
	right := root.right
	root.right = right.left
	right.left = root
	return right
}

func incrementalTopKAppendSelected(node *incrementalTopKNode, remaining uint64, result *[]incrementalTopKSelection) uint64 {
	if node == nil || remaining == 0 {
		return remaining
	}
	remaining = incrementalTopKAppendSelected(node.left, remaining, result)
	if remaining == 0 {
		return 0
	}
	count := node.count
	if count > remaining {
		count = remaining
	}
	*result = append(*result, incrementalTopKSelection{node: node, count: count})
	remaining -= count
	return incrementalTopKAppendSelected(node.right, remaining, result)
}

func incrementalTopKAppendAll(node *incrementalTopKNode, result *[]DifferentialRow) {
	if node == nil {
		return
	}
	incrementalTopKAppendAll(node.left, result)
	*result = append(*result, DifferentialRow{
		Key:  node.key,
		Time: node.time,
		Diff: int64(node.count),
		Row:  cloneDifferentialRow(node.row),
	})
	incrementalTopKAppendAll(node.right, result)
}
