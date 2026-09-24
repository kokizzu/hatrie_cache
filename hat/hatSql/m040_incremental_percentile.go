package hatSql

import (
	"errors"
	"fmt"
	"math"
	"reflect"
)

var (
	ErrIncrementalPercentileNil                  = errors.New("incremental percentile operator is nil")
	ErrIncrementalPercentileOrderKeyRequired     = errors.New("incremental percentile order key is required")
	ErrIncrementalPercentileKeyRequired          = errors.New("incremental percentile differential row key is required")
	ErrIncrementalPercentileInvalid              = errors.New("incremental percentile must be between zero and one")
	ErrIncrementalPercentileNegativeMultiplicity = errors.New("incremental percentile multiplicity became negative")
	ErrIncrementalPercentileOverflow             = errors.New("incremental percentile multiplicity overflowed")
	ErrIncrementalPercentileRowConflict          = errors.New("incremental percentile row conflicts with existing key")
)

// IncrementalPercentileDefinition configures an exact weighted percentile
// operator. Values are ordered ascending by OrderKey, with stable key order
// resolving equal values.
type IncrementalPercentileDefinition struct {
	OrderKey IncrementalWindowOrderKeyFunc
}

// IncrementalPercentile maintains weighted order statistics under signed
// differential updates. Percentile uses nearest-rank semantics: p=0 selects
// the first row, p=1 selects the last row, and other values select
// ceil(p*totalWeight).
type IncrementalPercentile struct {
	orderKey IncrementalWindowOrderKeyFunc
	root     *incrementalPercentileNode
	entries  map[string]*incrementalPercentileNode
	total    uint64
	priority uint64
}

type incrementalPercentileNode struct {
	key      string
	time     uint64
	row      Row
	order    interface{}
	count    uint64
	weight   uint64
	priority uint64
	left     *incrementalPercentileNode
	right    *incrementalPercentileNode
}

type incrementalPercentilePendingEntry struct {
	active bool
	time   uint64
	row    Row
	order  interface{}
	count  uint64
}

type incrementalPercentilePreparedUpdate struct {
	update DifferentialRow
	row    Row
	order  interface{}
}

// NewIncrementalPercentile creates an empty exact percentile operator.
func NewIncrementalPercentile(definition IncrementalPercentileDefinition) (*IncrementalPercentile, error) {
	if definition.OrderKey == nil {
		return nil, ErrIncrementalPercentileOrderKeyRequired
	}
	return &IncrementalPercentile{
		orderKey: definition.OrderKey,
		entries:  make(map[string]*incrementalPercentileNode),
		priority: 0x9e3779b97f4a7c15,
	}, nil
}

// Apply validates and applies signed weighted row updates atomically. It
// retains one node per logical key, so duplicate multiplicity does not create
// duplicate row storage.
func (percentile *IncrementalPercentile) Apply(updates []DifferentialRow) error {
	if percentile == nil {
		return ErrIncrementalPercentileNil
	}
	if len(updates) == 0 {
		return nil
	}
	if len(updates) == 1 {
		return percentile.applySingle(updates[0])
	}
	if len(updates) == 2 && updates[0].Key == updates[1].Key && updates[0].Diff < 0 && updates[1].Diff > 0 {
		if _, exists := percentile.entries[updates[0].Key]; exists {
			return percentile.applyReplacement(updates)
		}
	}

	pending := make(map[string]incrementalPercentilePendingEntry, len(updates))
	prepared := make([]incrementalPercentilePreparedUpdate, 0, len(updates))
	workingTotal := percentile.total
	for index, update := range updates {
		if update.Key == "" {
			return fmt.Errorf("incremental percentile update %d: %w", index, ErrIncrementalPercentileKeyRequired)
		}
		if update.Diff == 0 {
			continue
		}

		entry, ok := pending[update.Key]
		if !ok {
			if current, exists := percentile.entries[update.Key]; exists {
				entry = incrementalPercentilePendingEntry{
					active: true,
					time:   current.time,
					row:    current.row,
					order:  current.order,
					count:  current.count,
				}
			}
		}

		preparedUpdate := incrementalPercentilePreparedUpdate{update: update}
		if update.Diff > 0 {
			wasActive := entry.active
			if !wasActive {
				order, err := percentile.orderKey(update.Row)
				if err != nil {
					return fmt.Errorf("incremental percentile update %d key %q order key: %w", index, update.Key, err)
				}
				entry = incrementalPercentilePendingEntry{
					active: true,
					time:   update.Time,
					row:    cloneDifferentialRow(update.Row),
					order:  cloneIncrementalTopKOrder(order),
				}
				preparedUpdate.row = entry.row
				preparedUpdate.order = entry.order
			} else if update.Row != nil {
				order, err := percentile.orderKey(update.Row)
				if err != nil {
					return fmt.Errorf("incremental percentile update %d key %q order key: %w", index, update.Key, err)
				}
				if Compare(order, entry.order) != 0 || !reflect.DeepEqual(update.Row, entry.row) {
					return fmt.Errorf("incremental percentile update %d key %q: %w", index, update.Key, ErrIncrementalPercentileRowConflict)
				}
			}
			next, ok := incrementalPercentileAddMultiplicity(entry.count, update.Diff)
			if !ok {
				return fmt.Errorf("incremental percentile update %d key %q: %w", index, update.Key, ErrIncrementalPercentileOverflow)
			}
			increment := uint64(update.Diff)
			if ^uint64(0)-workingTotal < increment {
				return fmt.Errorf("incremental percentile update %d key %q: %w", index, update.Key, ErrIncrementalPercentileOverflow)
			}
			workingTotal += increment
			entry.count = next
		} else {
			if !entry.active {
				return fmt.Errorf("incremental percentile update %d key %q: %w", index, update.Key, ErrIncrementalPercentileNegativeMultiplicity)
			}
			decrement := incrementalPercentileMagnitude(update.Diff)
			if decrement > entry.count {
				return fmt.Errorf("incremental percentile update %d key %q: %w", index, update.Key, ErrIncrementalPercentileNegativeMultiplicity)
			}
			entry.count -= decrement
			workingTotal -= decrement
			if entry.count == 0 {
				entry.active = false
			}
		}
		pending[update.Key] = entry
		prepared = append(prepared, preparedUpdate)
	}
	if len(prepared) == 0 {
		return nil
	}

	for _, preparedUpdate := range prepared {
		update := preparedUpdate.update
		if update.Diff > 0 {
			increment := uint64(update.Diff)
			if current, exists := percentile.entries[update.Key]; exists {
				current.count += increment
				percentile.root = incrementalPercentileSetCount(percentile.root, current, current.count)
			} else {
				current := &incrementalPercentileNode{
					key:      update.Key,
					time:     update.Time,
					row:      preparedUpdate.row,
					order:    preparedUpdate.order,
					count:    increment,
					priority: percentile.nextPriority(),
				}
				percentile.entries[update.Key] = current
				percentile.root = incrementalPercentileInsert(percentile.root, current)
			}
			percentile.total += increment
			continue
		}

		current := percentile.entries[update.Key]
		current.count -= incrementalPercentileMagnitude(update.Diff)
		percentile.total -= incrementalPercentileMagnitude(update.Diff)
		if current.count == 0 {
			percentile.root = incrementalPercentileErase(percentile.root, current)
			delete(percentile.entries, update.Key)
		} else {
			percentile.root = incrementalPercentileSetCount(percentile.root, current, current.count)
		}
	}
	return nil
}

func (percentile *IncrementalPercentile) applySingle(update DifferentialRow) error {
	if update.Key == "" {
		return fmt.Errorf("incremental percentile update 0: %w", ErrIncrementalPercentileKeyRequired)
	}
	if update.Diff == 0 {
		return nil
	}

	current, exists := percentile.entries[update.Key]
	if update.Diff > 0 {
		increment := uint64(update.Diff)
		if exists {
			if update.Row != nil {
				order, err := percentile.orderKey(update.Row)
				if err != nil {
					return fmt.Errorf("incremental percentile update 0 key %q order key: %w", update.Key, err)
				}
				if Compare(order, current.order) != 0 || !reflect.DeepEqual(update.Row, current.row) {
					return fmt.Errorf("incremental percentile update 0 key %q: %w", update.Key, ErrIncrementalPercentileRowConflict)
				}
			}
			nextCount, ok := incrementalPercentileAddMultiplicity(current.count, update.Diff)
			if !ok {
				return fmt.Errorf("incremental percentile update 0 key %q: %w", update.Key, ErrIncrementalPercentileOverflow)
			}
			if ^uint64(0)-percentile.total < increment {
				return fmt.Errorf("incremental percentile update 0 key %q: %w", update.Key, ErrIncrementalPercentileOverflow)
			}
			current.count = nextCount
			percentile.root = incrementalPercentileSetCount(percentile.root, current, current.count)
			percentile.total += increment
			return nil
		}

		order, err := percentile.orderKey(update.Row)
		if err != nil {
			return fmt.Errorf("incremental percentile update 0 key %q order key: %w", update.Key, err)
		}
		if ^uint64(0)-percentile.total < increment {
			return fmt.Errorf("incremental percentile update 0 key %q: %w", update.Key, ErrIncrementalPercentileOverflow)
		}
		current = &incrementalPercentileNode{
			key:      update.Key,
			time:     update.Time,
			row:      cloneDifferentialRow(update.Row),
			order:    cloneIncrementalTopKOrder(order),
			count:    increment,
			priority: percentile.nextPriority(),
		}
		percentile.entries[update.Key] = current
		percentile.root = incrementalPercentileInsert(percentile.root, current)
		percentile.total += increment
		return nil
	}

	if !exists {
		return fmt.Errorf("incremental percentile update 0 key %q: %w", update.Key, ErrIncrementalPercentileNegativeMultiplicity)
	}
	decrement := incrementalPercentileMagnitude(update.Diff)
	if decrement > current.count {
		return fmt.Errorf("incremental percentile update 0 key %q: %w", update.Key, ErrIncrementalPercentileNegativeMultiplicity)
	}
	current.count -= decrement
	percentile.total -= decrement
	if current.count == 0 {
		percentile.root = incrementalPercentileErase(percentile.root, current)
		delete(percentile.entries, update.Key)
	} else {
		percentile.root = incrementalPercentileSetCount(percentile.root, current, current.count)
	}
	return nil
}

func (percentile *IncrementalPercentile) applyReplacement(updates []DifferentialRow) error {
	remove := updates[0]
	insert := updates[1]
	current := percentile.entries[remove.Key]
	decrement := incrementalPercentileMagnitude(remove.Diff)
	if decrement > current.count {
		return fmt.Errorf("incremental percentile update 0 key %q: %w", remove.Key, ErrIncrementalPercentileNegativeMultiplicity)
	}
	remaining := current.count - decrement
	workingTotal := percentile.total - decrement
	increment := uint64(insert.Diff)
	if ^uint64(0)-workingTotal < increment {
		return fmt.Errorf("incremental percentile update 1 key %q: %w", insert.Key, ErrIncrementalPercentileOverflow)
	}
	nextCount, ok := incrementalPercentileAddMultiplicity(remaining, insert.Diff)
	if !ok {
		return fmt.Errorf("incremental percentile update 1 key %q: %w", insert.Key, ErrIncrementalPercentileOverflow)
	}

	var row Row
	var order interface{}
	if remaining > 0 {
		if insert.Row != nil {
			candidateOrder, err := percentile.orderKey(insert.Row)
			if err != nil {
				return fmt.Errorf("incremental percentile update 1 key %q order key: %w", insert.Key, err)
			}
			if Compare(candidateOrder, current.order) != 0 || !reflect.DeepEqual(insert.Row, current.row) {
				return fmt.Errorf("incremental percentile update 1 key %q: %w", insert.Key, ErrIncrementalPercentileRowConflict)
			}
		}
	} else {
		candidateOrder, err := percentile.orderKey(insert.Row)
		if err != nil {
			return fmt.Errorf("incremental percentile update 1 key %q order key: %w", insert.Key, err)
		}
		row = cloneDifferentialRow(insert.Row)
		order = cloneIncrementalTopKOrder(candidateOrder)
	}

	if remaining > 0 {
		current.count = nextCount
		percentile.root = incrementalPercentileSetCount(percentile.root, current, nextCount)
	} else {
		percentile.root = incrementalPercentileErase(percentile.root, current)
		current.time = insert.Time
		current.row = row
		current.order = order
		current.count = increment
		current.weight = 0
		current.left = nil
		current.right = nil
		percentile.root = incrementalPercentileInsert(percentile.root, current)
	}
	percentile.total = workingTotal + increment
	return nil
}

// Percentile returns the exact nearest-rank row for p in [0, 1]. It returns
// false without an error when the relation is empty.
func (percentile *IncrementalPercentile) Percentile(p float64) (DifferentialRow, bool, error) {
	if percentile == nil {
		return DifferentialRow{}, false, ErrIncrementalPercentileNil
	}
	rank, err := incrementalPercentileRank(p, percentile.total)
	if err != nil {
		return DifferentialRow{}, false, err
	}
	if rank == 0 {
		return DifferentialRow{}, false, nil
	}
	node := incrementalPercentileSelect(percentile.root, rank)
	if node == nil {
		return DifferentialRow{}, false, nil
	}
	return DifferentialRow{
		Key:  node.key,
		Time: node.time,
		Diff: 1,
		Row:  cloneDifferentialRow(node.row),
	}, true, nil
}

// Quantile is an alias for Percentile for callers using quantile terminology.
func (percentile *IncrementalPercentile) Quantile(p float64) (DifferentialRow, bool, error) {
	return percentile.Percentile(p)
}

// TotalWeight returns the sum of active row multiplicities.
func (percentile *IncrementalPercentile) TotalWeight() uint64 {
	if percentile == nil {
		return 0
	}
	return percentile.total
}

// Snapshot returns every active logical row in ascending order. Diff is the
// full stored multiplicity for each row.
func (percentile *IncrementalPercentile) Snapshot() []DifferentialRow {
	if percentile == nil || percentile.root == nil {
		return nil
	}
	result := make([]DifferentialRow, 0, len(percentile.entries))
	incrementalPercentileAppendAll(percentile.root, &result)
	return result
}

// AllRows is an alias for Snapshot for relation-style callers.
func (percentile *IncrementalPercentile) AllRows() []DifferentialRow {
	return percentile.Snapshot()
}

func (percentile *IncrementalPercentile) nextPriority() uint64 {
	x := percentile.priority
	x ^= x << 7
	x ^= x >> 9
	x ^= x << 8
	percentile.priority = x
	return x
}

func incrementalPercentileAddMultiplicity(current uint64, diff int64) (uint64, bool) {
	increment := uint64(diff)
	if current > uint64(math.MaxInt64)-increment {
		return 0, false
	}
	return current + increment, true
}

func incrementalPercentileMagnitude(diff int64) uint64 {
	return uint64(-(diff + 1)) + 1
}

func incrementalPercentileRank(p float64, total uint64) (uint64, error) {
	if math.IsNaN(p) || p < 0 || p > 1 {
		return 0, ErrIncrementalPercentileInvalid
	}
	if total == 0 {
		return 0, nil
	}
	if p <= 0 {
		return 1, nil
	}
	if p >= 1 {
		return total, nil
	}
	scaled := p * float64(total)
	if scaled <= 1 {
		return 1, nil
	}
	rank := math.Ceil(scaled)
	if rank >= float64(total) {
		return total, nil
	}
	return uint64(rank), nil
}

func incrementalPercentileBefore(left, right *incrementalPercentileNode) bool {
	comparison := Compare(left.order, right.order)
	if comparison != 0 {
		return comparison < 0
	}
	return left.key < right.key
}

func incrementalPercentileInsert(root, node *incrementalPercentileNode) *incrementalPercentileNode {
	if root == nil {
		incrementalPercentileRecompute(node)
		return node
	}
	if incrementalPercentileBefore(node, root) {
		root.left = incrementalPercentileInsert(root.left, node)
		if root.left.priority > root.priority {
			root = incrementalPercentileRotateRight(root)
		}
	} else {
		root.right = incrementalPercentileInsert(root.right, node)
		if root.right.priority > root.priority {
			root = incrementalPercentileRotateLeft(root)
		}
	}
	incrementalPercentileRecompute(root)
	return root
}

func incrementalPercentileErase(root, target *incrementalPercentileNode) *incrementalPercentileNode {
	if root == nil {
		return nil
	}
	if root == target {
		return incrementalPercentileMerge(root.left, root.right)
	}
	if incrementalPercentileBefore(target, root) {
		root.left = incrementalPercentileErase(root.left, target)
	} else {
		root.right = incrementalPercentileErase(root.right, target)
	}
	incrementalPercentileRecompute(root)
	return root
}

func incrementalPercentileSetCount(root, target *incrementalPercentileNode, count uint64) *incrementalPercentileNode {
	if root == nil {
		return nil
	}
	if root == target {
		root.count = count
		incrementalPercentileRecompute(root)
		return root
	}
	if incrementalPercentileBefore(target, root) {
		root.left = incrementalPercentileSetCount(root.left, target, count)
	} else {
		root.right = incrementalPercentileSetCount(root.right, target, count)
	}
	incrementalPercentileRecompute(root)
	return root
}

func incrementalPercentileMerge(left, right *incrementalPercentileNode) *incrementalPercentileNode {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	if left.priority > right.priority {
		left.right = incrementalPercentileMerge(left.right, right)
		incrementalPercentileRecompute(left)
		return left
	}
	right.left = incrementalPercentileMerge(left, right.left)
	incrementalPercentileRecompute(right)
	return right
}

func incrementalPercentileRotateRight(root *incrementalPercentileNode) *incrementalPercentileNode {
	left := root.left
	root.left = left.right
	incrementalPercentileRecompute(root)
	left.right = root
	incrementalPercentileRecompute(left)
	return left
}

func incrementalPercentileRotateLeft(root *incrementalPercentileNode) *incrementalPercentileNode {
	right := root.right
	root.right = right.left
	incrementalPercentileRecompute(root)
	right.left = root
	incrementalPercentileRecompute(right)
	return right
}

func incrementalPercentileRecompute(node *incrementalPercentileNode) {
	if node == nil {
		return
	}
	node.weight = node.count + incrementalPercentileNodeWeight(node.left) + incrementalPercentileNodeWeight(node.right)
}

func incrementalPercentileNodeWeight(node *incrementalPercentileNode) uint64 {
	if node == nil {
		return 0
	}
	return node.weight
}

func incrementalPercentileSelect(node *incrementalPercentileNode, rank uint64) *incrementalPercentileNode {
	if node == nil || rank == 0 || rank > node.weight {
		return nil
	}
	leftWeight := incrementalPercentileNodeWeight(node.left)
	if rank <= leftWeight {
		return incrementalPercentileSelect(node.left, rank)
	}
	rank -= leftWeight
	if rank <= node.count {
		return node
	}
	return incrementalPercentileSelect(node.right, rank-node.count)
}

func incrementalPercentileAppendAll(node *incrementalPercentileNode, result *[]DifferentialRow) {
	if node == nil {
		return
	}
	incrementalPercentileAppendAll(node.left, result)
	*result = append(*result, DifferentialRow{
		Key:  node.key,
		Time: node.time,
		Diff: int64(node.count),
		Row:  cloneDifferentialRow(node.row),
	})
	incrementalPercentileAppendAll(node.right, result)
}
