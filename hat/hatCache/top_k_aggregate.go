package hatCache

import (
	"errors"
	"fmt"

	json "github.com/goccy/go-json"
	hatDataStructure "hatrie_cache/hat/hatDataStructure"
)

const topKAggregateStateVersion uint64 = 1

var (
	// ErrTopKNil indicates that Merge was called on a nil receiver.
	ErrTopKNil = errors.New("hatriecache: top-k receiver is nil")
	// ErrTopKCapacityMismatch indicates that two configured sketches have
	// different capacities.
	ErrTopKCapacityMismatch = errors.New("hatriecache: top-k capacity mismatch")
	// ErrTopKStateInvalid indicates invalid candidate metadata or counts.
	ErrTopKStateInvalid = errors.New("hatriecache: top-k state is invalid")
)

// TopK is the importable bounded approximate heavy-hitter sketch. It is an
// alias of the command-backed implementation, so Add, Estimate, Items, and
// Info retain the same behavior as the existing TOPK commands.
type TopK = topKData

// NewTopK constructs an empty bounded approximate top-K sketch.
func NewTopK(capacity uint64) (TopK, error) {
	return newTopKData(capacity)
}

// Merge combines two sketches without replaying their input values. Common
// candidates add counts and errors. A candidate present on only one side adds
// the other side's minimum count to both its estimate and uncertainty, which
// preserves the Space-Saving upper-bound interpretation.
func (top *topKData) Merge(other TopK) error {
	if top == nil {
		return ErrTopKNil
	}
	merged, err := mergeTopKData(*top, other)
	if err != nil {
		return err
	}
	*top = merged
	return nil
}

// MarshalAggregateState encodes the bounded TopK state in a versioned HAG1
// envelope. The inner snapshot remains JSON-compatible for arbitrary values;
// the envelope supplies explicit kind/version and bounded corruption checks.
func (top topKData) MarshalAggregateState() ([]byte, error) {
	if err := validateTopKData(top); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(top.Snapshot())
	if err != nil {
		return nil, err
	}
	return hatDataStructure.MarshalAggregateStateEnvelope(hatDataStructure.AggregateStateKindTopK, topKAggregateStateVersion, payload)
}

// NewTopKFromAggregateState reconstructs a TopK sketch from a checked
// versioned aggregate-state envelope.
func NewTopKFromAggregateState(data []byte) (TopK, error) {
	envelope, err := hatDataStructure.UnmarshalAggregateStateEnvelope(data)
	if err != nil {
		return topKData{}, err
	}
	if envelope.Kind != hatDataStructure.AggregateStateKindTopK {
		return topKData{}, fmt.Errorf("%w: got %q want %q", hatDataStructure.ErrAggregateStateKindMismatch, envelope.Kind, hatDataStructure.AggregateStateKindTopK)
	}
	if envelope.Version != topKAggregateStateVersion {
		return topKData{}, fmt.Errorf("%w: got %d want %d", hatDataStructure.ErrAggregateStateVersionUnsupported, envelope.Version, topKAggregateStateVersion)
	}
	var snapshot topKSnapshot
	if err := json.Unmarshal(envelope.Payload, &snapshot); err != nil {
		return topKData{}, fmt.Errorf("%w: snapshot JSON: %v", ErrTopKStateInvalid, err)
	}
	return newTopKDataFromSnapshot(snapshot)
}

// MergeTopK imports or merges a bounded TopK state at key. It is atomic on
// validation or capacity errors and preserves the source sketch.
func (ht *HatTrie) MergeTopK(key string, other TopK) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.MergeTopK(key, other)
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()
	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsTopK() {
		merged, err := mergeTopKData(ht.topKs.array[hval.Index], other)
		if err != nil {
			return err
		}
		ht.topKs.array[hval.Index] = merged
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	merged, err := mergeTopKData(topKData{}, other)
	if err != nil {
		return err
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.topKs.AddData(merged)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_TOP_K}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func mergeTopKData(left, right topKData) (topKData, error) {
	if err := validateTopKData(left); err != nil {
		return topKData{}, err
	}
	if err := validateTopKData(right); err != nil {
		return topKData{}, err
	}
	if topKDataIsZero(left) {
		return cloneTopKData(right), nil
	}
	if topKDataIsZero(right) {
		return cloneTopKData(left), nil
	}
	if left.capacity != right.capacity {
		return topKData{}, fmt.Errorf("%w: left=%d right=%d", ErrTopKCapacityMismatch, left.capacity, right.capacity)
	}

	leftMinimum := topKMinimumCount(left)
	rightMinimum := topKMinimumCount(right)
	candidates := make(map[string]topKItem, len(left.items)+len(right.items))
	for _, item := range left.items {
		candidates[item.Key] = topKItem{
			Key:   item.Key,
			Value: cloneValue(item.Value),
			Count: saturatingAddUint64(item.Count, rightMinimum),
			Error: saturatingAddUint64(item.Error, rightMinimum),
		}
	}
	for _, item := range right.items {
		if existing, ok := candidates[item.Key]; ok {
			existing.Count = saturatingAddUint64(item.Count, findTopKOriginalCount(left, item.Key))
			existing.Error = saturatingAddUint64(item.Error, findTopKOriginalError(left, item.Key))
			candidates[item.Key] = existing
			continue
		}
		candidates[item.Key] = topKItem{
			Key:   item.Key,
			Value: cloneValue(item.Value),
			Count: saturatingAddUint64(item.Count, leftMinimum),
			Error: saturatingAddUint64(item.Error, leftMinimum),
		}
	}

	items := make([]topKItem, 0, len(candidates))
	for _, item := range candidates {
		items = append(items, item)
	}
	sortTopKItems(items)
	if uint64(len(items)) > left.capacity {
		items = items[:int(left.capacity)]
	}
	out := topKData{
		capacity: left.capacity,
		total:    saturatingAddUint64(left.total, right.total),
		items:    items,
	}
	if len(out.items) > topKInlineIndexSize {
		out.byKey = make(map[string]int, len(out.items))
		for idx, item := range out.items {
			out.byKey[item.Key] = idx
		}
	}
	for idx := len(out.items) / 2; idx >= 0; idx-- {
		out.siftDown(idx)
	}
	return out, nil
}

func validateTopKData(top topKData) error {
	if topKDataIsZero(top) {
		return nil
	}
	if err := validateTopKCapacity(top.capacity); err != nil {
		return fmt.Errorf("%w: %v", ErrTopKStateInvalid, err)
	}
	if uint64(len(top.items)) > top.capacity {
		return fmt.Errorf("%w: too many items", ErrTopKStateInvalid)
	}
	if len(top.items) == 0 {
		if top.total != 0 {
			return fmt.Errorf("%w: empty state has nonzero total", ErrTopKStateInvalid)
		}
		return nil
	}
	seen := make(map[string]struct{}, len(top.items))
	var countTotal uint64
	for _, item := range top.items {
		if item.Key == "" {
			return fmt.Errorf("%w: item key is required", ErrTopKStateInvalid)
		}
		if _, ok := seen[item.Key]; ok {
			return fmt.Errorf("%w: duplicate item", ErrTopKStateInvalid)
		}
		derived, err := topKItemKey(item.Value)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrTopKStateInvalid, err)
		}
		if derived != item.Key {
			return fmt.Errorf("%w: item key does not match value", ErrTopKStateInvalid)
		}
		if item.Count == 0 || item.Error > item.Count {
			return fmt.Errorf("%w: item count/error is invalid", ErrTopKStateInvalid)
		}
		if top.total != ^uint64(0) {
			if item.Count > top.total-countTotal {
				return fmt.Errorf("%w: item counts exceed total", ErrTopKStateInvalid)
			}
			countTotal += item.Count
		}
		seen[item.Key] = struct{}{}
	}
	if top.total != ^uint64(0) && countTotal != top.total {
		return fmt.Errorf("%w: item counts do not match total", ErrTopKStateInvalid)
	}
	return nil
}

func topKDataIsZero(top topKData) bool {
	return top.capacity == 0 && top.total == 0 && len(top.items) == 0 && top.byKey == nil
}

func cloneTopKData(top topKData) topKData {
	clone := topKData{capacity: top.capacity, total: top.total, items: make([]topKItem, len(top.items))}
	for idx, item := range top.items {
		clone.items[idx] = topKItem{Key: item.Key, Value: cloneValue(item.Value), Count: item.Count, Error: item.Error}
	}
	if len(clone.items) > topKInlineIndexSize {
		clone.byKey = make(map[string]int, len(clone.items))
		for idx, item := range clone.items {
			clone.byKey[item.Key] = idx
		}
	}
	return clone
}

func topKMinimumCount(top topKData) uint64 {
	if len(top.items) == 0 {
		return 0
	}
	return top.items[0].Count
}

func findTopKOriginalCount(top topKData, key string) uint64 {
	for _, item := range top.items {
		if item.Key == key {
			return item.Count
		}
	}
	return 0
}

func findTopKOriginalError(top topKData, key string) uint64 {
	for _, item := range top.items {
		if item.Key == key {
			return item.Error
		}
	}
	return 0
}

func sortTopKItems(items []topKItem) {
	for index := 1; index < len(items); index++ {
		item := items[index]
		position := index
		for position > 0 && topKItemsByRankLess(item, items[position-1]) {
			items[position] = items[position-1]
			position--
		}
		items[position] = item
	}
}

func topKItemsByRankLess(left, right topKItem) bool {
	if left.Count != right.Count {
		return left.Count > right.Count
	}
	if left.Error != right.Error {
		return left.Error < right.Error
	}
	return left.Key < right.Key
}
