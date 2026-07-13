package hatriecache

import (
	"encoding/json"
	"errors"
	"sort"
	"strconv"
)

const (
	DefaultTopKCapacity uint64 = 100
	maxTopKCapacity     uint64 = 1 << 20
)

// TopKItem is one tracked heavy-hitter candidate. Count is an upper-bound
// estimate, and Error is the maximum overcount introduced when an item replaced
// an older candidate in a full sketch.
type TopKItem struct {
	Value interface{} `json:"value"`
	Count uint64      `json:"count"`
	Error uint64      `json:"error"`
}

type TopKEstimate struct {
	Tracked bool   `json:"tracked"`
	Count   uint64 `json:"count"`
	Error   uint64 `json:"error"`
}

type TopKInfo struct {
	Capacity uint64 `json:"capacity"`
	Tracked  uint64 `json:"tracked"`
	Total    uint64 `json:"total"`
	MinCount uint64 `json:"min_count"`
	MaxCount uint64 `json:"max_count"`
}

type topKSnapshot struct {
	Capacity uint64     `json:"capacity"`
	Total    uint64     `json:"total"`
	Items    []topKItem `json:"items"`
}

type topKItem struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
	Count uint64      `json:"count"`
	Error uint64      `json:"error"`
}

type topKData struct {
	capacity uint64
	total    uint64
	items    []topKItem
	byKey    map[string]int
}

func newTopKData(capacity uint64) (topKData, error) {
	if err := validateTopKCapacity(capacity); err != nil {
		return topKData{}, err
	}
	return topKData{
		capacity: capacity,
		items:    make([]topKItem, 0, int(capacity)),
		byKey:    make(map[string]int, int(capacity)),
	}, nil
}

func newDefaultTopKData() topKData {
	data, err := newTopKData(DefaultTopKCapacity)
	if err != nil {
		panic(err)
	}
	return data
}

func validateTopKCapacity(capacity uint64) error {
	if capacity == 0 {
		return errors.New("hatriecache: top-k capacity must be positive")
	}
	if capacity > maxTopKCapacity {
		return errors.New("hatriecache: top-k capacity is too large")
	}
	return nil
}

func validateTopKSnapshot(snapshot topKSnapshot) error {
	if err := validateTopKCapacity(snapshot.Capacity); err != nil {
		return err
	}
	if uint64(len(snapshot.Items)) > snapshot.Capacity {
		return errors.New("hatriecache: top-k snapshot has too many items")
	}
	seen := make(map[string]struct{}, len(snapshot.Items))
	for _, item := range snapshot.Items {
		if item.Key == "" {
			return errors.New("hatriecache: top-k snapshot item key is required")
		}
		if _, ok := seen[item.Key]; ok {
			return errors.New("hatriecache: duplicate top-k snapshot item")
		}
		derived, err := topKItemKey(item.Value)
		if err != nil {
			return err
		}
		if derived != item.Key {
			return errors.New("hatriecache: top-k snapshot item key does not match value")
		}
		if item.Error > item.Count {
			return errors.New("hatriecache: top-k snapshot item error exceeds count")
		}
		seen[item.Key] = struct{}{}
	}
	return nil
}

func newTopKDataFromSnapshot(snapshot topKSnapshot) (topKData, error) {
	if err := validateTopKSnapshot(snapshot); err != nil {
		return topKData{}, err
	}
	data := topKData{
		capacity: snapshot.Capacity,
		total:    snapshot.Total,
		items:    make([]topKItem, len(snapshot.Items)),
		byKey:    make(map[string]int, int(snapshot.Capacity)),
	}
	for idx, item := range snapshot.Items {
		data.items[idx] = topKItem{
			Key:   item.Key,
			Value: cloneValue(item.Value),
			Count: item.Count,
			Error: item.Error,
		}
		data.byKey[item.Key] = idx
	}
	for i := len(data.items)/2 - 1; i >= 0; i-- {
		data.siftDown(i)
	}
	return data, nil
}

func (top *topKData) Add(value interface{}, count uint64) TopKEstimate {
	if top == nil || top.capacity == 0 {
		return TopKEstimate{}
	}
	if count == 0 {
		return top.Estimate(value)
	}
	if top.byKey == nil {
		top.byKey = make(map[string]int, int(top.capacity))
	}
	key := mustTopKItemKey(value)
	top.total = saturatingAddUint64(top.total, count)
	if idx, ok := top.byKey[key]; ok {
		top.items[idx].Count = saturatingAddUint64(top.items[idx].Count, count)
		top.siftDown(idx)
		item := top.items[top.byKey[key]]
		return TopKEstimate{Tracked: true, Count: item.Count, Error: item.Error}
	}
	if uint64(len(top.items)) < top.capacity {
		item := topKItem{Key: key, Value: cloneValue(value), Count: count}
		top.items = append(top.items, item)
		idx := len(top.items) - 1
		top.byKey[key] = idx
		top.siftUp(idx)
		item = top.items[top.byKey[key]]
		return TopKEstimate{Tracked: true, Count: item.Count, Error: item.Error}
	}

	evicted := top.items[0]
	delete(top.byKey, evicted.Key)
	next := topKItem{
		Key:   key,
		Value: cloneValue(value),
		Count: saturatingAddUint64(evicted.Count, count),
		Error: evicted.Count,
	}
	top.items[0] = next
	top.byKey[key] = 0
	top.siftDown(0)
	item := top.items[top.byKey[key]]
	return TopKEstimate{Tracked: true, Count: item.Count, Error: item.Error}
}

func (top *topKData) AddOne(value interface{}, count uint64, values ...interface{}) TopKEstimate {
	estimate := top.Add(value, count)
	for _, value := range values {
		estimate = top.Add(value, count)
	}
	return estimate
}

func (top topKData) Estimate(value interface{}) TopKEstimate {
	key := mustTopKItemKey(value)
	idx, ok := top.byKey[key]
	if !ok {
		return TopKEstimate{}
	}
	item := top.items[idx]
	return TopKEstimate{Tracked: true, Count: item.Count, Error: item.Error}
}

func (top topKData) Items() []TopKItem {
	items := top.sortedItems()
	out := make([]TopKItem, len(items))
	for idx, item := range items {
		out[idx] = TopKItem{
			Value: cloneValue(item.Value),
			Count: item.Count,
			Error: item.Error,
		}
	}
	return out
}

func (top topKData) Snapshot() topKSnapshot {
	items := top.sortedItems()
	out := make([]topKItem, len(items))
	for idx, item := range items {
		out[idx] = topKItem{
			Key:   item.Key,
			Value: cloneValue(item.Value),
			Count: item.Count,
			Error: item.Error,
		}
	}
	return topKSnapshot{
		Capacity: top.capacity,
		Total:    top.total,
		Items:    out,
	}
}

func (top topKData) Info() TopKInfo {
	info := TopKInfo{
		Capacity: top.capacity,
		Tracked:  uint64(len(top.items)),
		Total:    top.total,
	}
	for idx, item := range top.items {
		if idx == 0 || item.Count < info.MinCount {
			info.MinCount = item.Count
		}
		if item.Count > info.MaxCount {
			info.MaxCount = item.Count
		}
	}
	return info
}

func (top topKData) EncodedSize() int64 {
	data, err := json.Marshal(top.Snapshot())
	if err != nil {
		return 0
	}
	return int64(len(data))
}

func (top topKData) sortedItems() []topKItem {
	if len(top.items) == 0 {
		return []topKItem{}
	}
	out := make([]topKItem, len(top.items))
	copy(out, top.items)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		if out[i].Error != out[j].Error {
			return out[i].Error < out[j].Error
		}
		return out[i].Key < out[j].Key
	})
	return out
}

func (top *topKData) siftUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if !topKHeapLess(top.items[idx], top.items[parent]) {
			return
		}
		top.swap(idx, parent)
		idx = parent
	}
}

func (top *topKData) siftDown(idx int) {
	for {
		left := 2*idx + 1
		if left >= len(top.items) {
			return
		}
		smallest := left
		right := left + 1
		if right < len(top.items) && topKHeapLess(top.items[right], top.items[left]) {
			smallest = right
		}
		if !topKHeapLess(top.items[smallest], top.items[idx]) {
			return
		}
		top.swap(idx, smallest)
		idx = smallest
	}
}

func (top *topKData) swap(i, j int) {
	top.items[i], top.items[j] = top.items[j], top.items[i]
	top.byKey[top.items[i].Key] = i
	top.byKey[top.items[j].Key] = j
}

func topKHeapLess(a, b topKItem) bool {
	if a.Count != b.Count {
		return a.Count < b.Count
	}
	if a.Error != b.Error {
		return a.Error > b.Error
	}
	return a.Key < b.Key
}

func mustTopKItemKey(value interface{}) string {
	key, err := topKItemKey(value)
	if err != nil {
		panic(err)
	}
	return key
}

func topKItemKey(value interface{}) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// TopKStorage stores heavy-hitter sketches outside the trie.
type TopKStorage struct {
	array     []topKData
	reusables reusableIndexes
}

func CreateTopKStorage() *TopKStorage {
	return &TopKStorage{
		array: []topKData{},
	}
}

func (store *TopKStorage) PutData(idx int32, value topKData) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = value
	store.reusables.Use(idx)
}

func (store *TopKStorage) AppendData(value topKData) int32 {
	store.array = append(store.array, value)
	return int32(len(store.array) - 1)
}

func (store *TopKStorage) AddData(value topKData) int32 {
	if idx, ok := store.reusables.Take(); ok {
		store.array[idx] = value
		return idx
	}
	return store.AppendData(value)
}

func (store *TopKStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = topKData{}
	store.reusables.Mark(idx)
	store.array = trimReusableTail(store.array, &store.reusables)
}

func (ht *HatTrie) UpsertTopK(key string, capacity uint64) error {
	data, err := newTopKData(capacity)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsTopK() {
		ht.topKs.PutData(hval.Index, data)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.topKs.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_TOP_K}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) AddTopK(key string, val interface{}, count uint64, vals ...interface{}) TopKEstimate {
	if count == 0 {
		return ht.EstimateTopK(key, val)
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsTopK() {
		estimate := ht.topKs.array[hval.Index].AddOne(val, count, vals...)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return estimate
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.topKs.AddData(newDefaultTopKData())
	estimate := ht.topKs.array[idx].AddOne(val, count, vals...)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_TOP_K}.toValue()
	ht.recordWriteLocked(key)
	return estimate
}

func (ht *HatTrie) EstimateTopK(key string, val interface{}) TopKEstimate {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsTopK() {
		ht.recordReadLocked(false, key)
		return TopKEstimate{}
	}
	estimate := ht.topKs.array[hval.Index].Estimate(val)
	ht.recordReadLocked(estimate.Tracked, key)
	return estimate
}

func (ht *HatTrie) GetTopK(key string) []TopKItem {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsTopK() {
		ht.recordReadLocked(false, key)
		return nil
	}
	ht.recordReadLocked(true, key)
	return ht.topKs.array[hval.Index].Items()
}

func (ht *HatTrie) TopKInfo(key string) (TopKInfo, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsTopK() {
		ht.recordReadLocked(false, key)
		return TopKInfo{}, false
	}
	ht.recordReadLocked(true, key)
	return ht.topKs.array[hval.Index].Info(), true
}

func topKCapacityValue(value uint64) (uint64, error) {
	if value == 0 || value > maxTopKCapacity {
		return 0, errors.New("hatriecache: top-k capacity must be between 1 and " + strconv.FormatUint(maxTopKCapacity, 10))
	}
	return value, nil
}
