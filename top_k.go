package hatriecache

import (
	"encoding/json"
	"errors"
	"fmt"
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
	estimate, _ := top.AddChecked(value, count)
	return estimate
}

func (top *topKData) AddChecked(value interface{}, count uint64) (TopKEstimate, error) {
	return top.AddOneChecked(value, count)
}

func (top *topKData) AddOne(value interface{}, count uint64, values ...interface{}) TopKEstimate {
	estimate, _ := top.AddOneChecked(value, count, values...)
	return estimate
}

func (top *topKData) AddOneChecked(value interface{}, count uint64, values ...interface{}) (TopKEstimate, error) {
	if top == nil || top.capacity == 0 {
		return TopKEstimate{}, nil
	}
	prepared, err := prepareTopKItems(value, values...)
	if err != nil {
		return TopKEstimate{}, err
	}
	if count == 0 {
		return top.estimateKey(prepared[len(prepared)-1].Key), nil
	}
	if top.byKey == nil {
		top.byKey = make(map[string]int, int(top.capacity))
	}
	estimate := TopKEstimate{}
	for _, item := range prepared {
		estimate = top.addPrepared(item, count)
	}
	return estimate, nil
}

func (top topKData) Estimate(value interface{}) TopKEstimate {
	estimate, _ := top.EstimateChecked(value)
	return estimate
}

func (top topKData) EstimateChecked(value interface{}) (TopKEstimate, error) {
	key, err := topKItemKey(value)
	if err != nil {
		return TopKEstimate{}, err
	}
	return top.estimateKey(key), nil
}

func (top topKData) estimateKey(key string) TopKEstimate {
	idx, ok := top.byKey[key]
	if !ok {
		return TopKEstimate{}
	}
	item := top.items[idx]
	return TopKEstimate{Tracked: true, Count: item.Count, Error: item.Error}
}

func (top *topKData) addPrepared(item topKItem, count uint64) TopKEstimate {
	top.total = saturatingAddUint64(top.total, count)
	if idx, ok := top.byKey[item.Key]; ok {
		top.items[idx].Count = saturatingAddUint64(top.items[idx].Count, count)
		top.siftDown(idx)
		item := top.items[top.byKey[item.Key]]
		return TopKEstimate{Tracked: true, Count: item.Count, Error: item.Error}
	}
	if uint64(len(top.items)) < top.capacity {
		item.Count = count
		top.items = append(top.items, item)
		idx := len(top.items) - 1
		top.byKey[item.Key] = idx
		top.siftUp(idx)
		item = top.items[top.byKey[item.Key]]
		return TopKEstimate{Tracked: true, Count: item.Count, Error: item.Error}
	}

	evicted := top.items[0]
	delete(top.byKey, evicted.Key)
	item.Count = saturatingAddUint64(evicted.Count, count)
	item.Error = evicted.Count
	top.items[0] = item
	top.byKey[item.Key] = 0
	top.siftDown(0)
	item = top.items[top.byKey[item.Key]]
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

func prepareTopKItems(value interface{}, values ...interface{}) ([]topKItem, error) {
	items := make([]topKItem, 0, 1+len(values))
	item, err := prepareTopKItem(value)
	if err != nil {
		return nil, err
	}
	items = append(items, item)
	for _, value := range values {
		item, err := prepareTopKItem(value)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func prepareTopKItem(value interface{}) (topKItem, error) {
	key, err := topKItemKey(value)
	if err != nil {
		return topKItem{}, err
	}
	return topKItem{
		Key:   key,
		Value: cloneValue(value),
	}, nil
}

func topKItemKey(value interface{}) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("hatriecache: unsupported top-k value: %w", err)
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
	estimate, _ := ht.AddTopKChecked(key, val, count, vals...)
	return estimate
}

func (ht *HatTrie) AddTopKChecked(key string, val interface{}, count uint64, vals ...interface{}) (TopKEstimate, error) {
	if count == 0 {
		return ht.EstimateTopKChecked(key, val)
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return TopKEstimate{}, err
	}
	if hval.IsTopK() {
		estimate, err := ht.topKs.array[hval.Index].AddOneChecked(val, count, vals...)
		if err != nil {
			return TopKEstimate{}, err
		}
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return estimate, nil
	}

	data := newDefaultTopKData()
	estimate, err := data.AddOneChecked(val, count, vals...)
	if err != nil {
		return TopKEstimate{}, err
	}
	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.topKs.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_TOP_K}.toValue()
	ht.recordWriteLocked(key)
	return estimate, nil
}

func (ht *HatTrie) EstimateTopK(key string, val interface{}) TopKEstimate {
	estimate, _ := ht.EstimateTopKChecked(key, val)
	return estimate
}

func (ht *HatTrie) EstimateTopKChecked(key string, val interface{}) (TopKEstimate, error) {
	valueKey, err := topKItemKey(val)
	if err != nil {
		return TopKEstimate{}, err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return TopKEstimate{}, err
	}
	if !hval.IsTopK() {
		ht.recordReadLocked(false, key)
		return TopKEstimate{}, nil
	}
	estimate := ht.topKs.array[hval.Index].estimateKey(valueKey)
	ht.recordReadLocked(estimate.Tracked, key)
	return estimate, nil
}

func (ht *HatTrie) GetTopK(key string) []TopKItem {
	items, ok, _ := ht.GetTopKChecked(key)
	if !ok {
		return nil
	}
	return items
}

func (ht *HatTrie) GetTopKChecked(key string) ([]TopKItem, bool, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, false, err
	}
	if !hval.IsTopK() {
		ht.recordReadLocked(false, key)
		return nil, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.topKs.array[hval.Index].Items(), true, nil
}

func (ht *HatTrie) TopKInfo(key string) (TopKInfo, bool) {
	info, ok, _ := ht.TopKInfoChecked(key)
	return info, ok
}

func (ht *HatTrie) TopKInfoChecked(key string) (TopKInfo, bool, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return TopKInfo{}, false, err
	}
	if !hval.IsTopK() {
		ht.recordReadLocked(false, key)
		return TopKInfo{}, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.topKs.array[hval.Index].Info(), true, nil
}

func topKCapacityValue(value uint64) (uint64, error) {
	if value == 0 || value > maxTopKCapacity {
		return 0, errors.New("hatriecache: top-k capacity must be between 1 and " + strconv.FormatUint(maxTopKCapacity, 10))
	}
	return value, nil
}
