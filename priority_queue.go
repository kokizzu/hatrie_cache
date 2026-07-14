package hatriecache

import (
	"encoding/json"
	"errors"
	"fmt"
)

// PriorityItem is one value stored in a priority queue. Lower priority values
// are returned first; equal priorities keep insertion order.
type PriorityItem struct {
	Priority int64       `json:"priority"`
	Value    interface{} `json:"value"`
}

type PriorityQueue []PriorityItem

type priorityQueueItem struct {
	Priority int64       `json:"priority"`
	Sequence uint64      `json:"sequence"`
	Value    interface{} `json:"value"`
}

type priorityQueueData struct {
	items        []priorityQueueItem
	nextSequence uint64
}

var errPriorityQueueSequenceExhausted = errors.New("hatriecache: priority queue sequence is exhausted")

func clonePriorityQueue(value PriorityQueue) PriorityQueue {
	if value == nil {
		return nil
	}
	out := make(PriorityQueue, len(value))
	for idx, item := range value {
		out[idx] = PriorityItem{
			Priority: item.Priority,
			Value:    cloneValue(item.Value),
		}
	}
	return out
}

func validatePriorityQueueValue(value PriorityQueue) error {
	if _, err := json.Marshal(value); err != nil {
		return fmt.Errorf("hatriecache: unsupported priority queue value: %w", err)
	}
	return nil
}

func validatePriorityQueuePayload(value interface{}, values ...interface{}) error {
	if err := validatePriorityQueueItemValue(value); err != nil {
		return err
	}
	for _, value := range values {
		if err := validatePriorityQueueItemValue(value); err != nil {
			return err
		}
	}
	return nil
}

func validatePriorityQueueItemValue(value interface{}) error {
	if _, err := json.Marshal(value); err != nil {
		return fmt.Errorf("hatriecache: unsupported priority queue value: %w", err)
	}
	return nil
}

func newPriorityQueueData(values PriorityQueue) priorityQueueData {
	if len(values) == 0 {
		return priorityQueueData{}
	}
	out := priorityQueueData{
		items:        make([]priorityQueueItem, len(values)),
		nextSequence: uint64(len(values)),
	}
	for idx, value := range values {
		out.items[idx] = priorityQueueItem{
			Priority: value.Priority,
			Sequence: uint64(idx),
			Value:    cloneValue(value.Value),
		}
	}
	for i := len(out.items)/2 - 1; i >= 0; i-- {
		out.siftDown(i)
	}
	return out
}

func newPriorityQueueDataFromItems(values []priorityQueueItem) priorityQueueData {
	out := priorityQueueData{
		items: make([]priorityQueueItem, len(values)),
	}
	for idx, value := range values {
		out.items[idx] = priorityQueueItem{
			Priority: value.Priority,
			Sequence: value.Sequence,
			Value:    cloneValue(value.Value),
		}
	}
	for _, value := range values {
		if value.Sequence >= out.nextSequence {
			out.nextSequence = value.Sequence + 1
		}
	}
	for i := len(out.items)/2 - 1; i >= 0; i-- {
		out.siftDown(i)
	}
	return out
}

func (pq *priorityQueueData) Len() int {
	if pq == nil {
		return 0
	}
	return len(pq.items)
}

func (pq *priorityQueueData) PushOne(priority int64, value interface{}, values ...interface{}) int {
	added, _ := pq.PushOneChecked(priority, value, values...)
	return added
}

func (pq *priorityQueueData) PushOneChecked(priority int64, value interface{}, values ...interface{}) (int, error) {
	count := 1 + len(values)
	if err := pq.ensureSequenceCapacity(count); err != nil {
		return 0, err
	}
	if len(values) > 0 {
		pq.reserveCapacity(len(pq.items) + count)
	}
	pq.pushValue(priority, value)
	for _, value := range values {
		pq.pushValue(priority, value)
	}
	return count, nil
}

func (pq *priorityQueueData) ensureSequenceCapacity(count int) error {
	if count <= 0 {
		return nil
	}
	if pq.nextSequence == ^uint64(0) {
		return errPriorityQueueSequenceExhausted
	}
	if uint64(count-1) >= ^uint64(0)-pq.nextSequence {
		return errPriorityQueueSequenceExhausted
	}
	return nil
}

func (pq *priorityQueueData) reserveCapacity(needed int) {
	if cap(pq.items) >= needed {
		return
	}
	next := make([]priorityQueueItem, len(pq.items), needed)
	copy(next, pq.items)
	pq.items = next
}

func (pq *priorityQueueData) pushValue(priority int64, value interface{}) {
	item := priorityQueueItem{
		Priority: priority,
		Sequence: pq.nextSequence,
		Value:    cloneValue(value),
	}
	pq.nextSequence++
	pq.items = append(pq.items, item)
	pq.siftUp(len(pq.items) - 1)
}

func (pq *priorityQueueData) Peek() (PriorityItem, bool) {
	if pq == nil || len(pq.items) == 0 {
		return PriorityItem{}, false
	}
	return pq.items[0].PriorityItem(), true
}

func (pq *priorityQueueData) Pop() (PriorityItem, bool) {
	item, ok := pq.popItem()
	if !ok {
		return PriorityItem{}, false
	}
	return item.PriorityItem(), true
}

func (pq *priorityQueueData) popItem() (priorityQueueItem, bool) {
	if pq == nil || len(pq.items) == 0 {
		return priorityQueueItem{}, false
	}

	root := pq.items[0]
	last := len(pq.items) - 1
	pq.items[0] = pq.items[last]
	pq.items[last].Value = nil
	pq.items = pq.items[:last]
	if len(pq.items) > 0 {
		pq.siftDown(0)
	}
	pq.compactIfSparse()
	return root, true
}

func (pq *priorityQueueData) Items() PriorityQueue {
	if pq == nil {
		return nil
	}
	if len(pq.items) == 0 {
		return make(PriorityQueue, 0)
	}

	copyData := priorityQueueData{
		items:        make([]priorityQueueItem, len(pq.items)),
		nextSequence: pq.nextSequence,
	}
	copy(copyData.items, pq.items)
	out := make(PriorityQueue, 0, len(copyData.items))
	for {
		item, ok := copyData.Pop()
		if !ok {
			break
		}
		out = append(out, item)
	}
	return out
}

func (pq *priorityQueueData) SnapshotItems() []priorityQueueItem {
	if pq == nil {
		return nil
	}
	if len(pq.items) == 0 {
		return []priorityQueueItem{}
	}

	copyData := priorityQueueData{
		items:        make([]priorityQueueItem, len(pq.items)),
		nextSequence: pq.nextSequence,
	}
	copy(copyData.items, pq.items)
	out := make([]priorityQueueItem, 0, len(copyData.items))
	for len(copyData.items) > 0 {
		item, _ := copyData.popItem()
		item.Value = cloneValue(item.Value)
		out = append(out, item)
	}
	return out
}

func (pq *priorityQueueData) compactIfSparse() {
	if pq.items == nil {
		return
	}
	if len(pq.items) == 0 {
		pq.items = make([]priorityQueueItem, 0)
		return
	}
	if cap(pq.items) <= 16 || len(pq.items)*4 > cap(pq.items) {
		return
	}
	next := make([]priorityQueueItem, len(pq.items), maxInt(16, len(pq.items)*2))
	copy(next, pq.items)
	pq.items = next
}

func (pq *priorityQueueData) siftUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if !priorityQueueLess(pq.items[idx], pq.items[parent]) {
			return
		}
		pq.items[idx], pq.items[parent] = pq.items[parent], pq.items[idx]
		idx = parent
	}
}

func (pq *priorityQueueData) siftDown(idx int) {
	for {
		left := 2*idx + 1
		if left >= len(pq.items) {
			return
		}
		smallest := left
		right := left + 1
		if right < len(pq.items) && priorityQueueLess(pq.items[right], pq.items[left]) {
			smallest = right
		}
		if !priorityQueueLess(pq.items[smallest], pq.items[idx]) {
			return
		}
		pq.items[idx], pq.items[smallest] = pq.items[smallest], pq.items[idx]
		idx = smallest
	}
}

func priorityQueueLess(a, b priorityQueueItem) bool {
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}
	return a.Sequence < b.Sequence
}

func (item priorityQueueItem) PriorityItem() PriorityItem {
	return PriorityItem{
		Priority: item.Priority,
		Value:    cloneValue(item.Value),
	}
}

// PriorityQueueStorage stores priority queue values outside the trie.
type PriorityQueueStorage struct {
	array     []priorityQueueData
	reusables reusableIndexes
}

func CreatePriorityQueueStorage() *PriorityQueueStorage {
	return &PriorityQueueStorage{
		array: []priorityQueueData{},
	}
}

func (store *PriorityQueueStorage) Put(idx int32, value PriorityQueue) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = newPriorityQueueData(value)
	store.reusables.Use(idx)
}

func (store *PriorityQueueStorage) PutItems(idx int32, value []priorityQueueItem) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = newPriorityQueueDataFromItems(value)
	store.reusables.Use(idx)
}

func (store *PriorityQueueStorage) Append(value PriorityQueue) int32 {
	store.array = append(store.array, newPriorityQueueData(value))
	return int32(len(store.array) - 1)
}

func (store *PriorityQueueStorage) AppendItems(value []priorityQueueItem) int32 {
	store.array = append(store.array, newPriorityQueueDataFromItems(value))
	return int32(len(store.array) - 1)
}

func (store *PriorityQueueStorage) Add(value PriorityQueue) int32 {
	if idx, ok := store.reusables.Take(); ok {
		store.array[idx] = newPriorityQueueData(value)
		return idx
	}
	return store.Append(value)
}

func (store *PriorityQueueStorage) AddItems(value []priorityQueueItem) int32 {
	if idx, ok := store.reusables.Take(); ok {
		store.array[idx] = newPriorityQueueDataFromItems(value)
		return idx
	}
	return store.AppendItems(value)
}

func (store *PriorityQueueStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = priorityQueueData{}
	store.reusables.Mark(idx)
	store.array = trimReusableTail(store.array, &store.reusables)
}

func (ht *HatTrie) UpsertPriorityQueue(key string, val PriorityQueue) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	ht.upsertPriorityQueueLocked(key, val)
}

func (ht *HatTrie) UpsertPriorityQueueChecked(key string, val PriorityQueue) error {
	if err := validatePriorityQueueValue(val); err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	ht.upsertPriorityQueueLocked(key, val)
	return nil
}

func (ht *HatTrie) upsertPriorityQueueLocked(key string, val PriorityQueue) {
	rawPtr, hval := ht.upsertReplacementLocation(key)
	if hval.IsPriorityQueue() {
		ht.priorityQueues.Put(hval.Index, val)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.priorityQueues.Add(val)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_PRIORITY_QUEUE}.toValue()
	ht.recordWriteLocked(key)
}

func (ht *HatTrie) PushPriorityQueue(key string, priority int64, val interface{}, vals ...interface{}) int {
	added, _ := ht.pushPriorityQueue(key, priority, val, vals...)
	return added
}

func (ht *HatTrie) PushPriorityQueueChecked(key string, priority int64, val interface{}, vals ...interface{}) (int, error) {
	if err := validatePriorityQueuePayload(val, vals...); err != nil {
		return 0, err
	}
	return ht.pushPriorityQueue(key, priority, val, vals...)
}

func (ht *HatTrie) pushPriorityQueue(key string, priority int64, val interface{}, vals ...interface{}) (int, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return 0, err
	}
	if hval.IsPriorityQueue() {
		added, err := ht.priorityQueues.array[hval.Index].PushOneChecked(priority, val, vals...)
		if err != nil {
			return 0, err
		}
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return added, nil
	}

	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.priorityQueues.Add(nil)
	added, err := ht.priorityQueues.array[idx].PushOneChecked(priority, val, vals...)
	if err != nil {
		return 0, err
	}
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_PRIORITY_QUEUE}.toValue()
	ht.recordWriteLocked(key)
	return added, nil
}

func (ht *HatTrie) PeekPriorityQueue(key string) (PriorityItem, bool) {
	item, ok, _ := ht.PeekPriorityQueueChecked(key)
	return item, ok
}

func (ht *HatTrie) PeekPriorityQueueChecked(key string) (PriorityItem, bool, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return PriorityItem{}, false, err
	}
	if !hval.IsPriorityQueue() {
		ht.recordReadLocked(false, key)
		return PriorityItem{}, false, nil
	}
	item, ok := ht.priorityQueues.array[hval.Index].Peek()
	ht.recordReadLocked(ok, key)
	return item, ok, nil
}

func (ht *HatTrie) PopPriorityQueue(key string) (PriorityItem, bool) {
	item, ok, _ := ht.PopPriorityQueueChecked(key)
	return item, ok
}

func (ht *HatTrie) PopPriorityQueueChecked(key string) (PriorityItem, bool, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return PriorityItem{}, false, err
	}
	if !hval.IsPriorityQueue() {
		ht.recordReadLocked(false, key)
		return PriorityItem{}, false, nil
	}
	item, ok := ht.priorityQueues.array[hval.Index].Pop()
	if !ok {
		ht.recordReadLocked(false, key)
		return PriorityItem{}, false, nil
	}
	ht.recordReadLocked(true, key)
	ht.recordWriteLocked(key)
	return item, true, nil
}

func (ht *HatTrie) GetPriorityQueue(key string) PriorityQueue {
	value, _, _ := ht.GetPriorityQueueChecked(key)
	return value
}

func (ht *HatTrie) GetPriorityQueueChecked(key string) (PriorityQueue, bool, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, false, err
	}
	if !hval.IsPriorityQueue() {
		ht.recordReadLocked(false, key)
		return nil, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.priorityQueues.array[hval.Index].Items(), true, nil
}
