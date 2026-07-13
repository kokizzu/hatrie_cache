package hatriecache

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

func newPriorityQueueData(values PriorityQueue) priorityQueueData {
	out := priorityQueueData{}
	for _, value := range values {
		out.Push(value.Priority, value.Value)
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

func (pq *priorityQueueData) Push(priority int64, values ...interface{}) int {
	if len(values) == 0 {
		return 0
	}
	for _, value := range values {
		pq.pushValue(priority, value)
	}
	return len(values)
}

func (pq *priorityQueueData) PushOne(priority int64, value interface{}, values ...interface{}) int {
	pq.pushValue(priority, value)
	for _, value := range values {
		pq.pushValue(priority, value)
	}
	return 1 + len(values)
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

	rawPtr, hval := ht.upsertFreshLocation(key)
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
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsPriorityQueue() {
		added := ht.priorityQueues.array[hval.Index].PushOne(priority, val, vals...)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return added
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.priorityQueues.Add(nil)
	added := ht.priorityQueues.array[idx].PushOne(priority, val, vals...)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_PRIORITY_QUEUE}.toValue()
	ht.recordWriteLocked(key)
	return added
}

func (ht *HatTrie) PeekPriorityQueue(key string) (PriorityItem, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsPriorityQueue() {
		ht.recordReadLocked(false, key)
		return PriorityItem{}, false
	}
	item, ok := ht.priorityQueues.array[hval.Index].Peek()
	ht.recordReadLocked(ok, key)
	return item, ok
}

func (ht *HatTrie) PopPriorityQueue(key string) (PriorityItem, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsPriorityQueue() {
		ht.recordReadLocked(false, key)
		return PriorityItem{}, false
	}
	item, ok := ht.priorityQueues.array[hval.Index].Pop()
	if !ok {
		ht.recordReadLocked(false, key)
		return PriorityItem{}, false
	}
	ht.recordReadLocked(true, key)
	ht.recordWriteLocked(key)
	return item, true
}

func (ht *HatTrie) GetPriorityQueue(key string) PriorityQueue {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsPriorityQueue() {
		ht.recordReadLocked(false, key)
		return nil
	}
	ht.recordReadLocked(true, key)
	return ht.priorityQueues.array[hval.Index].Items()
}
