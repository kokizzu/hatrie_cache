package hatriecache

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/fnv"
	"sort"
	"strconv"
)

const (
	DefaultReservoirSampleCapacity uint64 = 128
	maxReservoirSampleCapacity     uint64 = 1 << 20
)

// ReservoirSampleItem is one retained sample from a stream. Lower priority
// values are retained first, and Sequence is the 1-based stream position.
type ReservoirSampleItem struct {
	Value    interface{} `json:"value"`
	Priority uint64      `json:"priority"`
	Sequence uint64      `json:"sequence"`
}

// ReservoirSampleUpdate reports the result of adding one or more stream values.
type ReservoirSampleUpdate struct {
	Accepted bool   `json:"accepted"`
	Seen     uint64 `json:"seen"`
	Tracked  uint64 `json:"tracked"`
	Capacity uint64 `json:"capacity"`
}

// ReservoirSampleInfo reports the shape and encoded size of a fixed-memory
// stream sample.
type ReservoirSampleInfo struct {
	Capacity     uint64 `json:"capacity"`
	Tracked      uint64 `json:"tracked"`
	Seen         uint64 `json:"seen"`
	MinPriority  uint64 `json:"min_priority"`
	MaxPriority  uint64 `json:"max_priority"`
	EncodedBytes int64  `json:"encoded_bytes"`
}

type reservoirSampleSnapshot struct {
	Capacity uint64                `json:"capacity"`
	Seen     uint64                `json:"seen"`
	Items    []reservoirSampleItem `json:"items"`
}

type reservoirSampleItem struct {
	Value    interface{} `json:"value"`
	Priority uint64      `json:"priority"`
	Sequence uint64      `json:"sequence"`
}

type reservoirSampleData struct {
	capacity uint64
	seen     uint64
	items    []reservoirSampleItem
}

func newReservoirSampleData(capacity uint64) (reservoirSampleData, error) {
	if err := validateReservoirSampleCapacity(capacity); err != nil {
		return reservoirSampleData{}, err
	}
	return reservoirSampleData{
		capacity: capacity,
		items:    make([]reservoirSampleItem, 0, int(capacity)),
	}, nil
}

func newDefaultReservoirSampleData() reservoirSampleData {
	data, err := newReservoirSampleData(DefaultReservoirSampleCapacity)
	if err != nil {
		panic(err)
	}
	return data
}

func validateReservoirSampleCapacity(capacity uint64) error {
	if capacity == 0 {
		return errors.New("hatriecache: reservoir sample capacity must be positive")
	}
	if capacity > maxReservoirSampleCapacity {
		return errors.New("hatriecache: reservoir sample capacity is too large")
	}
	return nil
}

func validateReservoirSampleSnapshot(snapshot reservoirSampleSnapshot) error {
	if err := validateReservoirSampleCapacity(snapshot.Capacity); err != nil {
		return err
	}
	if uint64(len(snapshot.Items)) > snapshot.Capacity {
		return errors.New("hatriecache: reservoir sample snapshot has too many items")
	}
	if uint64(len(snapshot.Items)) > snapshot.Seen {
		return errors.New("hatriecache: reservoir sample snapshot tracks more items than it has seen")
	}
	if snapshot.Seen < snapshot.Capacity && uint64(len(snapshot.Items)) != snapshot.Seen {
		return errors.New("hatriecache: reservoir sample snapshot must retain every early stream item")
	}
	if snapshot.Seen >= snapshot.Capacity && uint64(len(snapshot.Items)) != snapshot.Capacity {
		return errors.New("hatriecache: reservoir sample snapshot must be full after capacity is reached")
	}
	seenSequences := make(map[uint64]struct{}, len(snapshot.Items))
	for _, item := range snapshot.Items {
		if item.Sequence == 0 || item.Sequence > snapshot.Seen {
			return errors.New("hatriecache: reservoir sample snapshot sequence is out of range")
		}
		if _, ok := seenSequences[item.Sequence]; ok {
			return errors.New("hatriecache: duplicate reservoir sample sequence")
		}
		if item.Priority != reservoirSamplePriority(item.Sequence, item.Value) {
			return errors.New("hatriecache: reservoir sample priority does not match value")
		}
		seenSequences[item.Sequence] = struct{}{}
	}
	return nil
}

func newReservoirSampleDataFromSnapshot(snapshot reservoirSampleSnapshot) (reservoirSampleData, error) {
	if err := validateReservoirSampleSnapshot(snapshot); err != nil {
		return reservoirSampleData{}, err
	}
	data := reservoirSampleData{
		capacity: snapshot.Capacity,
		seen:     snapshot.Seen,
		items:    make([]reservoirSampleItem, len(snapshot.Items)),
	}
	for idx, item := range snapshot.Items {
		data.items[idx] = reservoirSampleItem{
			Value:    cloneValue(item.Value),
			Priority: item.Priority,
			Sequence: item.Sequence,
		}
	}
	for i := len(data.items)/2 - 1; i >= 0; i-- {
		data.siftDown(i)
	}
	return data, nil
}

func (sample *reservoirSampleData) Add(value interface{}) ReservoirSampleUpdate {
	if sample == nil || sample.capacity == 0 {
		return ReservoirSampleUpdate{}
	}
	sample.seen = saturatingAddUint64(sample.seen, 1)
	item := reservoirSampleItem{
		Value:    cloneValue(value),
		Priority: reservoirSamplePriority(sample.seen, value),
		Sequence: sample.seen,
	}
	accepted := false
	if uint64(len(sample.items)) < sample.capacity {
		sample.items = append(sample.items, item)
		sample.siftUp(len(sample.items) - 1)
		accepted = true
	} else if reservoirSampleBetter(item, sample.items[0]) {
		sample.items[0] = item
		sample.siftDown(0)
		accepted = true
	}
	return ReservoirSampleUpdate{
		Accepted: accepted,
		Seen:     sample.seen,
		Tracked:  uint64(len(sample.items)),
		Capacity: sample.capacity,
	}
}

func (sample *reservoirSampleData) AddOne(value interface{}, values ...interface{}) ReservoirSampleUpdate {
	update := sample.Add(value)
	for _, value := range values {
		update = sample.Add(value)
	}
	return update
}

func (sample reservoirSampleData) Items() []ReservoirSampleItem {
	items := sample.sortedItems()
	out := make([]ReservoirSampleItem, len(items))
	for idx, item := range items {
		out[idx] = ReservoirSampleItem{
			Value:    cloneValue(item.Value),
			Priority: item.Priority,
			Sequence: item.Sequence,
		}
	}
	return out
}

func (sample reservoirSampleData) Snapshot() reservoirSampleSnapshot {
	items := sample.sortedItems()
	out := make([]reservoirSampleItem, len(items))
	for idx, item := range items {
		out[idx] = reservoirSampleItem{
			Value:    cloneValue(item.Value),
			Priority: item.Priority,
			Sequence: item.Sequence,
		}
	}
	return reservoirSampleSnapshot{
		Capacity: sample.capacity,
		Seen:     sample.seen,
		Items:    out,
	}
}

func (sample reservoirSampleData) Info() ReservoirSampleInfo {
	info := ReservoirSampleInfo{
		Capacity:     sample.capacity,
		Tracked:      uint64(len(sample.items)),
		Seen:         sample.seen,
		EncodedBytes: sample.EncodedSize(),
	}
	for idx, item := range sample.items {
		if idx == 0 || item.Priority < info.MinPriority {
			info.MinPriority = item.Priority
		}
		if item.Priority > info.MaxPriority {
			info.MaxPriority = item.Priority
		}
	}
	return info
}

func (sample reservoirSampleData) EncodedSize() int64 {
	data, err := json.Marshal(sample.Snapshot())
	if err != nil {
		return 0
	}
	return int64(len(data))
}

func (sample reservoirSampleData) sortedItems() []reservoirSampleItem {
	if len(sample.items) == 0 {
		return []reservoirSampleItem{}
	}
	out := make([]reservoirSampleItem, len(sample.items))
	copy(out, sample.items)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority < out[j].Priority
		}
		return out[i].Sequence < out[j].Sequence
	})
	return out
}

func (sample *reservoirSampleData) siftUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if !reservoirSampleWorse(sample.items[idx], sample.items[parent]) {
			return
		}
		sample.items[idx], sample.items[parent] = sample.items[parent], sample.items[idx]
		idx = parent
	}
}

func (sample *reservoirSampleData) siftDown(idx int) {
	for {
		left := 2*idx + 1
		if left >= len(sample.items) {
			return
		}
		worst := left
		right := left + 1
		if right < len(sample.items) && reservoirSampleWorse(sample.items[right], sample.items[left]) {
			worst = right
		}
		if !reservoirSampleWorse(sample.items[worst], sample.items[idx]) {
			return
		}
		sample.items[idx], sample.items[worst] = sample.items[worst], sample.items[idx]
		idx = worst
	}
}

func reservoirSampleBetter(a reservoirSampleItem, b reservoirSampleItem) bool {
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}
	return a.Sequence < b.Sequence
}

func reservoirSampleWorse(a reservoirSampleItem, b reservoirSampleItem) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	return a.Sequence > b.Sequence
}

func reservoirSamplePriority(sequence uint64, value interface{}) uint64 {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	hash := fnv.New64a()
	var sequenceBytes [8]byte
	binary.LittleEndian.PutUint64(sequenceBytes[:], sequence)
	_, _ = hash.Write(sequenceBytes[:])
	_, _ = hash.Write(data)
	return hash.Sum64()
}

// ReservoirSampleStorage stores fixed-memory stream samples outside the trie.
type ReservoirSampleStorage struct {
	array     []reservoirSampleData
	reusables reusableIndexes
}

func CreateReservoirSampleStorage() *ReservoirSampleStorage {
	return &ReservoirSampleStorage{
		array: []reservoirSampleData{},
	}
}

func (store *ReservoirSampleStorage) PutData(idx int32, value reservoirSampleData) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = value
	store.reusables.Use(idx)
}

func (store *ReservoirSampleStorage) AppendData(value reservoirSampleData) int32 {
	store.array = append(store.array, value)
	return int32(len(store.array) - 1)
}

func (store *ReservoirSampleStorage) AddData(value reservoirSampleData) int32 {
	if idx, ok := store.reusables.Take(); ok {
		store.array[idx] = value
		return idx
	}
	return store.AppendData(value)
}

func (store *ReservoirSampleStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = reservoirSampleData{}
	store.reusables.Mark(idx)
	store.array = trimReusableTail(store.array, &store.reusables)
}

func (ht *HatTrie) UpsertReservoirSample(key string, capacity uint64) error {
	data, err := newReservoirSampleData(capacity)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsReservoirSample() {
		ht.reservoirSamples.PutData(hval.Index, data)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.reservoirSamples.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RESERVOIR_SAMPLE}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) AddReservoirSample(key string, val interface{}, vals ...interface{}) ReservoirSampleUpdate {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsReservoirSample() {
		update := ht.reservoirSamples.array[hval.Index].AddOne(val, vals...)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return update
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.reservoirSamples.AddData(newDefaultReservoirSampleData())
	update := ht.reservoirSamples.array[idx].AddOne(val, vals...)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RESERVOIR_SAMPLE}.toValue()
	ht.recordWriteLocked(key)
	return update
}

func (ht *HatTrie) GetReservoirSample(key string) []ReservoirSampleItem {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsReservoirSample() {
		ht.recordReadLocked(false, key)
		return nil
	}
	ht.recordReadLocked(true, key)
	return ht.reservoirSamples.array[hval.Index].Items()
}

func (ht *HatTrie) ReservoirSampleInfo(key string) (ReservoirSampleInfo, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsReservoirSample() {
		ht.recordReadLocked(false, key)
		return ReservoirSampleInfo{}, false
	}
	ht.recordReadLocked(true, key)
	return ht.reservoirSamples.array[hval.Index].Info(), true
}

func reservoirSampleCapacityValue(value uint64) (uint64, error) {
	if value == 0 || value > maxReservoirSampleCapacity {
		return 0, errors.New("hatriecache: reservoir sample capacity must be between 1 and " + strconv.FormatUint(maxReservoirSampleCapacity, 10))
	}
	return value, nil
}
