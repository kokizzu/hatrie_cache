package hatriecache

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strconv"
)

const (
	DefaultCountMinSketchWidth uint64 = 2048
	DefaultCountMinSketchDepth uint8  = 4
	maxCountMinSketchCounters  uint64 = 1 << 24
	maxCountMinSketchDepth     uint8  = 64
	maxCountMinSketchCounter   uint32 = ^uint32(0)
)

// CountMinSketchInfo reports the shape and fill state of a Count-Min Sketch.
// The sketch stores approximate frequencies in compact uint32 counters.
type CountMinSketchInfo struct {
	Width             uint64 `json:"width"`
	Depth             uint8  `json:"depth"`
	CounterBytes      uint64 `json:"counter_bytes"`
	TotalCount        uint64 `json:"total_count"`
	MaxCounter        uint32 `json:"max_counter"`
	SaturatedCounters uint64 `json:"saturated_counters"`
}

type countMinSketchSnapshot struct {
	Width      uint64 `json:"width"`
	Depth      uint8  `json:"depth"`
	TotalCount uint64 `json:"total_count"`
	Counters   string `json:"counters"`
}

type countMinSketchData struct {
	counters []uint32
	width    uint64
	depth    uint8
	total    uint64
}

func newCountMinSketchData(width uint64, depth uint8) (countMinSketchData, error) {
	if err := validateCountMinSketchShape(width, depth); err != nil {
		return countMinSketchData{}, err
	}
	return countMinSketchData{
		counters: make([]uint32, int(width*uint64(depth))),
		width:    width,
		depth:    depth,
	}, nil
}

func newDefaultCountMinSketchData() countMinSketchData {
	data, err := newCountMinSketchData(DefaultCountMinSketchWidth, DefaultCountMinSketchDepth)
	if err != nil {
		panic(err)
	}
	return data
}

func validateCountMinSketchShape(width uint64, depth uint8) error {
	if width == 0 {
		return errors.New("hatriecache: count-min sketch width must be positive")
	}
	if depth == 0 {
		return errors.New("hatriecache: count-min sketch depth must be positive")
	}
	if depth > maxCountMinSketchDepth {
		return errors.New("hatriecache: count-min sketch depth is too large")
	}
	if width > maxCountMinSketchCounters/uint64(depth) {
		return errors.New("hatriecache: count-min sketch counter count is too large")
	}
	return nil
}

func validateCountMinSketchSnapshot(snapshot countMinSketchSnapshot) error {
	if err := validateCountMinSketchShape(snapshot.Width, snapshot.Depth); err != nil {
		return err
	}
	data, err := base64.StdEncoding.DecodeString(snapshot.Counters)
	if err != nil {
		return err
	}
	if uint64(len(data)) != snapshot.Width*uint64(snapshot.Depth)*4 {
		return errors.New("hatriecache: invalid count-min sketch counter length")
	}
	return nil
}

func newCountMinSketchDataFromSnapshot(snapshot countMinSketchSnapshot) (countMinSketchData, error) {
	if err := validateCountMinSketchSnapshot(snapshot); err != nil {
		return countMinSketchData{}, err
	}
	raw, err := base64.StdEncoding.DecodeString(snapshot.Counters)
	if err != nil {
		return countMinSketchData{}, err
	}
	counters := make([]uint32, len(raw)/4)
	for idx := range counters {
		counters[idx] = binary.LittleEndian.Uint32(raw[idx*4 : idx*4+4])
	}
	return countMinSketchData{
		counters: counters,
		width:    snapshot.Width,
		depth:    snapshot.Depth,
		total:    snapshot.TotalCount,
	}, nil
}

func (sketch *countMinSketchData) Add(value interface{}, count uint32) uint64 {
	if sketch == nil || sketch.width == 0 || sketch.depth == 0 {
		return 0
	}
	if count == 0 {
		return sketch.Estimate(value)
	}
	key := mustCountMinSketchItemKey(value)
	estimate := uint64(maxCountMinSketchCounter)
	sketch.visitIndexes(key, func(index uint64) {
		next := saturatingAddUint32(sketch.counters[index], count)
		sketch.counters[index] = next
		if uint64(next) < estimate {
			estimate = uint64(next)
		}
	})
	sketch.total = saturatingAddUint64(sketch.total, uint64(count))
	return estimate
}

func (sketch *countMinSketchData) AddOne(value interface{}, count uint32, values ...interface{}) uint64 {
	estimate := sketch.Add(value, count)
	for _, value := range values {
		estimate = sketch.Add(value, count)
	}
	return estimate
}

func (sketch *countMinSketchData) Estimate(value interface{}) uint64 {
	if sketch == nil || sketch.width == 0 || sketch.depth == 0 {
		return 0
	}
	key := mustCountMinSketchItemKey(value)
	estimate := maxCountMinSketchCounter
	sketch.visitIndexes(key, func(index uint64) {
		if sketch.counters[index] < estimate {
			estimate = sketch.counters[index]
		}
	})
	return uint64(estimate)
}

func (sketch countMinSketchData) Info() CountMinSketchInfo {
	var maxCounter uint32
	var saturated uint64
	for _, counter := range sketch.counters {
		if counter > maxCounter {
			maxCounter = counter
		}
		if counter == maxCountMinSketchCounter {
			saturated++
		}
	}
	return CountMinSketchInfo{
		Width:             sketch.width,
		Depth:             sketch.depth,
		CounterBytes:      uint64(len(sketch.counters)) * 4,
		TotalCount:        sketch.total,
		MaxCounter:        maxCounter,
		SaturatedCounters: saturated,
	}
}

func (sketch countMinSketchData) Snapshot() countMinSketchSnapshot {
	data := make([]byte, len(sketch.counters)*4)
	for idx, counter := range sketch.counters {
		binary.LittleEndian.PutUint32(data[idx*4:idx*4+4], counter)
	}
	return countMinSketchSnapshot{
		Width:      sketch.width,
		Depth:      sketch.depth,
		TotalCount: sketch.total,
		Counters:   base64.StdEncoding.EncodeToString(data),
	}
}

func (sketch countMinSketchData) EncodedSize() int64 {
	return int64(len(sketch.counters) * 4)
}

func (sketch *countMinSketchData) visitIndexes(key []byte, visit func(uint64)) {
	first := bloomFilterFNV64a(key)
	step := bloomFilterFNV64(key)
	if step == 0 {
		step = bloomFilterFNVPrime64
	}
	step |= 1
	for row := uint8(0); row < sketch.depth; row++ {
		column := (first + uint64(row)*step) % sketch.width
		visit(uint64(row)*sketch.width + column)
	}
}

func saturatingAddUint32(value uint32, delta uint32) uint32 {
	if maxCountMinSketchCounter-value < delta {
		return maxCountMinSketchCounter
	}
	return value + delta
}

func saturatingAddUint64(value uint64, delta uint64) uint64 {
	if ^uint64(0)-value < delta {
		return ^uint64(0)
	}
	return value + delta
}

func mustCountMinSketchItemKey(value interface{}) []byte {
	key, err := countMinSketchItemKey(value)
	if err != nil {
		panic(err)
	}
	return key
}

func countMinSketchItemKey(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

// CountMinSketchStorage stores Count-Min Sketch values outside the trie.
type CountMinSketchStorage struct {
	array     []countMinSketchData
	reusables reusableIndexes
}

func CreateCountMinSketchStorage() *CountMinSketchStorage {
	return &CountMinSketchStorage{
		array: []countMinSketchData{},
	}
}

func (store *CountMinSketchStorage) PutData(idx int32, value countMinSketchData) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = value
	store.reusables.Use(idx)
}

func (store *CountMinSketchStorage) AppendData(value countMinSketchData) int32 {
	store.array = append(store.array, value)
	return int32(len(store.array) - 1)
}

func (store *CountMinSketchStorage) AddData(value countMinSketchData) int32 {
	if idx, ok := store.reusables.Take(); ok {
		store.array[idx] = value
		return idx
	}
	return store.AppendData(value)
}

func (store *CountMinSketchStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = countMinSketchData{}
	store.reusables.Mark(idx)
	store.array = trimReusableTail(store.array, &store.reusables)
}

func (ht *HatTrie) UpsertCountMinSketch(key string, width uint64, depth uint8) error {
	data, err := newCountMinSketchData(width, depth)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsCountMinSketch() {
		ht.countMinSketches.PutData(hval.Index, data)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.countMinSketches.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_COUNT_MIN_SKETCH}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) IncrementCountMinSketch(key string, val interface{}, count uint32) uint64 {
	if count == 0 {
		estimate, _ := ht.EstimateCountMinSketch(key, val)
		return estimate
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsCountMinSketch() {
		estimate := ht.countMinSketches.array[hval.Index].Add(val, count)
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return estimate
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.countMinSketches.AddData(newDefaultCountMinSketchData())
	estimate := ht.countMinSketches.array[idx].Add(val, count)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_COUNT_MIN_SKETCH}.toValue()
	ht.recordWriteLocked(key)
	return estimate
}

func (ht *HatTrie) EstimateCountMinSketch(key string, val interface{}) (uint64, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsCountMinSketch() {
		ht.recordReadLocked(false, key)
		return 0, false
	}
	estimate := ht.countMinSketches.array[hval.Index].Estimate(val)
	ht.recordReadLocked(true, key)
	return estimate, true
}

func (ht *HatTrie) CountMinSketchInfo(key string) (CountMinSketchInfo, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsCountMinSketch() {
		ht.recordReadLocked(false, key)
		return CountMinSketchInfo{}, false
	}
	ht.recordReadLocked(true, key)
	return ht.countMinSketches.array[hval.Index].Info(), true
}

func countMinSketchDepthValue(value uint64) (uint8, error) {
	if value == 0 || value > uint64(maxCountMinSketchDepth) || value > uint64(^uint8(0)) {
		return 0, errors.New("hatriecache: count-min sketch depth must be between 1 and " + strconv.Itoa(int(maxCountMinSketchDepth)))
	}
	return uint8(value), nil
}
