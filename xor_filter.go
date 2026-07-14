package hatriecache

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
)

const (
	DefaultXorFilterExpectedItems uint64  = 10000
	maxXorFilterItems             uint64  = 1 << 22
	xorFilterLoadFactor           float64 = 1.23
	xorFilterFingerprintBits      uint8   = 8
	xorFilterMaxBuildAttempts     int     = 128
	xorFilterSeedBase             uint64  = 0x9e3779b97f4a7c15
)

// XorFilterInfo reports the shape and compactness of a static XOR filter.
// XOR filters are immutable after build and store only 8-bit fingerprints.
type XorFilterInfo struct {
	ExpectedItems              uint64  `json:"expected_items"`
	Items                      uint64  `json:"items"`
	Staged                     uint64  `json:"staged"`
	Built                      bool    `json:"built"`
	FingerprintBits            uint8   `json:"fingerprint_bits"`
	FingerprintCount           uint64  `json:"fingerprint_count"`
	FingerprintBytes           uint64  `json:"fingerprint_bytes"`
	LoadFactor                 float64 `json:"load_factor"`
	EstimatedFalsePositiveRate float64 `json:"estimated_false_positive_rate"`
}

type xorFilterSnapshot struct {
	ExpectedItems uint64                `json:"expected_items"`
	Built         bool                  `json:"built"`
	Items         uint64                `json:"items"`
	Seed          uint64                `json:"seed,omitempty"`
	BlockLength   uint32                `json:"block_length,omitempty"`
	Fingerprints  string                `json:"fingerprints,omitempty"`
	Staged        []xorFilterStagedItem `json:"staged,omitempty"`
}

type xorFilterStagedItem struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

type xorFilterData struct {
	expectedItems uint64
	built         bool
	items         uint64
	seed          uint64
	blockLength   uint32
	fingerprints  []uint8
	staged        map[string]interface{}
}

type xorFilterBuildSlot struct {
	xor   uint64
	count uint32
}

type xorFilterPeel struct {
	hash  uint64
	index uint32
}

type xorFilterPendingItem struct {
	key   string
	value interface{}
}

func newXorFilterData(expectedItems uint64) (xorFilterData, error) {
	if err := validateXorFilterExpectedItems(expectedItems); err != nil {
		return xorFilterData{}, err
	}
	return xorFilterData{
		expectedItems: expectedItems,
		staged:        make(map[string]interface{}, int(expectedItems)),
	}, nil
}

func newDefaultXorFilterData() xorFilterData {
	data, err := newXorFilterData(DefaultXorFilterExpectedItems)
	if err != nil {
		panic(err)
	}
	return data
}

func validateXorFilterExpectedItems(expectedItems uint64) error {
	if expectedItems == 0 {
		return errors.New("hatriecache: xor filter expected items must be positive")
	}
	if expectedItems > maxXorFilterItems {
		return errors.New("hatriecache: xor filter expected items must be <= " + strconv.FormatUint(maxXorFilterItems, 10))
	}
	return nil
}

func validateXorFilterSnapshot(snapshot xorFilterSnapshot) error {
	if err := validateXorFilterExpectedItems(snapshot.ExpectedItems); err != nil {
		return err
	}
	if snapshot.Items > maxXorFilterItems {
		return errors.New("hatriecache: xor filter snapshot item count is too large")
	}
	if snapshot.Built {
		if len(snapshot.Staged) != 0 {
			return errors.New("hatriecache: built xor filter snapshot cannot contain staged values")
		}
		raw, err := base64.StdEncoding.DecodeString(snapshot.Fingerprints)
		if err != nil {
			return err
		}
		if snapshot.BlockLength == 0 {
			if len(raw) != 0 || snapshot.Items != 0 {
				return errors.New("hatriecache: invalid empty xor filter snapshot")
			}
			return nil
		}
		if len(raw) != int(snapshot.BlockLength)*3 {
			return errors.New("hatriecache: invalid xor filter fingerprint length")
		}
		return nil
	}
	if snapshot.BlockLength != 0 || snapshot.Seed != 0 || snapshot.Fingerprints != "" {
		return errors.New("hatriecache: pending xor filter snapshot cannot contain built fingerprints")
	}
	if uint64(len(snapshot.Staged)) > maxXorFilterItems || snapshot.Items != uint64(len(snapshot.Staged)) {
		return errors.New("hatriecache: invalid xor filter staged item count")
	}
	seen := make(map[string]struct{}, len(snapshot.Staged))
	for _, item := range snapshot.Staged {
		if item.Key == "" {
			return errors.New("hatriecache: xor filter staged item key is required")
		}
		derived, err := xorFilterItemKey(item.Value)
		if err != nil {
			return err
		}
		if derived != item.Key {
			return errors.New("hatriecache: xor filter staged item key does not match value")
		}
		if _, ok := seen[item.Key]; ok {
			return errors.New("hatriecache: duplicate xor filter staged item")
		}
		seen[item.Key] = struct{}{}
	}
	return nil
}

func newXorFilterDataFromSnapshot(snapshot xorFilterSnapshot) (xorFilterData, error) {
	if err := validateXorFilterSnapshot(snapshot); err != nil {
		return xorFilterData{}, err
	}
	data := xorFilterData{
		expectedItems: snapshot.ExpectedItems,
		built:         snapshot.Built,
		items:         snapshot.Items,
		seed:          snapshot.Seed,
		blockLength:   snapshot.BlockLength,
	}
	if snapshot.Built {
		raw, err := base64.StdEncoding.DecodeString(snapshot.Fingerprints)
		if err != nil {
			return xorFilterData{}, err
		}
		data.fingerprints = make([]uint8, len(raw))
		copy(data.fingerprints, raw)
		return data, nil
	}
	data.staged = make(map[string]interface{}, len(snapshot.Staged))
	for _, item := range snapshot.Staged {
		data.staged[item.Key] = cloneValue(item.Value)
	}
	return data, nil
}

func (filter *xorFilterData) Add(value interface{}) (bool, error) {
	added, err := filter.AddOne(value)
	return added == 1, err
}

func (filter *xorFilterData) AddOne(value interface{}, values ...interface{}) (int, error) {
	if filter == nil {
		return 0, nil
	}
	if filter.built {
		return 0, errors.New("hatriecache: xor filter is already built")
	}
	if filter.staged == nil {
		filter.staged = make(map[string]interface{})
	}
	pending := make([]xorFilterPendingItem, 0, 1+len(values))
	seen := make(map[string]struct{}, 1+len(values))
	key, err := xorFilterItemKey(value)
	if err != nil {
		return 0, err
	}
	if _, ok := filter.staged[key]; !ok {
		pending = append(pending, xorFilterPendingItem{key: key, value: value})
		seen[key] = struct{}{}
	}
	for _, value := range values {
		key, err := xorFilterItemKey(value)
		if err != nil {
			return 0, err
		}
		if _, ok := filter.staged[key]; ok {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		pending = append(pending, xorFilterPendingItem{key: key, value: value})
		seen[key] = struct{}{}
	}
	if uint64(len(filter.staged)+len(pending)) > maxXorFilterItems {
		return 0, errors.New("hatriecache: xor filter staged item count is too large")
	}
	for _, item := range pending {
		filter.staged[item.key] = cloneValue(item.value)
	}
	filter.items = uint64(len(filter.staged))
	return len(pending), nil
}

func (filter *xorFilterData) Build() error {
	if filter == nil {
		return nil
	}
	if filter.built {
		return nil
	}
	if uint64(len(filter.staged)) > maxXorFilterItems {
		return errors.New("hatriecache: xor filter staged item count is too large")
	}
	keys := make([]string, 0, len(filter.staged))
	for key := range filter.staged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		filter.built = true
		filter.items = 0
		filter.seed = 0
		filter.blockLength = 0
		filter.fingerprints = nil
		filter.staged = nil
		return nil
	}
	for attempt := 0; attempt < xorFilterMaxBuildAttempts; attempt++ {
		seed := xorFilterSeed(len(keys), attempt)
		fingerprints, blockLength, ok := buildXorFilterFingerprints(keys, seed)
		if !ok {
			continue
		}
		filter.built = true
		filter.items = uint64(len(keys))
		filter.seed = seed
		filter.blockLength = blockLength
		filter.fingerprints = fingerprints
		filter.staged = nil
		return nil
	}
	return errors.New("hatriecache: xor filter build failed")
}

func (filter xorFilterData) Contains(value interface{}) (bool, bool) {
	hit, queryable, _ := filter.ContainsChecked(value)
	return hit, queryable
}

func (filter xorFilterData) ContainsChecked(value interface{}) (bool, bool, error) {
	key, err := xorFilterItemKey(value)
	if err != nil {
		return false, false, err
	}
	hit, queryable := filter.containsKey(key)
	return hit, queryable, nil
}

func (filter xorFilterData) containsKey(key string) (bool, bool) {
	if !filter.built {
		return false, false
	}
	if filter.blockLength == 0 || len(filter.fingerprints) == 0 {
		return false, true
	}
	if len(filter.fingerprints) != int(filter.blockLength)*3 {
		return false, false
	}
	hash := xorFilterHashString(key, filter.seed)
	fingerprint := xorFilterFingerprint(hash)
	for _, index := range xorFilterIndexes(hash, filter.blockLength) {
		fingerprint ^= filter.fingerprints[index]
	}
	return fingerprint == 0, true
}

func (filter xorFilterData) Snapshot() xorFilterSnapshot {
	snapshot := xorFilterSnapshot{
		ExpectedItems: filter.expectedItems,
		Built:         filter.built,
		Items:         filter.itemCount(),
		Seed:          filter.seed,
		BlockLength:   filter.blockLength,
	}
	if filter.built {
		snapshot.Fingerprints = base64.StdEncoding.EncodeToString(filter.fingerprints)
		return snapshot
	}
	keys := make([]string, 0, len(filter.staged))
	for key := range filter.staged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	snapshot.Staged = make([]xorFilterStagedItem, 0, len(keys))
	for _, key := range keys {
		snapshot.Staged = append(snapshot.Staged, xorFilterStagedItem{
			Key:   key,
			Value: cloneValue(filter.staged[key]),
		})
	}
	return snapshot
}

func (filter xorFilterData) Info() XorFilterInfo {
	fingerprintCount := uint64(len(filter.fingerprints))
	items := filter.itemCount()
	loadFactor := 0.0
	if fingerprintCount > 0 {
		loadFactor = float64(items) / float64(fingerprintCount)
	}
	staged := uint64(0)
	if !filter.built {
		staged = uint64(len(filter.staged))
	}
	fpr := 0.0
	if filter.built && fingerprintCount > 0 {
		fpr = 1.0 / 256.0
	}
	return XorFilterInfo{
		ExpectedItems:              filter.expectedItems,
		Items:                      items,
		Staged:                     staged,
		Built:                      filter.built,
		FingerprintBits:            xorFilterFingerprintBits,
		FingerprintCount:           fingerprintCount,
		FingerprintBytes:           fingerprintCount,
		LoadFactor:                 loadFactor,
		EstimatedFalsePositiveRate: fpr,
	}
}

func (filter xorFilterData) EncodedSize() int64 {
	if filter.built {
		return int64(len(filter.fingerprints))
	}
	var size int64
	for key := range filter.staged {
		size += int64(len(key))
	}
	return size
}

func (filter xorFilterData) itemCount() uint64 {
	if filter.built {
		return filter.items
	}
	return uint64(len(filter.staged))
}

func buildXorFilterFingerprints(keys []string, seed uint64) ([]uint8, uint32, bool) {
	blockLength := xorFilterBlockLength(uint64(len(keys)))
	if blockLength == 0 {
		return nil, 0, true
	}
	size := int(blockLength) * 3
	slots := make([]xorFilterBuildSlot, size)
	for _, key := range keys {
		hash := xorFilterHashString(key, seed)
		for _, index := range xorFilterIndexes(hash, blockLength) {
			slots[index].count++
			slots[index].xor ^= hash
		}
	}

	queue := make([]uint32, 0, size)
	for index, slot := range slots {
		if slot.count == 1 {
			queue = append(queue, uint32(index))
		}
	}

	order := make([]xorFilterPeel, 0, len(keys))
	for head := 0; head < len(queue); head++ {
		index := queue[head]
		slot := slots[index]
		if slot.count != 1 {
			continue
		}
		hash := slot.xor
		order = append(order, xorFilterPeel{hash: hash, index: index})
		for _, other := range xorFilterIndexes(hash, blockLength) {
			if slots[other].count == 0 {
				continue
			}
			slots[other].count--
			slots[other].xor ^= hash
			if slots[other].count == 1 {
				queue = append(queue, other)
			}
		}
	}
	if len(order) != len(keys) {
		return nil, blockLength, false
	}

	fingerprints := make([]uint8, size)
	for pos := len(order) - 1; pos >= 0; pos-- {
		item := order[pos]
		fingerprint := xorFilterFingerprint(item.hash)
		for _, index := range xorFilterIndexes(item.hash, blockLength) {
			if index == item.index {
				continue
			}
			fingerprint ^= fingerprints[index]
		}
		fingerprints[item.index] = fingerprint
	}
	return fingerprints, blockLength, true
}

func xorFilterSeed(items int, attempt int) uint64 {
	return splitmix64(xorFilterSeedBase + uint64(items)*0xbf58476d1ce4e5b9 + uint64(attempt)*0x94d049bb133111eb)
}

func xorFilterBlockLength(items uint64) uint32 {
	if items == 0 {
		return 0
	}
	slots := uint64(math.Ceil(float64(items)*xorFilterLoadFactor)) + 32
	blockLength := slots/3 + 1
	if blockLength < 2 {
		blockLength = 2
	}
	return uint32(blockLength)
}

func xorFilterHashString(key string, seed uint64) uint64 {
	hash := bloomFilterFNV64a([]byte(key))
	hash = splitmix64(hash ^ seed)
	if hash == 0 {
		return xorFilterSeedBase
	}
	return hash
}

func xorFilterIndexes(hash uint64, blockLength uint32) [3]uint32 {
	second := splitmix64(hash)
	third := splitmix64(second)
	return [3]uint32{
		uint32(hash % uint64(blockLength)),
		blockLength + uint32(second%uint64(blockLength)),
		2*blockLength + uint32(third%uint64(blockLength)),
	}
}

func xorFilterFingerprint(hash uint64) uint8 {
	fingerprint := uint8(hash)
	if fingerprint == 0 {
		return 1
	}
	return fingerprint
}

func xorFilterItemKey(value interface{}) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("hatriecache: unsupported xor filter value: %w", err)
	}
	return string(data), nil
}

// XorFilterStorage stores XOR filter values outside the trie.
type XorFilterStorage struct {
	array     []xorFilterData
	reusables reusableIndexes
}

func CreateXorFilterStorage() *XorFilterStorage {
	return &XorFilterStorage{
		array: []xorFilterData{},
	}
}

func (store *XorFilterStorage) PutData(idx int32, value xorFilterData) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = value
	store.reusables.Use(idx)
}

func (store *XorFilterStorage) AppendData(value xorFilterData) int32 {
	store.array = append(store.array, value)
	return int32(len(store.array) - 1)
}

func (store *XorFilterStorage) AddData(value xorFilterData) int32 {
	if idx, ok := store.reusables.Take(); ok {
		store.array[idx] = value
		return idx
	}
	return store.AppendData(value)
}

func (store *XorFilterStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = xorFilterData{}
	store.reusables.Mark(idx)
	store.array = trimReusableTail(store.array, &store.reusables)
}

func (ht *HatTrie) UpsertXorFilter(key string, expectedItems uint64) error {
	data, err := newXorFilterData(expectedItems)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsXorFilter() {
		ht.xorFilters.PutData(hval.Index, data)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.xorFilters.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_XOR_FILTER}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) AddXorFilter(key string, val interface{}, vals ...interface{}) (int, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return 0, err
	}
	if hval.IsXorFilter() {
		added, err := ht.xorFilters.array[hval.Index].AddOne(val, vals...)
		if err != nil {
			return added, err
		}
		*rawPtr = hval.toValue()
		if added > 0 {
			ht.recordWriteLocked(key)
		}
		return added, nil
	}

	data := newDefaultXorFilterData()
	added, err := data.AddOne(val, vals...)
	if err != nil {
		return 0, err
	}
	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.xorFilters.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_XOR_FILTER}.toValue()
	ht.recordWriteLocked(key)
	return added, nil
}

func (ht *HatTrie) BuildXorFilter(key string) (XorFilterInfo, bool, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return XorFilterInfo{}, false, err
	}
	if !hval.IsXorFilter() {
		ht.recordReadLocked(false, key)
		return XorFilterInfo{}, false, nil
	}
	if err := ht.xorFilters.array[hval.Index].Build(); err != nil {
		return XorFilterInfo{}, true, err
	}
	ht.recordReadLocked(true, key)
	ht.recordWriteLocked(key)
	return ht.xorFilters.array[hval.Index].Info(), true, nil
}

func (ht *HatTrie) HasXorFilter(key string, val interface{}) (bool, bool) {
	hit, queryable, _ := ht.HasXorFilterChecked(key, val)
	return hit, queryable
}

func (ht *HatTrie) HasXorFilterChecked(key string, val interface{}) (bool, bool, error) {
	valueKey, err := xorFilterItemKey(val)
	if err != nil {
		return false, false, err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return false, false, err
	}
	if !hval.IsXorFilter() {
		ht.recordReadLocked(false, key)
		return false, false, nil
	}
	hit, queryable := ht.xorFilters.array[hval.Index].containsKey(valueKey)
	ht.recordReadLocked(queryable && hit, key)
	return hit, queryable, nil
}

func (ht *HatTrie) XorFilterInfo(key string) (XorFilterInfo, bool) {
	info, ok, _ := ht.XorFilterInfoChecked(key)
	return info, ok
}

func (ht *HatTrie) XorFilterInfoChecked(key string) (XorFilterInfo, bool, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return XorFilterInfo{}, false, err
	}
	if !hval.IsXorFilter() {
		ht.recordReadLocked(false, key)
		return XorFilterInfo{}, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.xorFilters.array[hval.Index].Info(), true, nil
}
