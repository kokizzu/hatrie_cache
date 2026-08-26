package hatCache

import (
	"fmt"

	json "github.com/goccy/go-json"
	"hatrie_cache/hat/hatDataStructure"
	"hatrie_cache/hat/hatHash"
)

const (
	DefaultBloomFilterExpectedItems     uint64  = 10000
	DefaultBloomFilterFalsePositiveRate float64 = 0.01
	minBloomFilterBits                  uint64  = hatDataStructure.MinBloomFilterBits
	maxBloomFilterBits                  uint64  = hatDataStructure.MaxBloomFilterBits
	maxBloomFilterHashes                uint8   = hatDataStructure.MaxBloomFilterHashes
	bloomFilterFNVOffset64              uint64  = hatHash.FNVOffset64
	bloomFilterFNVPrime64               uint64  = hatHash.FNVPrime64
)

type BloomFilterInfo = hatDataStructure.BloomFilterInfo
type bloomFilterSnapshot = hatDataStructure.BloomFilterSnapshot

type bloomFilterData struct {
	filter hatDataStructure.BloomFilter
}

func newBloomFilterData(expectedItems uint64, falsePositiveRate float64) (bloomFilterData, error) {
	filter, err := hatDataStructure.NewBloomFilter(expectedItems, falsePositiveRate)
	if err != nil {
		return bloomFilterData{}, err
	}
	return bloomFilterData{filter: filter}, nil
}

func newDefaultBloomFilterData() bloomFilterData {
	data, err := newBloomFilterData(DefaultBloomFilterExpectedItems, DefaultBloomFilterFalsePositiveRate)
	if err != nil {
		panic(err)
	}
	return data
}

func newBloomFilterDataWithShape(bitCount uint64, hashCount uint8) bloomFilterData {
	filter, err := hatDataStructure.NewBloomFilterWithShape(bitCount, hashCount)
	if err != nil {
		return bloomFilterData{}
	}
	return bloomFilterData{filter: filter}
}

func bloomFilterShape(expectedItems uint64, falsePositiveRate float64) (uint64, uint8, error) {
	return hatDataStructure.BloomFilterShape(expectedItems, falsePositiveRate)
}

func validateBloomFilterSnapshot(snapshot bloomFilterSnapshot) error {
	return hatDataStructure.ValidateBloomFilterSnapshot(snapshot)
}

func validateBloomFilterUnusedBits(bitCount uint64, data []byte) error {
	return hatDataStructure.ValidateBloomFilterUnusedBits(bitCount, data)
}

func bloomFilterRawSetBits(data []byte) uint64 {
	return hatDataStructure.BloomFilterRawSetBits(data)
}

func newBloomFilterDataFromSnapshot(snapshot bloomFilterSnapshot) (bloomFilterData, error) {
	filter, err := hatDataStructure.NewBloomFilterFromSnapshot(snapshot)
	if err != nil {
		return bloomFilterData{}, err
	}
	return bloomFilterData{filter: filter}, nil
}

func (filter *bloomFilterData) Add(value interface{}) bool {
	added, _ := filter.AddChecked(value)
	return added
}

func (filter *bloomFilterData) AddChecked(value interface{}) (bool, error) {
	if filter == nil || filter.filter.BitCount() == 0 || filter.filter.HashCount() == 0 {
		return false, nil
	}
	key, err := bloomFilterItemKey(value)
	if err != nil {
		return false, err
	}
	return filter.filter.AddBytes(key), nil
}

func (filter *bloomFilterData) AddOne(value interface{}, values ...interface{}) int {
	added, _ := filter.AddOneChecked(value, values...)
	return added
}

func (filter *bloomFilterData) AddOneChecked(value interface{}, values ...interface{}) (int, error) {
	if filter == nil || filter.filter.BitCount() == 0 || filter.filter.HashCount() == 0 {
		return 0, nil
	}
	keys, err := bloomFilterItemKeys(value, values...)
	if err != nil {
		return 0, err
	}
	added := 0
	for _, key := range keys {
		if filter.addKey(key) {
			added++
		}
	}
	return added, nil
}

func (filter *bloomFilterData) addCommandBatch(values Slice) (int, error) {
	if filter == nil || filter.filter.BitCount() == 0 || filter.filter.HashCount() == 0 {
		return 0, nil
	}
	var encoded [][]byte
	for index, value := range values {
		text, ok := value.(string)
		if ok && commandFastCanonicalJSONString(text) {
			continue
		}
		if encoded == nil {
			encoded = make([][]byte, len(values))
		}
		key, err := bloomFilterItemKey(value)
		if err != nil {
			return 0, err
		}
		encoded[index] = key
	}

	added := 0
	if encoded == nil {
		for _, value := range values {
			if filter.addJSONString(value.(string)) {
				added++
			}
		}
		return added, nil
	}
	for index, value := range values {
		changed := false
		if encoded[index] != nil {
			changed = filter.addKey(encoded[index])
		} else {
			changed = filter.addJSONString(value.(string))
		}
		if changed {
			added++
		}
	}
	return added, nil
}

func (filter *bloomFilterData) addKey(key []byte) bool {
	return filter != nil && filter.filter.AddBytes(key)
}

func (filter *bloomFilterData) addJSONString(value string) bool {
	return filter != nil && filter.filter.AddJSONString(value)
}

func (filter *bloomFilterData) Contains(value interface{}) bool {
	contains, _ := filter.ContainsChecked(value)
	return contains
}

func (filter *bloomFilterData) ContainsChecked(value interface{}) (bool, error) {
	if filter == nil || filter.filter.BitCount() == 0 || filter.filter.HashCount() == 0 {
		return false, nil
	}
	key, err := bloomFilterItemKey(value)
	if err != nil {
		return false, err
	}
	return filter.filter.ContainsBytes(key), nil
}

func (filter *bloomFilterData) containsKey(key []byte) bool {
	return filter != nil && filter.filter.ContainsBytes(key)
}

func (filter *bloomFilterData) containsJSONString(value string) bool {
	return filter != nil && filter.filter.ContainsJSONString(value)
}

func (filter bloomFilterData) Info() BloomFilterInfo {
	return filter.filter.Info()
}

func (filter bloomFilterData) SetBits() uint64 {
	return filter.filter.SetBits()
}

func (filter bloomFilterData) Snapshot() bloomFilterSnapshot {
	return filter.filter.Snapshot()
}

func (filter bloomFilterData) EncodedSize() int64 {
	return filter.filter.EncodedSize()
}

func bloomFilterItemKeys(value interface{}, values ...interface{}) ([][]byte, error) {
	count, ok := checkedBatchSize(1, len(values))
	if !ok {
		return nil, errBatchSizeTooLarge
	}
	keys := make([][]byte, 0, count)
	key, err := bloomFilterItemKey(value)
	if err != nil {
		return nil, err
	}
	keys = append(keys, key)
	for _, value := range values {
		key, err := bloomFilterItemKey(value)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func bloomFilterItemKey(value interface{}) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("hatriecache: unsupported bloom filter value: %w", err)
	}
	return data, nil
}

func bloomFilterFNV64a(value []byte) uint64 {
	return hatHash.FNV1a64(value)
}

func bloomFilterFNV64(value []byte) uint64 {
	return hatHash.FNV1_64(value)
}

func bloomFilterFNV64aJSONString(value string) uint64 {
	return hatHash.FNV1a64JSONString(value)
}

func bloomFilterFNV64JSONString(value string) uint64 {
	return hatHash.FNV1_64JSONString(value)
}

func bloomFilterWordCount(bitCount uint64) uint64 {
	return hatDataStructure.BloomFilterWordCount(bitCount)
}

// BloomFilterStorage stores Bloom filter values outside the trie.
type BloomFilterStorage struct {
	array     []bloomFilterData
	reusables reusableIndexes
}

func CreateBloomFilterStorage() *BloomFilterStorage {
	return &BloomFilterStorage{
		array: []bloomFilterData{},
	}
}

func (store *BloomFilterStorage) PutData(idx int32, value bloomFilterData) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = value
	store.reusables.Use(idx)
}

func (store *BloomFilterStorage) AppendData(value bloomFilterData) int32 {
	store.array = append(store.array, value)
	return int32(len(store.array) - 1)
}

func (store *BloomFilterStorage) AddData(value bloomFilterData) int32 {
	if idx, ok := store.reusables.Take(); ok {
		store.array[idx] = value
		return idx
	}
	return store.AppendData(value)
}

func (store *BloomFilterStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = bloomFilterData{}
	store.reusables.Mark(idx)
	store.array = trimReusableTail(store.array, &store.reusables)
}

func (ht *HatTrie) UpsertBloomFilter(key string, expectedItems uint64, falsePositiveRate float64) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.UpsertBloomFilter(key, expectedItems, falsePositiveRate)
	}
	data, err := newBloomFilterData(expectedItems, falsePositiveRate)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsBloomFilter() {
		ht.bloomFilters.PutData(hval.Index, data)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.bloomFilters.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_BLOOM_FILTER}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) AddBloomFilter(key string, val interface{}, vals ...interface{}) int {
	added, _ := ht.AddBloomFilterChecked(key, val, vals...)
	return added
}

func (ht *HatTrie) AddBloomFilterChecked(key string, val interface{}, vals ...interface{}) (int, error) {
	if ht == nil {
		return 0, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.AddBloomFilterChecked(key, val, vals...)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return 0, err
	}
	if hval.IsBloomFilter() {
		added, err := ht.bloomFilters.array[hval.Index].AddOneChecked(val, vals...)
		if err != nil {
			return 0, err
		}
		*rawPtr = hval.toValue()
		if added > 0 {
			ht.recordWriteLocked(key)
		}
		return added, nil
	}

	data := newDefaultBloomFilterData()
	added, err := data.AddOneChecked(val, vals...)
	if err != nil {
		return 0, err
	}
	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.bloomFilters.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_BLOOM_FILTER}.toValue()
	ht.recordWriteLocked(key)
	return added, nil
}

func (ht *HatTrie) HasBloomFilter(key string, val interface{}) bool {
	hit, _ := ht.HasBloomFilterChecked(key, val)
	return hit
}

func (ht *HatTrie) HasBloomFilterChecked(key string, val interface{}) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.HasBloomFilterChecked(key, val)
	}
	if value, ok := val.(string); ok && !jsonPlainStringNeedsCanonicalKey(value) {
		return ht.hasBloomFilterPlainJSONStringChecked(key, value)
	}
	valueKey, err := bloomFilterItemKey(val)
	if err != nil {
		return false, err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return false, err
	}
	if !hval.IsBloomFilter() {
		ht.recordReadLocked(false, key)
		return false, nil
	}
	hit := ht.bloomFilters.array[hval.Index].containsKey(valueKey)
	ht.recordReadLocked(hit, key)
	return hit, nil
}

func (ht *HatTrie) hasBloomFilterPlainJSONStringChecked(key string, value string) (bool, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return false, err
	}
	if !hval.IsBloomFilter() {
		ht.recordReadLocked(false, key)
		return false, nil
	}
	hit := ht.bloomFilters.array[hval.Index].containsJSONString(value)
	ht.recordReadLocked(hit, key)
	return hit, nil
}

func (ht *HatTrie) BloomFilterInfo(key string) (BloomFilterInfo, bool) {
	info, ok, _ := ht.BloomFilterInfoChecked(key)
	return info, ok
}

func (ht *HatTrie) BloomFilterInfoChecked(key string) (BloomFilterInfo, bool, error) {
	if ht == nil {
		return BloomFilterInfo{}, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.BloomFilterInfoChecked(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return BloomFilterInfo{}, false, err
	}
	if !hval.IsBloomFilter() {
		ht.recordReadLocked(false, key)
		return BloomFilterInfo{}, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.bloomFilters.array[hval.Index].Info(), true, nil
}
