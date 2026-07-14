package hatriecache

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	mathbits "math/bits"
)

const (
	DefaultBloomFilterExpectedItems     uint64  = 10000
	DefaultBloomFilterFalsePositiveRate float64 = 0.01
	minBloomFilterBits                  uint64  = 64
	maxBloomFilterBits                  uint64  = 1 << 31
	maxBloomFilterHashes                uint8   = 64
	bloomFilterFNVOffset64              uint64  = 14695981039346656037
	bloomFilterFNVPrime64               uint64  = 1099511628211
)

// BloomFilterInfo reports the shape and current fill level of a Bloom filter.
// Bloom filters do not store their inserted values, only a compact bitset.
type BloomFilterInfo struct {
	BitCount                   uint64  `json:"bit_count"`
	BitBytes                   uint64  `json:"bit_bytes"`
	HashCount                  uint8   `json:"hash_count"`
	Insertions                 uint64  `json:"insertions"`
	SetBits                    uint64  `json:"set_bits"`
	FillRatio                  float64 `json:"fill_ratio"`
	EstimatedFalsePositiveRate float64 `json:"estimated_false_positive_rate"`
}

type bloomFilterSnapshot struct {
	BitCount   uint64 `json:"bit_count"`
	HashCount  uint8  `json:"hash_count"`
	Insertions uint64 `json:"insertions"`
	Bits       string `json:"bits"`
}

type bloomFilterData struct {
	words      []uint64
	bitCount   uint64
	hashCount  uint8
	insertions uint64
}

func newBloomFilterData(expectedItems uint64, falsePositiveRate float64) (bloomFilterData, error) {
	bitCount, hashCount, err := bloomFilterShape(expectedItems, falsePositiveRate)
	if err != nil {
		return bloomFilterData{}, err
	}
	return newBloomFilterDataWithShape(bitCount, hashCount), nil
}

func newDefaultBloomFilterData() bloomFilterData {
	data, err := newBloomFilterData(DefaultBloomFilterExpectedItems, DefaultBloomFilterFalsePositiveRate)
	if err != nil {
		panic(err)
	}
	return data
}

func newBloomFilterDataWithShape(bitCount uint64, hashCount uint8) bloomFilterData {
	wordCount := bloomFilterWordCount(bitCount)
	return bloomFilterData{
		words:     make([]uint64, int(wordCount)),
		bitCount:  bitCount,
		hashCount: hashCount,
	}
}

func bloomFilterShape(expectedItems uint64, falsePositiveRate float64) (uint64, uint8, error) {
	if expectedItems == 0 {
		return 0, 0, errors.New("hatriecache: bloom filter expected items must be positive")
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 || math.IsNaN(falsePositiveRate) {
		return 0, 0, errors.New("hatriecache: bloom filter false positive rate must be between 0 and 1")
	}

	bits := math.Ceil(-float64(expectedItems) * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2))
	if math.IsInf(bits, 0) || bits > float64(maxBloomFilterBits) {
		return 0, 0, errors.New("hatriecache: bloom filter bit count is too large")
	}
	bitCount := uint64(bits)
	if bitCount < minBloomFilterBits {
		bitCount = minBloomFilterBits
	}

	hashes := math.Ceil((float64(bitCount) / float64(expectedItems)) * math.Ln2)
	if math.IsInf(hashes, 0) || hashes < 1 {
		hashes = 1
	}
	if hashes > float64(maxBloomFilterHashes) {
		return 0, 0, errors.New("hatriecache: bloom filter hash count is too large")
	}
	return bitCount, uint8(hashes), nil
}

func validateBloomFilterSnapshot(snapshot bloomFilterSnapshot) error {
	if snapshot.BitCount < minBloomFilterBits || snapshot.BitCount > maxBloomFilterBits {
		return errors.New("hatriecache: invalid bloom filter bit count")
	}
	if snapshot.HashCount == 0 || snapshot.HashCount > maxBloomFilterHashes {
		return errors.New("hatriecache: invalid bloom filter hash count")
	}
	data, err := base64.StdEncoding.DecodeString(snapshot.Bits)
	if err != nil {
		return err
	}
	if uint64(len(data)) != bloomFilterWordCount(snapshot.BitCount)*8 {
		return errors.New("hatriecache: invalid bloom filter bitset length")
	}
	return nil
}

func newBloomFilterDataFromSnapshot(snapshot bloomFilterSnapshot) (bloomFilterData, error) {
	if err := validateBloomFilterSnapshot(snapshot); err != nil {
		return bloomFilterData{}, err
	}
	raw, err := base64.StdEncoding.DecodeString(snapshot.Bits)
	if err != nil {
		return bloomFilterData{}, err
	}
	words := make([]uint64, len(raw)/8)
	for idx := range words {
		words[idx] = binary.LittleEndian.Uint64(raw[idx*8 : idx*8+8])
	}
	out := bloomFilterData{
		words:      words,
		bitCount:   snapshot.BitCount,
		hashCount:  snapshot.HashCount,
		insertions: snapshot.Insertions,
	}
	out.maskUnusedBits()
	return out, nil
}

func (filter *bloomFilterData) Add(value interface{}) bool {
	added, _ := filter.AddChecked(value)
	return added
}

func (filter *bloomFilterData) AddChecked(value interface{}) (bool, error) {
	added, err := filter.AddOneChecked(value)
	return added > 0, err
}

func (filter *bloomFilterData) AddOne(value interface{}, values ...interface{}) int {
	added, _ := filter.AddOneChecked(value, values...)
	return added
}

func (filter *bloomFilterData) AddOneChecked(value interface{}, values ...interface{}) (int, error) {
	if filter == nil || filter.bitCount == 0 || filter.hashCount == 0 {
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

func (filter *bloomFilterData) addKey(key []byte) bool {
	changed := false
	filter.visitIndexes(key, func(index uint64) {
		word := index / 64
		mask := uint64(1) << uint(index%64)
		if filter.words[word]&mask == 0 {
			filter.words[word] |= mask
			changed = true
		}
	})
	if changed {
		filter.insertions++
	}
	return changed
}

func (filter *bloomFilterData) Contains(value interface{}) bool {
	contains, _ := filter.ContainsChecked(value)
	return contains
}

func (filter *bloomFilterData) ContainsChecked(value interface{}) (bool, error) {
	if filter == nil || filter.bitCount == 0 || filter.hashCount == 0 {
		return false, nil
	}
	key, err := bloomFilterItemKey(value)
	if err != nil {
		return false, err
	}
	return filter.containsKey(key), nil
}

func (filter *bloomFilterData) containsKey(key []byte) bool {
	contains := true
	filter.visitIndexes(key, func(index uint64) {
		word := index / 64
		mask := uint64(1) << uint(index%64)
		if filter.words[word]&mask == 0 {
			contains = false
		}
	})
	return contains
}

func (filter bloomFilterData) Info() BloomFilterInfo {
	setBits := filter.SetBits()
	fillRatio := 0.0
	if filter.bitCount > 0 {
		fillRatio = float64(setBits) / float64(filter.bitCount)
	}
	return BloomFilterInfo{
		BitCount:                   filter.bitCount,
		BitBytes:                   bloomFilterWordCount(filter.bitCount) * 8,
		HashCount:                  filter.hashCount,
		Insertions:                 filter.insertions,
		SetBits:                    setBits,
		FillRatio:                  fillRatio,
		EstimatedFalsePositiveRate: math.Pow(fillRatio, float64(filter.hashCount)),
	}
}

func (filter bloomFilterData) SetBits() uint64 {
	var total uint64
	for _, word := range filter.words {
		total += uint64(mathbits.OnesCount64(word))
	}
	return total
}

func (filter bloomFilterData) Snapshot() bloomFilterSnapshot {
	data := make([]byte, len(filter.words)*8)
	for idx, word := range filter.words {
		binary.LittleEndian.PutUint64(data[idx*8:idx*8+8], word)
	}
	return bloomFilterSnapshot{
		BitCount:   filter.bitCount,
		HashCount:  filter.hashCount,
		Insertions: filter.insertions,
		Bits:       base64.StdEncoding.EncodeToString(data),
	}
}

func (filter bloomFilterData) EncodedSize() int64 {
	return int64(len(filter.words) * 8)
}

func (filter *bloomFilterData) visitIndexes(key []byte, visit func(uint64)) {
	first := bloomFilterFNV64a(key)
	step := bloomFilterFNV64(key)
	if step == 0 {
		step = bloomFilterFNVPrime64
	}
	step |= 1
	for idx := uint8(0); idx < filter.hashCount; idx++ {
		visit((first + uint64(idx)*step) % filter.bitCount)
	}
}

func (filter *bloomFilterData) maskUnusedBits() {
	if filter == nil || len(filter.words) == 0 || filter.bitCount%64 == 0 {
		return
	}
	mask := (uint64(1) << uint(filter.bitCount%64)) - 1
	filter.words[len(filter.words)-1] &= mask
}

func bloomFilterItemKeys(value interface{}, values ...interface{}) ([][]byte, error) {
	keys := make([][]byte, 0, 1+len(values))
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
	hash := bloomFilterFNVOffset64
	for idx := 0; idx < len(value); idx++ {
		hash ^= uint64(value[idx])
		hash *= bloomFilterFNVPrime64
	}
	return hash
}

func bloomFilterFNV64(value []byte) uint64 {
	hash := bloomFilterFNVOffset64
	for idx := 0; idx < len(value); idx++ {
		hash *= bloomFilterFNVPrime64
		hash ^= uint64(value[idx])
	}
	return hash
}

func bloomFilterWordCount(bitCount uint64) uint64 {
	return (bitCount + 63) / 64
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
	data, err := newBloomFilterData(expectedItems, falsePositiveRate)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertReplacementLocation(key)
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
		ht.recordWriteLocked(key)
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

func (ht *HatTrie) BloomFilterInfo(key string) (BloomFilterInfo, bool) {
	info, ok, _ := ht.BloomFilterInfoChecked(key)
	return info, ok
}

func (ht *HatTrie) BloomFilterInfoChecked(key string) (BloomFilterInfo, bool, error) {
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
