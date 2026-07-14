package hatriecache

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
)

const (
	DefaultCuckooFilterCapacity          uint64  = 10000
	DefaultCuckooFilterFalsePositiveRate float64 = 0.01
	cuckooFilterBucketSize               uint8   = 4
	cuckooFilterTargetLoad               float64 = 0.95
	cuckooFilterMaxKicks                 int     = 500
	minCuckooFilterBuckets               uint64  = 2
	maxCuckooFilterBuckets               uint64  = 1 << 24
	minCuckooFilterFingerprintBits       uint8   = 4
	maxCuckooFilterFingerprintBits       uint8   = 16
)

// CuckooFilterInfo reports the shape and fill level of a Cuckoo filter.
// Cuckoo filters store compact fingerprints and support approximate deletes.
type CuckooFilterInfo struct {
	BucketCount                uint64  `json:"bucket_count"`
	BucketSize                 uint8   `json:"bucket_size"`
	Capacity                   uint64  `json:"capacity"`
	Count                      uint64  `json:"count"`
	FingerprintBits            uint8   `json:"fingerprint_bits"`
	FingerprintBytes           uint64  `json:"fingerprint_bytes"`
	LoadFactor                 float64 `json:"load_factor"`
	EstimatedFalsePositiveRate float64 `json:"estimated_false_positive_rate"`
}

type cuckooFilterSnapshot struct {
	BucketCount     uint64 `json:"bucket_count"`
	BucketSize      uint8  `json:"bucket_size"`
	FingerprintBits uint8  `json:"fingerprint_bits"`
	Count           uint64 `json:"count"`
	Fingerprints    string `json:"fingerprints"`
}

type cuckooFilterData struct {
	fingerprints    []uint16
	bucketCount     uint64
	fingerprintBits uint8
	count           uint64
}

func newCuckooFilterData(capacity uint64, falsePositiveRate float64) (cuckooFilterData, error) {
	bucketCount, fingerprintBits, err := cuckooFilterShape(capacity, falsePositiveRate)
	if err != nil {
		return cuckooFilterData{}, err
	}
	return newCuckooFilterDataWithShape(bucketCount, fingerprintBits), nil
}

func newDefaultCuckooFilterData() cuckooFilterData {
	data, err := newCuckooFilterData(DefaultCuckooFilterCapacity, DefaultCuckooFilterFalsePositiveRate)
	if err != nil {
		panic(err)
	}
	return data
}

func newCuckooFilterDataWithShape(bucketCount uint64, fingerprintBits uint8) cuckooFilterData {
	return cuckooFilterData{
		bucketCount:     bucketCount,
		fingerprintBits: fingerprintBits,
	}
}

func cuckooFilterShape(capacity uint64, falsePositiveRate float64) (uint64, uint8, error) {
	if capacity == 0 {
		return 0, 0, errors.New("hatriecache: cuckoo filter capacity must be positive")
	}
	if falsePositiveRate <= 0 || falsePositiveRate >= 1 || math.IsNaN(falsePositiveRate) {
		return 0, 0, errors.New("hatriecache: cuckoo filter false positive rate must be between 0 and 1")
	}
	bitsNeeded := math.Ceil(math.Log2((2 * float64(cuckooFilterBucketSize)) / falsePositiveRate))
	if math.IsInf(bitsNeeded, 0) || bitsNeeded > float64(maxCuckooFilterFingerprintBits) {
		return 0, 0, errors.New("hatriecache: cuckoo filter false positive rate is too small")
	}
	if bitsNeeded < float64(minCuckooFilterFingerprintBits) {
		bitsNeeded = float64(minCuckooFilterFingerprintBits)
	}
	bucketsNeeded := math.Ceil(float64(capacity) / (float64(cuckooFilterBucketSize) * cuckooFilterTargetLoad))
	if math.IsInf(bucketsNeeded, 0) || bucketsNeeded > float64(maxCuckooFilterBuckets) {
		return 0, 0, errors.New("hatriecache: cuckoo filter bucket count is too large")
	}
	bucketCount := nextPowerOfTwoUint64(uint64(bucketsNeeded))
	if bucketCount < minCuckooFilterBuckets {
		bucketCount = minCuckooFilterBuckets
	}
	if bucketCount > maxCuckooFilterBuckets {
		return 0, 0, errors.New("hatriecache: cuckoo filter bucket count is too large")
	}
	return bucketCount, uint8(bitsNeeded), nil
}

func validateCuckooFilterSnapshot(snapshot cuckooFilterSnapshot) error {
	if snapshot.BucketSize != cuckooFilterBucketSize {
		return errors.New("hatriecache: invalid cuckoo filter bucket size")
	}
	if snapshot.BucketCount < minCuckooFilterBuckets || snapshot.BucketCount > maxCuckooFilterBuckets || !isPowerOfTwoUint64(snapshot.BucketCount) {
		return errors.New("hatriecache: invalid cuckoo filter bucket count")
	}
	if snapshot.FingerprintBits < minCuckooFilterFingerprintBits || snapshot.FingerprintBits > maxCuckooFilterFingerprintBits {
		return errors.New("hatriecache: invalid cuckoo filter fingerprint bits")
	}
	if snapshot.Count > snapshot.BucketCount*uint64(cuckooFilterBucketSize) {
		return errors.New("hatriecache: invalid cuckoo filter count")
	}
	raw, err := base64.StdEncoding.DecodeString(snapshot.Fingerprints)
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		if snapshot.Count != 0 {
			return errors.New("hatriecache: empty cuckoo filter fingerprints have count")
		}
		return nil
	}
	if uint64(len(raw)) != snapshot.BucketCount*uint64(cuckooFilterBucketSize)*2 {
		return errors.New("hatriecache: invalid cuckoo filter fingerprint length")
	}
	occupied, err := cuckooFilterRawOccupied(raw, snapshot.FingerprintBits)
	if err != nil {
		return err
	}
	if occupied != snapshot.Count {
		return errors.New("hatriecache: cuckoo filter count does not match fingerprints")
	}
	return nil
}

func cuckooFilterRawOccupied(raw []byte, fingerprintBits uint8) (uint64, error) {
	var occupied uint64
	mask := cuckooFilterFingerprintMask(fingerprintBits)
	for idx := 0; idx < len(raw)/2; idx++ {
		fp := binary.LittleEndian.Uint16(raw[idx*2 : idx*2+2])
		if fp == 0 {
			continue
		}
		if fp&^mask != 0 {
			return 0, errors.New("hatriecache: invalid cuckoo filter fingerprint")
		}
		occupied++
	}
	return occupied, nil
}

func newCuckooFilterDataFromSnapshot(snapshot cuckooFilterSnapshot) (cuckooFilterData, error) {
	if err := validateCuckooFilterSnapshot(snapshot); err != nil {
		return cuckooFilterData{}, err
	}
	raw, err := base64.StdEncoding.DecodeString(snapshot.Fingerprints)
	if err != nil {
		return cuckooFilterData{}, err
	}
	out := cuckooFilterData{
		bucketCount:     snapshot.BucketCount,
		fingerprintBits: snapshot.FingerprintBits,
		count:           snapshot.Count,
	}
	if len(raw) == 0 || snapshot.Count == 0 {
		return out, nil
	}
	out.fingerprints = make([]uint16, len(raw)/2)
	for idx := range out.fingerprints {
		out.fingerprints[idx] = binary.LittleEndian.Uint16(raw[idx*2 : idx*2+2])
	}
	return out, nil
}

func (filter *cuckooFilterData) Add(value interface{}) bool {
	added, _ := filter.AddChecked(value)
	return added
}

func (filter *cuckooFilterData) AddChecked(value interface{}) (bool, error) {
	added, err := filter.AddOneChecked(value)
	return added > 0, err
}

func (filter *cuckooFilterData) AddOne(value interface{}, values ...interface{}) int {
	added, _ := filter.AddOneChecked(value, values...)
	return added
}

func (filter *cuckooFilterData) AddOneChecked(value interface{}, values ...interface{}) (int, error) {
	if filter == nil || filter.bucketCount == 0 || filter.fingerprintBits == 0 {
		return 0, nil
	}
	keys, err := cuckooFilterItemKeys(value, values...)
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

func (filter *cuckooFilterData) addKey(key []byte) bool {
	filter.ensureFingerprints()
	hash := bloomFilterFNV64a(key)
	fp := filter.fingerprint(hash)
	index := filter.index(hash)
	alternate := filter.alternateIndex(index, fp)
	if filter.containsFingerprint(index, alternate, fp) {
		return false
	}
	if filter.insertIntoBucket(index, fp) || filter.insertIntoBucket(alternate, fp) {
		filter.count++
		return true
	}
	if filter.relocateAndInsert(index, alternate, fp, hash) {
		filter.count++
		return true
	}
	return false
}

func (filter *cuckooFilterData) Contains(value interface{}) bool {
	contains, _ := filter.ContainsChecked(value)
	return contains
}

func (filter *cuckooFilterData) ContainsChecked(value interface{}) (bool, error) {
	if filter == nil || filter.bucketCount == 0 || filter.fingerprintBits == 0 {
		return false, nil
	}
	key, err := cuckooFilterItemKey(value)
	if err != nil {
		return false, err
	}
	return filter.containsKey(key), nil
}

func (filter *cuckooFilterData) containsKey(key []byte) bool {
	if len(filter.fingerprints) == 0 {
		return false
	}
	hash := bloomFilterFNV64a(key)
	fp := filter.fingerprint(hash)
	index := filter.index(hash)
	return filter.containsFingerprint(index, filter.alternateIndex(index, fp), fp)
}

func (filter *cuckooFilterData) Delete(value interface{}) bool {
	deleted, _ := filter.DeleteChecked(value)
	return deleted
}

func (filter *cuckooFilterData) DeleteChecked(value interface{}) (bool, error) {
	deleted, err := filter.DeleteOneChecked(value)
	return deleted > 0, err
}

func (filter *cuckooFilterData) DeleteOne(value interface{}, values ...interface{}) int {
	deleted, _ := filter.DeleteOneChecked(value, values...)
	return deleted
}

func (filter *cuckooFilterData) DeleteOneChecked(value interface{}, values ...interface{}) (int, error) {
	if filter == nil || filter.bucketCount == 0 || filter.fingerprintBits == 0 {
		return 0, nil
	}
	keys, err := cuckooFilterItemKeys(value, values...)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, key := range keys {
		if filter.deleteKey(key) {
			deleted++
		}
	}
	return deleted, nil
}

func (filter *cuckooFilterData) deleteKey(key []byte) bool {
	if len(filter.fingerprints) == 0 {
		return false
	}
	hash := bloomFilterFNV64a(key)
	fp := filter.fingerprint(hash)
	index := filter.index(hash)
	if filter.deleteFingerprint(index, fp) || filter.deleteFingerprint(filter.alternateIndex(index, fp), fp) {
		filter.count--
		return true
	}
	return false
}

func (filter cuckooFilterData) Info() CuckooFilterInfo {
	capacity := filter.bucketCount * uint64(cuckooFilterBucketSize)
	loadFactor := 0.0
	if capacity > 0 {
		loadFactor = float64(filter.count) / float64(capacity)
	}
	return CuckooFilterInfo{
		BucketCount:                filter.bucketCount,
		BucketSize:                 cuckooFilterBucketSize,
		Capacity:                   capacity,
		Count:                      filter.count,
		FingerprintBits:            filter.fingerprintBits,
		FingerprintBytes:           uint64(len(filter.fingerprints)) * 2,
		LoadFactor:                 loadFactor,
		EstimatedFalsePositiveRate: cuckooFilterEstimatedFalsePositiveRate(filter.fingerprintBits),
	}
}

func (filter cuckooFilterData) Snapshot() cuckooFilterSnapshot {
	var raw []byte
	if len(filter.fingerprints) > 0 {
		raw = make([]byte, len(filter.fingerprints)*2)
		for idx, fingerprint := range filter.fingerprints {
			binary.LittleEndian.PutUint16(raw[idx*2:idx*2+2], fingerprint)
		}
	}
	return cuckooFilterSnapshot{
		BucketCount:     filter.bucketCount,
		BucketSize:      cuckooFilterBucketSize,
		FingerprintBits: filter.fingerprintBits,
		Count:           filter.count,
		Fingerprints:    base64.StdEncoding.EncodeToString(raw),
	}
}

func (filter cuckooFilterData) EncodedSize() int64 {
	return int64(len(filter.fingerprints) * 2)
}

func (filter *cuckooFilterData) ensureFingerprints() {
	if filter == nil || len(filter.fingerprints) > 0 || filter.bucketCount == 0 {
		return
	}
	filter.fingerprints = make([]uint16, int(filter.bucketCount)*int(cuckooFilterBucketSize))
}

func (filter *cuckooFilterData) relocateAndInsert(index uint64, alternate uint64, fp uint16, hash uint64) bool {
	currentIndex := index
	if (splitmix64(hash)^uint64(fp))&1 == 1 {
		currentIndex = alternate
	}
	currentFingerprint := fp
	type relocation struct {
		index    uint64
		slot     int
		previous uint16
	}
	var path [cuckooFilterMaxKicks]relocation
	pathLen := 0
	for kick := 0; kick < cuckooFilterMaxKicks; kick++ {
		slot := int(splitmix64(hash+uint64(kick)*0x9e3779b97f4a7c15+uint64(currentFingerprint)) % uint64(cuckooFilterBucketSize))
		offset := filter.bucketOffset(currentIndex, slot)
		evicted := filter.fingerprints[offset]
		filter.fingerprints[offset] = currentFingerprint
		path[pathLen] = relocation{index: currentIndex, slot: slot, previous: evicted}
		pathLen++

		currentFingerprint = evicted
		currentIndex = filter.alternateIndex(currentIndex, currentFingerprint)
		if filter.insertIntoBucket(currentIndex, currentFingerprint) {
			return true
		}
	}
	for idx := pathLen - 1; idx >= 0; idx-- {
		step := path[idx]
		filter.fingerprints[filter.bucketOffset(step.index, step.slot)] = step.previous
	}
	return false
}

func (filter *cuckooFilterData) insertIntoBucket(index uint64, fp uint16) bool {
	start := filter.bucketOffset(index, 0)
	for slot := 0; slot < int(cuckooFilterBucketSize); slot++ {
		if filter.fingerprints[start+slot] == 0 {
			filter.fingerprints[start+slot] = fp
			return true
		}
	}
	return false
}

func (filter *cuckooFilterData) containsFingerprint(index uint64, alternate uint64, fp uint16) bool {
	return filter.bucketContains(index, fp) || filter.bucketContains(alternate, fp)
}

func (filter *cuckooFilterData) bucketContains(index uint64, fp uint16) bool {
	start := filter.bucketOffset(index, 0)
	for slot := 0; slot < int(cuckooFilterBucketSize); slot++ {
		if filter.fingerprints[start+slot] == fp {
			return true
		}
	}
	return false
}

func (filter *cuckooFilterData) deleteFingerprint(index uint64, fp uint16) bool {
	start := filter.bucketOffset(index, 0)
	for slot := 0; slot < int(cuckooFilterBucketSize); slot++ {
		if filter.fingerprints[start+slot] == fp {
			filter.fingerprints[start+slot] = 0
			return true
		}
	}
	return false
}

func (filter *cuckooFilterData) bucketOffset(index uint64, slot int) int {
	return int(index)*int(cuckooFilterBucketSize) + slot
}

func (filter *cuckooFilterData) index(hash uint64) uint64 {
	return hash & (filter.bucketCount - 1)
}

func (filter *cuckooFilterData) alternateIndex(index uint64, fp uint16) uint64 {
	delta := splitmix64(uint64(fp)) & (filter.bucketCount - 1)
	if delta == 0 {
		delta = 1
	}
	return (index ^ delta) & (filter.bucketCount - 1)
}

func (filter *cuckooFilterData) fingerprint(hash uint64) uint16 {
	fp := uint16((hash >> 32) & uint64(cuckooFilterFingerprintMask(filter.fingerprintBits)))
	if fp == 0 {
		return 1
	}
	return fp
}

func cuckooFilterFingerprintMask(bits uint8) uint16 {
	if bits >= 16 {
		return ^uint16(0)
	}
	return uint16((uint32(1) << uint(bits)) - 1)
}

func cuckooFilterEstimatedFalsePositiveRate(fingerprintBits uint8) float64 {
	if fingerprintBits == 0 {
		return 0
	}
	return 1 - math.Pow(1-math.Pow(2, -float64(fingerprintBits)), float64(2*cuckooFilterBucketSize))
}

func cuckooFilterItemKeys(value interface{}, values ...interface{}) ([][]byte, error) {
	count, ok := checkedBatchSize(1, len(values))
	if !ok {
		return nil, errBatchSizeTooLarge
	}
	keys := make([][]byte, 0, count)
	key, err := cuckooFilterItemKey(value)
	if err != nil {
		return nil, err
	}
	keys = append(keys, key)
	for _, value := range values {
		key, err := cuckooFilterItemKey(value)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func cuckooFilterItemKey(value interface{}) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("hatriecache: unsupported cuckoo filter value: %w", err)
	}
	return data, nil
}

func nextPowerOfTwoUint64(value uint64) uint64 {
	if value <= 1 {
		return 1
	}
	if value > 1<<63 {
		return 0
	}
	return uint64(1) << uint(bits.Len64(value-1))
}

func isPowerOfTwoUint64(value uint64) bool {
	return value > 0 && value&(value-1) == 0
}

func splitmix64(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

// CuckooFilterStorage stores Cuckoo filter values outside the trie.
type CuckooFilterStorage struct {
	array     []cuckooFilterData
	reusables reusableIndexes
}

func CreateCuckooFilterStorage() *CuckooFilterStorage {
	return &CuckooFilterStorage{
		array: []cuckooFilterData{},
	}
}

func (store *CuckooFilterStorage) PutData(idx int32, value cuckooFilterData) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = value
	store.reusables.Use(idx)
}

func (store *CuckooFilterStorage) AppendData(value cuckooFilterData) int32 {
	store.array = append(store.array, value)
	return int32(len(store.array) - 1)
}

func (store *CuckooFilterStorage) AddData(value cuckooFilterData) int32 {
	if idx, ok := store.reusables.Take(); ok {
		store.array[idx] = value
		return idx
	}
	return store.AppendData(value)
}

func (store *CuckooFilterStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = cuckooFilterData{}
	store.reusables.Mark(idx)
	store.array = trimReusableTail(store.array, &store.reusables)
}

func (ht *HatTrie) UpsertCuckooFilter(key string, capacity uint64, falsePositiveRate float64) error {
	data, err := newCuckooFilterData(capacity, falsePositiveRate)
	if err != nil {
		return err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsCuckooFilter() {
		ht.cuckooFilters.PutData(hval.Index, data)
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.cuckooFilters.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_CUCKOO_FILTER}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) AddCuckooFilter(key string, val interface{}, vals ...interface{}) int {
	added, _ := ht.AddCuckooFilterChecked(key, val, vals...)
	return added
}

func (ht *HatTrie) AddCuckooFilterChecked(key string, val interface{}, vals ...interface{}) (int, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return 0, err
	}
	if hval.IsCuckooFilter() {
		added, err := ht.cuckooFilters.array[hval.Index].AddOneChecked(val, vals...)
		if err != nil {
			return 0, err
		}
		*rawPtr = hval.toValue()
		if added > 0 {
			ht.recordWriteLocked(key)
		}
		return added, nil
	}

	data := newDefaultCuckooFilterData()
	added, err := data.AddOneChecked(val, vals...)
	if err != nil {
		return 0, err
	}
	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.cuckooFilters.AddData(data)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_CUCKOO_FILTER}.toValue()
	ht.recordWriteLocked(key)
	return added, nil
}

func (ht *HatTrie) HasCuckooFilter(key string, val interface{}) bool {
	hit, _ := ht.HasCuckooFilterChecked(key, val)
	return hit
}

func (ht *HatTrie) HasCuckooFilterChecked(key string, val interface{}) (bool, error) {
	valueKey, err := cuckooFilterItemKey(val)
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
	if !hval.IsCuckooFilter() {
		ht.recordReadLocked(false, key)
		return false, nil
	}
	hit := ht.cuckooFilters.array[hval.Index].containsKey(valueKey)
	ht.recordReadLocked(hit, key)
	return hit, nil
}

func (ht *HatTrie) DeleteCuckooFilter(key string, val interface{}, vals ...interface{}) int {
	deleted, _ := ht.DeleteCuckooFilterChecked(key, val, vals...)
	return deleted
}

func (ht *HatTrie) DeleteCuckooFilterChecked(key string, val interface{}, vals ...interface{}) (int, error) {
	valueKeys, err := cuckooFilterItemKeys(val, vals...)
	if err != nil {
		return 0, err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return 0, err
	}
	if !hval.IsCuckooFilter() {
		ht.recordReadLocked(false, key)
		return 0, nil
	}
	deleted := 0
	filter := &ht.cuckooFilters.array[hval.Index]
	for _, valueKey := range valueKeys {
		if filter.deleteKey(valueKey) {
			deleted++
		}
	}
	ht.recordReadLocked(deleted > 0, key)
	if deleted > 0 {
		ht.recordWriteLocked(key)
	}
	return deleted, nil
}

func (ht *HatTrie) CuckooFilterInfo(key string) (CuckooFilterInfo, bool) {
	info, ok, _ := ht.CuckooFilterInfoChecked(key)
	return info, ok
}

func (ht *HatTrie) CuckooFilterInfoChecked(key string) (CuckooFilterInfo, bool, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return CuckooFilterInfo{}, false, err
	}
	if !hval.IsCuckooFilter() {
		ht.recordReadLocked(false, key)
		return CuckooFilterInfo{}, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.cuckooFilters.array[hval.Index].Info(), true, nil
}
