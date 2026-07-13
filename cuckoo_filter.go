package hatriecache

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
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
		fingerprints:    make([]uint16, int(bucketCount)*int(cuckooFilterBucketSize)),
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
	if uint64(len(raw)) != snapshot.BucketCount*uint64(cuckooFilterBucketSize)*2 {
		return errors.New("hatriecache: invalid cuckoo filter fingerprint length")
	}
	var occupied uint64
	mask := cuckooFilterFingerprintMask(snapshot.FingerprintBits)
	for idx := 0; idx < len(raw)/2; idx++ {
		fp := binary.LittleEndian.Uint16(raw[idx*2 : idx*2+2])
		if fp == 0 {
			continue
		}
		if fp&^mask != 0 {
			return errors.New("hatriecache: invalid cuckoo filter fingerprint")
		}
		occupied++
	}
	if occupied != snapshot.Count {
		return errors.New("hatriecache: cuckoo filter count does not match fingerprints")
	}
	return nil
}

func newCuckooFilterDataFromSnapshot(snapshot cuckooFilterSnapshot) (cuckooFilterData, error) {
	if err := validateCuckooFilterSnapshot(snapshot); err != nil {
		return cuckooFilterData{}, err
	}
	raw, err := base64.StdEncoding.DecodeString(snapshot.Fingerprints)
	if err != nil {
		return cuckooFilterData{}, err
	}
	fingerprints := make([]uint16, len(raw)/2)
	for idx := range fingerprints {
		fingerprints[idx] = binary.LittleEndian.Uint16(raw[idx*2 : idx*2+2])
	}
	return cuckooFilterData{
		fingerprints:    fingerprints,
		bucketCount:     snapshot.BucketCount,
		fingerprintBits: snapshot.FingerprintBits,
		count:           snapshot.Count,
	}, nil
}

func (filter *cuckooFilterData) Add(value interface{}) bool {
	if filter == nil || filter.bucketCount == 0 || filter.fingerprintBits == 0 {
		return false
	}
	key := mustCuckooFilterItemKey(value)
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

func (filter *cuckooFilterData) AddOne(value interface{}, values ...interface{}) int {
	added := 0
	if filter.Add(value) {
		added++
	}
	for _, value := range values {
		if filter.Add(value) {
			added++
		}
	}
	return added
}

func (filter *cuckooFilterData) Contains(value interface{}) bool {
	if filter == nil || filter.bucketCount == 0 || filter.fingerprintBits == 0 {
		return false
	}
	key := mustCuckooFilterItemKey(value)
	hash := bloomFilterFNV64a(key)
	fp := filter.fingerprint(hash)
	index := filter.index(hash)
	return filter.containsFingerprint(index, filter.alternateIndex(index, fp), fp)
}

func (filter *cuckooFilterData) Delete(value interface{}) bool {
	if filter == nil || filter.bucketCount == 0 || filter.fingerprintBits == 0 {
		return false
	}
	key := mustCuckooFilterItemKey(value)
	hash := bloomFilterFNV64a(key)
	fp := filter.fingerprint(hash)
	index := filter.index(hash)
	if filter.deleteFingerprint(index, fp) || filter.deleteFingerprint(filter.alternateIndex(index, fp), fp) {
		filter.count--
		return true
	}
	return false
}

func (filter *cuckooFilterData) DeleteOne(value interface{}, values ...interface{}) int {
	deleted := 0
	if filter.Delete(value) {
		deleted++
	}
	for _, value := range values {
		if filter.Delete(value) {
			deleted++
		}
	}
	return deleted
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
	raw := make([]byte, len(filter.fingerprints)*2)
	for idx, fingerprint := range filter.fingerprints {
		binary.LittleEndian.PutUint16(raw[idx*2:idx*2+2], fingerprint)
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

func mustCuckooFilterItemKey(value interface{}) []byte {
	key, err := cuckooFilterItemKey(value)
	if err != nil {
		panic(err)
	}
	return key
}

func cuckooFilterItemKey(value interface{}) ([]byte, error) {
	return json.Marshal(value)
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

	rawPtr, hval := ht.upsertFreshLocation(key)
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
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsCuckooFilter() {
		added := ht.cuckooFilters.array[hval.Index].AddOne(val, vals...)
		*rawPtr = hval.toValue()
		if added > 0 {
			ht.recordWriteLocked(key)
		}
		return added
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.cuckooFilters.AddData(newDefaultCuckooFilterData())
	added := ht.cuckooFilters.array[idx].AddOne(val, vals...)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_CUCKOO_FILTER}.toValue()
	ht.recordWriteLocked(key)
	return added
}

func (ht *HatTrie) HasCuckooFilter(key string, val interface{}) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsCuckooFilter() {
		ht.recordReadLocked(false, key)
		return false
	}
	hit := ht.cuckooFilters.array[hval.Index].Contains(val)
	ht.recordReadLocked(hit, key)
	return hit
}

func (ht *HatTrie) DeleteCuckooFilter(key string, val interface{}, vals ...interface{}) int {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsCuckooFilter() {
		ht.recordReadLocked(false, key)
		return 0
	}
	deleted := ht.cuckooFilters.array[hval.Index].DeleteOne(val, vals...)
	ht.recordReadLocked(deleted > 0, key)
	if deleted > 0 {
		ht.recordWriteLocked(key)
	}
	return deleted
}

func (ht *HatTrie) CuckooFilterInfo(key string) (CuckooFilterInfo, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsCuckooFilter() {
		ht.recordReadLocked(false, key)
		return CuckooFilterInfo{}, false
	}
	ht.recordReadLocked(true, key)
	return ht.cuckooFilters.array[hval.Index].Info(), true
}
