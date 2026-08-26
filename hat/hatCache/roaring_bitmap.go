package hatCache

import (
	"errors"

	"hatrie_cache/hat/hatDataStructure"
)

const (
	roaringBitmapBitmapWords        = hatDataStructure.RoaringBitmapBitmapWords
	roaringBitmapMaxContainerCount  = hatDataStructure.RoaringBitmapMaxContainerCount
	roaringBitmapArrayMaxSize       = hatDataStructure.RoaringBitmapArrayMaxSize
	roaringBitmapArrayShrinkSize    = hatDataStructure.RoaringBitmapArrayShrinkSize
	roaringBitmapContainerKindArray = "array"
	roaringBitmapContainerKindBits  = "bitmap"
)

// RoaringBitmapInfo reports the shape and memory footprint of an exact uint32 set.
type RoaringBitmapInfo = hatDataStructure.RoaringBitmapInfo

// These aliases preserve the cache snapshot schema while the implementation is public.
type roaringBitmapSnapshot = hatDataStructure.RoaringBitmapSnapshot
type roaringBitmapContainerSnapshot = hatDataStructure.RoaringBitmapContainerSnapshot

type roaringBitmapData struct {
	bitmap hatDataStructure.RoaringBitmap
}

func newRoaringBitmapData() roaringBitmapData {
	return roaringBitmapData{bitmap: hatDataStructure.NewRoaringBitmap()}
}

func validateRoaringBitmapSnapshot(snapshot roaringBitmapSnapshot) error {
	return hatDataStructure.ValidateRoaringBitmapSnapshot(snapshot)
}

func newRoaringBitmapDataFromSnapshot(snapshot roaringBitmapSnapshot) (roaringBitmapData, error) {
	bitmap, err := hatDataStructure.NewRoaringBitmapFromSnapshot(snapshot)
	return roaringBitmapData{bitmap: bitmap}, err
}

func (bitmap *roaringBitmapData) Add(value uint32) bool {
	return bitmap != nil && bitmap.bitmap.Add(value) == 1
}

func (bitmap *roaringBitmapData) AddOne(value uint32, values ...uint32) int {
	if bitmap == nil {
		return 0
	}
	return bitmap.bitmap.Add(value, values...)
}

func (bitmap *roaringBitmapData) Remove(value uint32) bool {
	return bitmap != nil && bitmap.bitmap.Remove(value) == 1
}

func (bitmap *roaringBitmapData) RemoveOne(value uint32, values ...uint32) int {
	if bitmap == nil {
		return 0
	}
	return bitmap.bitmap.Remove(value, values...)
}

func (bitmap roaringBitmapData) Contains(value uint32) bool { return bitmap.bitmap.Contains(value) }
func (bitmap roaringBitmapData) Count() uint64              { return bitmap.bitmap.Count() }
func (bitmap roaringBitmapData) Values() []uint32           { return bitmap.bitmap.Values() }
func (bitmap roaringBitmapData) Info() RoaringBitmapInfo    { return bitmap.bitmap.Info() }
func (bitmap roaringBitmapData) Snapshot() roaringBitmapSnapshot {
	return bitmap.bitmap.Snapshot()
}
func (bitmap roaringBitmapData) EncodedSize() int64 { return bitmap.bitmap.EncodedSize() }

// RoaringBitmapStorage stores bitmap values outside the trie.
type RoaringBitmapStorage struct {
	array     []roaringBitmapData
	reusables reusableIndexes
}

func CreateRoaringBitmapStorage() *RoaringBitmapStorage {
	return &RoaringBitmapStorage{array: []roaringBitmapData{}}
}

func (store *RoaringBitmapStorage) PutData(idx int32, value roaringBitmapData) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = value
	store.reusables.Use(idx)
}

func (store *RoaringBitmapStorage) AppendData(value roaringBitmapData) int32 {
	store.array = append(store.array, value)
	return int32(len(store.array) - 1)
}

func (store *RoaringBitmapStorage) AddData(value roaringBitmapData) int32 {
	if idx, ok := store.reusables.Take(); ok {
		store.array[idx] = value
		return idx
	}
	return store.AppendData(value)
}

func (store *RoaringBitmapStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = roaringBitmapData{}
	store.reusables.Mark(idx)
	store.array = trimReusableTail(store.array, &store.reusables)
}

func (ht *HatTrie) UpsertRoaringBitmap(key string) { _ = ht.UpsertRoaringBitmapChecked(key) }

func (ht *HatTrie) UpsertRoaringBitmapChecked(key string) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.UpsertRoaringBitmapChecked(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsRoaringBitmap() {
		ht.roaringBitmaps.PutData(hval.Index, newRoaringBitmapData())
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.roaringBitmaps.AddData(newRoaringBitmapData())
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_ROARING_BITMAP}.toValue()
	ht.recordWriteLocked(key)
	return nil
}

func (ht *HatTrie) AddRoaringBitmap(key string, value uint32, values ...uint32) int {
	added, _ := ht.AddRoaringBitmapChecked(key, value, values...)
	return added
}

func (ht *HatTrie) AddRoaringBitmapChecked(key string, value uint32, values ...uint32) (int, error) {
	if ht == nil {
		return 0, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.AddRoaringBitmapChecked(key, value, values...)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return 0, err
	}
	if hval.IsRoaringBitmap() {
		added := ht.roaringBitmaps.array[hval.Index].AddOne(value, values...)
		*rawPtr = hval.toValue()
		if added > 0 {
			ht.recordWriteLocked(key)
		}
		return added, nil
	}
	if rawPtr == nil {
		rawPtr = ht.upsertLocation(key)
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.roaringBitmaps.AddData(newRoaringBitmapData())
	added := ht.roaringBitmaps.array[idx].AddOne(value, values...)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_ROARING_BITMAP}.toValue()
	ht.recordWriteLocked(key)
	return added, nil
}

func (ht *HatTrie) RemoveRoaringBitmap(key string, value uint32, values ...uint32) int {
	removed, _ := ht.RemoveRoaringBitmapChecked(key, value, values...)
	return removed
}

func (ht *HatTrie) RemoveRoaringBitmapChecked(key string, value uint32, values ...uint32) (int, error) {
	if ht == nil {
		return 0, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.RemoveRoaringBitmapChecked(key, value, values...)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return 0, err
	}
	if !hval.IsRoaringBitmap() {
		ht.recordReadLocked(false, key)
		return 0, nil
	}
	removed := ht.roaringBitmaps.array[hval.Index].RemoveOne(value, values...)
	ht.recordReadLocked(removed > 0, key)
	if removed > 0 {
		ht.recordWriteLocked(key)
	}
	return removed, nil
}

func (ht *HatTrie) HasRoaringBitmap(key string, value uint32) bool {
	hit, _ := ht.HasRoaringBitmapChecked(key, value)
	return hit
}
func (ht *HatTrie) HasRoaringBitmapChecked(key string, value uint32) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.HasRoaringBitmapChecked(key, value)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return false, err
	}
	if !hval.IsRoaringBitmap() {
		ht.recordReadLocked(false, key)
		return false, nil
	}
	hit := ht.roaringBitmaps.array[hval.Index].Contains(value)
	ht.recordReadLocked(hit, key)
	return hit, nil
}

func (ht *HatTrie) CountRoaringBitmap(key string) (uint64, bool) {
	count, ok, _ := ht.CountRoaringBitmapChecked(key)
	return count, ok
}
func (ht *HatTrie) CountRoaringBitmapChecked(key string) (uint64, bool, error) {
	if ht == nil {
		return 0, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.CountRoaringBitmapChecked(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return 0, false, err
	}
	if !hval.IsRoaringBitmap() {
		ht.recordReadLocked(false, key)
		return 0, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.roaringBitmaps.array[hval.Index].Count(), true, nil
}

func (ht *HatTrie) GetRoaringBitmap(key string) []uint32 {
	values, _, _ := ht.GetRoaringBitmapChecked(key)
	return values
}
func (ht *HatTrie) GetRoaringBitmapChecked(key string) ([]uint32, bool, error) {
	if ht == nil {
		return nil, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.GetRoaringBitmapChecked(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, false, err
	}
	if !hval.IsRoaringBitmap() {
		ht.recordReadLocked(false, key)
		return nil, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.roaringBitmaps.array[hval.Index].Values(), true, nil
}

func (ht *HatTrie) RoaringBitmapInfo(key string) (RoaringBitmapInfo, bool) {
	info, ok, _ := ht.RoaringBitmapInfoChecked(key)
	return info, ok
}
func (ht *HatTrie) RoaringBitmapInfoChecked(key string) (RoaringBitmapInfo, bool, error) {
	if ht == nil {
		return RoaringBitmapInfo{}, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.RoaringBitmapInfoChecked(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return RoaringBitmapInfo{}, false, err
	}
	if !hval.IsRoaringBitmap() {
		ht.recordReadLocked(false, key)
		return RoaringBitmapInfo{}, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.roaringBitmaps.array[hval.Index].Info(), true, nil
}

func roaringBitmapValuesFromCommand(request CacheCommandRequest) ([]uint32, error) {
	values, ok := commandSliceValues(request)
	if !ok {
		return nil, errors.New("value or values is required")
	}
	return roaringBitmapValuesFromCommandSlice(values)
}

func roaringBitmapValuesFromCommandSlice(values Slice) ([]uint32, error) {
	parsed := make([]uint32, len(values))
	if err := parseRoaringBitmapCommandValues(parsed, values); err != nil {
		return nil, err
	}
	return parsed, nil
}

func parseRoaringBitmapCommandValues(parsed []uint32, values Slice) error {
	for index, value := range values {
		parsedValue, err := roaringBitmapValueFromCommand(value)
		if err != nil {
			return err
		}
		parsed[index] = parsedValue
	}
	return nil
}

func roaringBitmapValueFromCommand(value interface{}) (uint32, error) {
	parsed, err := commandUint64Value(value)
	if err != nil || parsed > uint64(^uint32(0)) {
		return 0, errors.New("roaring bitmap value must be an unsigned 32-bit integer")
	}
	return uint32(parsed), nil
}
