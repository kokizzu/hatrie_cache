package hatCache

import (
	"errors"

	"hatrie_cache/hat/hatDataStructure"
)

const (
	sparseBitsetBitmapWords        = hatDataStructure.SparseBitsetBitmapWords
	sparseBitsetArrayMaxSize       = hatDataStructure.SparseBitsetArrayMaxSize
	sparseBitsetArrayShrinkSize    = hatDataStructure.SparseBitsetArrayShrinkSize
	sparseBitsetMaxContainerCount  = hatDataStructure.SparseBitsetMaxContainerCount
	sparseBitsetMaxContainerKey    = hatDataStructure.SparseBitsetMaxContainerKey
	sparseBitsetContainerKindArray = "array"
	sparseBitsetContainerKindBits  = "bitmap"
)

type SparseBitsetInfo = hatDataStructure.SparseBitsetInfo
type sparseBitsetSnapshot = hatDataStructure.SparseBitsetSnapshot
type sparseBitsetContainerSnapshot = hatDataStructure.SparseBitsetContainerSnapshot
type sparseBitsetData struct{ bitset hatDataStructure.SparseBitset }

func newSparseBitsetData() sparseBitsetData {
	return sparseBitsetData{bitset: hatDataStructure.NewSparseBitset()}
}
func validateSparseBitsetSnapshot(snapshot sparseBitsetSnapshot) error {
	return hatDataStructure.ValidateSparseBitsetSnapshot(snapshot)
}
func newSparseBitsetDataFromSnapshot(snapshot sparseBitsetSnapshot) (sparseBitsetData, error) {
	bitset, err := hatDataStructure.NewSparseBitsetFromSnapshot(snapshot)
	return sparseBitsetData{bitset: bitset}, err
}
func (bitset *sparseBitsetData) Add(value uint64) bool {
	return bitset != nil && bitset.bitset.Add(value) == 1
}
func (bitset *sparseBitsetData) AddOne(value uint64, values ...uint64) int {
	if bitset == nil {
		return 0
	}
	return bitset.bitset.Add(value, values...)
}
func (bitset *sparseBitsetData) Remove(value uint64) bool {
	return bitset != nil && bitset.bitset.Remove(value) == 1
}
func (bitset *sparseBitsetData) RemoveOne(value uint64, values ...uint64) int {
	if bitset == nil {
		return 0
	}
	return bitset.bitset.Remove(value, values...)
}
func (bitset sparseBitsetData) Contains(value uint64) bool     { return bitset.bitset.Contains(value) }
func (bitset sparseBitsetData) Count() uint64                  { return bitset.bitset.Count() }
func (bitset sparseBitsetData) Values() []uint64               { return bitset.bitset.Values() }
func (bitset sparseBitsetData) Info() SparseBitsetInfo         { return bitset.bitset.Info() }
func (bitset sparseBitsetData) Snapshot() sparseBitsetSnapshot { return bitset.bitset.Snapshot() }
func (bitset sparseBitsetData) EncodedSize() int64             { return bitset.bitset.EncodedSize() }

// SparseBitsetStorage stores sparse bitset values outside the trie.
type SparseBitsetStorage struct {
	array     []sparseBitsetData
	reusables reusableIndexes
}

func CreateSparseBitsetStorage() *SparseBitsetStorage {
	return &SparseBitsetStorage{array: []sparseBitsetData{}}
}
func (store *SparseBitsetStorage) PutData(idx int32, value sparseBitsetData) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = value
	store.reusables.Use(idx)
}
func (store *SparseBitsetStorage) AppendData(value sparseBitsetData) int32 {
	store.array = append(store.array, value)
	return int32(len(store.array) - 1)
}
func (store *SparseBitsetStorage) AddData(value sparseBitsetData) int32 {
	if idx, ok := store.reusables.Take(); ok {
		store.array[idx] = value
		return idx
	}
	return store.AppendData(value)
}
func (store *SparseBitsetStorage) Del(idx int32) {
	if idx < 0 || int(idx) >= len(store.array) {
		return
	}
	store.array[idx] = sparseBitsetData{}
	store.reusables.Mark(idx)
	store.array = trimReusableTail(store.array, &store.reusables)
}

func (ht *HatTrie) UpsertSparseBitset(key string) { _ = ht.UpsertSparseBitsetChecked(key) }
func (ht *HatTrie) UpsertSparseBitsetChecked(key string) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.UpsertSparseBitsetChecked(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	rawPtr, hval, err := ht.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsSparseBitset() {
		ht.sparseBitsets.PutData(hval.Index, newSparseBitsetData())
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return nil
	}
	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.sparseBitsets.AddData(newSparseBitsetData())
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SPARSE_BITSET}.toValue()
	ht.recordWriteLocked(key)
	return nil
}
func (ht *HatTrie) AddSparseBitset(key string, value uint64, values ...uint64) int {
	added, _ := ht.AddSparseBitsetChecked(key, value, values...)
	return added
}
func (ht *HatTrie) AddSparseBitsetChecked(key string, value uint64, values ...uint64) (int, error) {
	if ht == nil {
		return 0, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.AddSparseBitsetChecked(key, value, values...)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return 0, err
	}
	if hval.IsSparseBitset() {
		added := ht.sparseBitsets.array[hval.Index].AddOne(value, values...)
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
	idx := ht.sparseBitsets.AddData(newSparseBitsetData())
	added := ht.sparseBitsets.array[idx].AddOne(value, values...)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SPARSE_BITSET}.toValue()
	ht.recordWriteLocked(key)
	return added, nil
}
func (ht *HatTrie) RemoveSparseBitset(key string, value uint64, values ...uint64) int {
	removed, _ := ht.RemoveSparseBitsetChecked(key, value, values...)
	return removed
}
func (ht *HatTrie) RemoveSparseBitsetChecked(key string, value uint64, values ...uint64) (int, error) {
	if ht == nil {
		return 0, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.RemoveSparseBitsetChecked(key, value, values...)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return 0, err
	}
	if !hval.IsSparseBitset() {
		ht.recordReadLocked(false, key)
		return 0, nil
	}
	removed := ht.sparseBitsets.array[hval.Index].RemoveOne(value, values...)
	ht.recordReadLocked(removed > 0, key)
	if removed > 0 {
		ht.recordWriteLocked(key)
	}
	return removed, nil
}
func (ht *HatTrie) HasSparseBitset(key string, value uint64) bool {
	hit, _ := ht.HasSparseBitsetChecked(key, value)
	return hit
}
func (ht *HatTrie) HasSparseBitsetChecked(key string, value uint64) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.HasSparseBitsetChecked(key, value)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return false, err
	}
	if !hval.IsSparseBitset() {
		ht.recordReadLocked(false, key)
		return false, nil
	}
	hit := ht.sparseBitsets.array[hval.Index].Contains(value)
	ht.recordReadLocked(hit, key)
	return hit, nil
}
func (ht *HatTrie) CountSparseBitset(key string) (uint64, bool) {
	count, ok, _ := ht.CountSparseBitsetChecked(key)
	return count, ok
}
func (ht *HatTrie) CountSparseBitsetChecked(key string) (uint64, bool, error) {
	if ht == nil {
		return 0, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.CountSparseBitsetChecked(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return 0, false, err
	}
	if !hval.IsSparseBitset() {
		ht.recordReadLocked(false, key)
		return 0, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.sparseBitsets.array[hval.Index].Count(), true, nil
}
func (ht *HatTrie) GetSparseBitset(key string) []uint64 {
	values, _, _ := ht.GetSparseBitsetChecked(key)
	return values
}
func (ht *HatTrie) GetSparseBitsetChecked(key string) ([]uint64, bool, error) {
	if ht == nil {
		return nil, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.GetSparseBitsetChecked(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return nil, false, err
	}
	if !hval.IsSparseBitset() {
		ht.recordReadLocked(false, key)
		return nil, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.sparseBitsets.array[hval.Index].Values(), true, nil
}
func (ht *HatTrie) SparseBitsetInfo(key string) (SparseBitsetInfo, bool) {
	info, ok, _ := ht.SparseBitsetInfoChecked(key)
	return info, ok
}
func (ht *HatTrie) SparseBitsetInfoChecked(key string) (SparseBitsetInfo, bool, error) {
	if ht == nil {
		return SparseBitsetInfo{}, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.SparseBitsetInfoChecked(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return SparseBitsetInfo{}, false, err
	}
	if !hval.IsSparseBitset() {
		ht.recordReadLocked(false, key)
		return SparseBitsetInfo{}, false, nil
	}
	ht.recordReadLocked(true, key)
	return ht.sparseBitsets.array[hval.Index].Info(), true, nil
}
func sparseBitsetValuesFromCommand(request CacheCommandRequest) ([]uint64, error) {
	values, ok := commandSliceValues(request)
	if !ok {
		return nil, errors.New("value or values is required")
	}
	out := make([]uint64, 0, len(values))
	for _, value := range values {
		parsed, err := commandUint64Value(value)
		if err != nil {
			return nil, errors.New("sparse bitset value must be an unsigned 64-bit integer")
		}
		out = append(out, parsed)
	}
	return out, nil
}
