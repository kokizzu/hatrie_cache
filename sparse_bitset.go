package hatriecache

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math/bits"
	"sort"
)

const (
	sparseBitsetContainerBits      = 16
	sparseBitsetContainerSize      = 1 << sparseBitsetContainerBits
	sparseBitsetBitmapWords        = sparseBitsetContainerSize / 64
	sparseBitsetArrayMaxSize       = 4096
	sparseBitsetArrayShrinkSize    = sparseBitsetArrayMaxSize / 2
	sparseBitsetMaxContainerCount  = 1 << 20
	sparseBitsetMaxContainerKey    = (uint64(1) << (64 - sparseBitsetContainerBits)) - 1
	sparseBitsetContainerKindArray = "array"
	sparseBitsetContainerKindBits  = "bitmap"
)

// SparseBitsetInfo reports the shape and memory footprint of an exact uint64
// set stored as sorted sparse containers plus packed bitsets for dense ranges.
type SparseBitsetInfo struct {
	Cardinality      uint64 `json:"cardinality"`
	Containers       uint64 `json:"containers"`
	ArrayContainers  uint64 `json:"array_containers"`
	BitmapContainers uint64 `json:"bitmap_containers"`
	EncodedBytes     uint64 `json:"encoded_bytes"`
}

type sparseBitsetSnapshot struct {
	Cardinality uint64                          `json:"cardinality"`
	Containers  []sparseBitsetContainerSnapshot `json:"containers"`
}

type sparseBitsetContainerSnapshot struct {
	Key         uint64 `json:"key"`
	Kind        string `json:"kind"`
	Cardinality uint32 `json:"cardinality"`
	Values      string `json:"values,omitempty"`
	Bits        string `json:"bits,omitempty"`
}

type sparseBitsetData struct {
	containers []sparseBitsetContainer
	count      uint64
}

type sparseBitsetContainer struct {
	key         uint64
	values      []uint16
	bits        []uint64
	cardinality uint32
}

func newSparseBitsetData() sparseBitsetData {
	return sparseBitsetData{}
}

func validateSparseBitsetSnapshot(snapshot sparseBitsetSnapshot) error {
	if len(snapshot.Containers) > sparseBitsetMaxContainerCount {
		return errors.New("hatriecache: sparse bitset has too many containers")
	}
	var total uint64
	var previous uint64
	for idx, container := range snapshot.Containers {
		if container.Key > sparseBitsetMaxContainerKey {
			return errors.New("hatriecache: sparse bitset container key is out of range")
		}
		if idx > 0 && container.Key <= previous {
			return errors.New("hatriecache: sparse bitset containers must be sorted")
		}
		previous = container.Key
		cardinality, err := validateSparseBitsetContainerSnapshot(container)
		if err != nil {
			return err
		}
		total += uint64(cardinality)
	}
	if total != snapshot.Cardinality {
		return errors.New("hatriecache: sparse bitset cardinality does not match containers")
	}
	return nil
}

func validateSparseBitsetContainerSnapshot(snapshot sparseBitsetContainerSnapshot) (uint32, error) {
	switch snapshot.Kind {
	case sparseBitsetContainerKindArray:
		raw, err := base64.StdEncoding.DecodeString(snapshot.Values)
		if err != nil {
			return 0, err
		}
		if len(raw)%2 != 0 {
			return 0, errors.New("hatriecache: invalid sparse bitset array payload")
		}
		if len(raw)/2 > sparseBitsetArrayMaxSize {
			return 0, errors.New("hatriecache: sparse bitset array container is too large")
		}
		if uint32(len(raw)/2) != snapshot.Cardinality {
			return 0, errors.New("hatriecache: sparse bitset array cardinality mismatch")
		}
		var previous uint16
		for idx := 0; idx < len(raw)/2; idx++ {
			value := binary.LittleEndian.Uint16(raw[idx*2 : idx*2+2])
			if idx > 0 && value <= previous {
				return 0, errors.New("hatriecache: sparse bitset array values must be sorted")
			}
			previous = value
		}
		return snapshot.Cardinality, nil
	case sparseBitsetContainerKindBits:
		raw, err := base64.StdEncoding.DecodeString(snapshot.Bits)
		if err != nil {
			return 0, err
		}
		if len(raw) != sparseBitsetBitmapWords*8 {
			return 0, errors.New("hatriecache: invalid sparse bitset bitset payload")
		}
		var cardinality uint32
		for idx := 0; idx < sparseBitsetBitmapWords; idx++ {
			cardinality += uint32(bits.OnesCount64(binary.LittleEndian.Uint64(raw[idx*8 : idx*8+8])))
		}
		if cardinality != snapshot.Cardinality {
			return 0, errors.New("hatriecache: sparse bitset bitset cardinality mismatch")
		}
		return cardinality, nil
	default:
		return 0, errors.New("hatriecache: unsupported sparse bitset container kind")
	}
}

func newSparseBitsetDataFromSnapshot(snapshot sparseBitsetSnapshot) (sparseBitsetData, error) {
	if err := validateSparseBitsetSnapshot(snapshot); err != nil {
		return sparseBitsetData{}, err
	}
	out := sparseBitsetData{
		containers: make([]sparseBitsetContainer, 0, len(snapshot.Containers)),
		count:      snapshot.Cardinality,
	}
	for _, rawContainer := range snapshot.Containers {
		container, err := newSparseBitsetContainerFromSnapshot(rawContainer)
		if err != nil {
			return sparseBitsetData{}, err
		}
		out.containers = append(out.containers, container)
	}
	return out, nil
}

func newSparseBitsetContainerFromSnapshot(snapshot sparseBitsetContainerSnapshot) (sparseBitsetContainer, error) {
	container := sparseBitsetContainer{
		key:         snapshot.Key,
		cardinality: snapshot.Cardinality,
	}
	switch snapshot.Kind {
	case sparseBitsetContainerKindArray:
		raw, err := base64.StdEncoding.DecodeString(snapshot.Values)
		if err != nil {
			return sparseBitsetContainer{}, err
		}
		container.values = make([]uint16, len(raw)/2)
		for idx := range container.values {
			container.values[idx] = binary.LittleEndian.Uint16(raw[idx*2 : idx*2+2])
		}
	case sparseBitsetContainerKindBits:
		raw, err := base64.StdEncoding.DecodeString(snapshot.Bits)
		if err != nil {
			return sparseBitsetContainer{}, err
		}
		container.bits = make([]uint64, sparseBitsetBitmapWords)
		for idx := range container.bits {
			container.bits[idx] = binary.LittleEndian.Uint64(raw[idx*8 : idx*8+8])
		}
	}
	return container, nil
}

func (bitset *sparseBitsetData) Add(value uint64) bool {
	if bitset == nil {
		return false
	}
	key, low := sparseBitsetSplit(value)
	idx, found := bitset.findContainer(key)
	if !found {
		container := sparseBitsetContainer{key: key}
		container.add(low)
		bitset.containers = insertSparseBitsetContainer(bitset.containers, idx, container)
		bitset.count++
		return true
	}
	if bitset.containers[idx].add(low) {
		bitset.count++
		return true
	}
	return false
}

func (bitset *sparseBitsetData) AddOne(value uint64, values ...uint64) int {
	added := 0
	if bitset.Add(value) {
		added++
	}
	for _, value := range values {
		if bitset.Add(value) {
			added++
		}
	}
	return added
}

func (bitset *sparseBitsetData) Remove(value uint64) bool {
	if bitset == nil {
		return false
	}
	key, low := sparseBitsetSplit(value)
	idx, found := bitset.findContainer(key)
	if !found {
		return false
	}
	if !bitset.containers[idx].remove(low) {
		return false
	}
	bitset.count--
	if bitset.containers[idx].empty() {
		bitset.containers[idx].clear()
		copy(bitset.containers[idx:], bitset.containers[idx+1:])
		bitset.containers[len(bitset.containers)-1] = sparseBitsetContainer{}
		bitset.containers = bitset.containers[:len(bitset.containers)-1]
		if cap(bitset.containers) > 16 && len(bitset.containers)*4 < cap(bitset.containers) {
			next := make([]sparseBitsetContainer, len(bitset.containers))
			copy(next, bitset.containers)
			bitset.containers = next
		}
	}
	return true
}

func (bitset *sparseBitsetData) RemoveOne(value uint64, values ...uint64) int {
	removed := 0
	if bitset.Remove(value) {
		removed++
	}
	for _, value := range values {
		if bitset.Remove(value) {
			removed++
		}
	}
	return removed
}

func (bitset sparseBitsetData) Contains(value uint64) bool {
	key, low := sparseBitsetSplit(value)
	idx, found := bitset.findContainer(key)
	return found && bitset.containers[idx].contains(low)
}

func (bitset sparseBitsetData) Count() uint64 {
	return bitset.count
}

func (bitset sparseBitsetData) Values() []uint64 {
	if bitset.count == 0 {
		return []uint64{}
	}
	out := make([]uint64, 0, int(bitset.count))
	for idx := range bitset.containers {
		out = bitset.containers[idx].appendValues(out)
	}
	return out
}

func (bitset sparseBitsetData) Info() SparseBitsetInfo {
	info := SparseBitsetInfo{
		Cardinality:  bitset.count,
		Containers:   uint64(len(bitset.containers)),
		EncodedBytes: uint64(bitset.EncodedSize()),
	}
	for idx := range bitset.containers {
		if bitset.containers[idx].isBitmap() {
			info.BitmapContainers++
		} else {
			info.ArrayContainers++
		}
	}
	return info
}

func (bitset sparseBitsetData) Snapshot() sparseBitsetSnapshot {
	containers := make([]sparseBitsetContainerSnapshot, len(bitset.containers))
	for idx := range bitset.containers {
		containers[idx] = bitset.containers[idx].Snapshot()
	}
	return sparseBitsetSnapshot{
		Cardinality: bitset.count,
		Containers:  containers,
	}
}

func (bitset sparseBitsetData) EncodedSize() int64 {
	var total int64
	for idx := range bitset.containers {
		total += bitset.containers[idx].EncodedSize()
	}
	return total
}

func (bitset sparseBitsetData) findContainer(key uint64) (int, bool) {
	idx := sort.Search(len(bitset.containers), func(idx int) bool {
		return bitset.containers[idx].key >= key
	})
	return idx, idx < len(bitset.containers) && bitset.containers[idx].key == key
}

func (container *sparseBitsetContainer) add(value uint16) bool {
	if container.isBitmap() {
		word, mask := sparseBitsetBit(value)
		if container.bits[word]&mask != 0 {
			return false
		}
		container.bits[word] |= mask
		container.cardinality++
		return true
	}
	idx := sort.Search(len(container.values), func(idx int) bool {
		return container.values[idx] >= value
	})
	if idx < len(container.values) && container.values[idx] == value {
		return false
	}
	container.values = append(container.values, 0)
	copy(container.values[idx+1:], container.values[idx:])
	container.values[idx] = value
	container.cardinality++
	if len(container.values) > sparseBitsetArrayMaxSize {
		container.convertToBitmap()
	}
	return true
}

func (container *sparseBitsetContainer) remove(value uint16) bool {
	if container.isBitmap() {
		word, mask := sparseBitsetBit(value)
		if container.bits[word]&mask == 0 {
			return false
		}
		container.bits[word] &^= mask
		container.cardinality--
		if container.cardinality <= sparseBitsetArrayShrinkSize {
			container.convertToArray()
		}
		return true
	}
	idx := sort.Search(len(container.values), func(idx int) bool {
		return container.values[idx] >= value
	})
	if idx >= len(container.values) || container.values[idx] != value {
		return false
	}
	copy(container.values[idx:], container.values[idx+1:])
	container.values[len(container.values)-1] = 0
	container.values = container.values[:len(container.values)-1]
	container.cardinality--
	if cap(container.values) > 16 && len(container.values)*4 < cap(container.values) {
		next := make([]uint16, len(container.values))
		copy(next, container.values)
		container.values = next
	}
	return true
}

func (container sparseBitsetContainer) contains(value uint16) bool {
	if container.isBitmap() {
		word, mask := sparseBitsetBit(value)
		return container.bits[word]&mask != 0
	}
	idx := sort.Search(len(container.values), func(idx int) bool {
		return container.values[idx] >= value
	})
	return idx < len(container.values) && container.values[idx] == value
}

func (container sparseBitsetContainer) appendValues(out []uint64) []uint64 {
	prefix := container.key << sparseBitsetContainerBits
	if container.isBitmap() {
		for wordIdx, word := range container.bits {
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				out = append(out, prefix|uint64(wordIdx*64+bit))
				word &^= uint64(1) << uint(bit)
			}
		}
		return out
	}
	for _, value := range container.values {
		out = append(out, prefix|uint64(value))
	}
	return out
}

func (container sparseBitsetContainer) Snapshot() sparseBitsetContainerSnapshot {
	snapshot := sparseBitsetContainerSnapshot{
		Key:         container.key,
		Cardinality: container.cardinality,
	}
	if container.isBitmap() {
		raw := make([]byte, len(container.bits)*8)
		for idx, word := range container.bits {
			binary.LittleEndian.PutUint64(raw[idx*8:idx*8+8], word)
		}
		snapshot.Kind = sparseBitsetContainerKindBits
		snapshot.Bits = base64.StdEncoding.EncodeToString(raw)
		return snapshot
	}
	raw := make([]byte, len(container.values)*2)
	for idx, value := range container.values {
		binary.LittleEndian.PutUint16(raw[idx*2:idx*2+2], value)
	}
	snapshot.Kind = sparseBitsetContainerKindArray
	snapshot.Values = base64.StdEncoding.EncodeToString(raw)
	return snapshot
}

func (container sparseBitsetContainer) EncodedSize() int64 {
	if container.isBitmap() {
		return sparseBitsetBitmapWords * 8
	}
	return int64(len(container.values) * 2)
}

func (container sparseBitsetContainer) empty() bool {
	return container.cardinality == 0
}

func (container sparseBitsetContainer) isBitmap() bool {
	return container.bits != nil
}

func (container *sparseBitsetContainer) convertToBitmap() {
	if container.isBitmap() {
		return
	}
	next := make([]uint64, sparseBitsetBitmapWords)
	for _, value := range container.values {
		word, mask := sparseBitsetBit(value)
		next[word] |= mask
	}
	for idx := range container.values {
		container.values[idx] = 0
	}
	container.values = nil
	container.bits = next
}

func (container *sparseBitsetContainer) convertToArray() {
	if !container.isBitmap() {
		return
	}
	values := make([]uint16, 0, container.cardinality)
	for wordIdx, word := range container.bits {
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			values = append(values, uint16(wordIdx*64+bit))
			word &^= uint64(1) << uint(bit)
		}
	}
	for idx := range container.bits {
		container.bits[idx] = 0
	}
	container.bits = nil
	container.values = values
}

func (container *sparseBitsetContainer) clear() {
	for idx := range container.values {
		container.values[idx] = 0
	}
	for idx := range container.bits {
		container.bits[idx] = 0
	}
	*container = sparseBitsetContainer{}
}

func sparseBitsetSplit(value uint64) (uint64, uint16) {
	return value >> sparseBitsetContainerBits, uint16(value)
}

func sparseBitsetBit(value uint16) (int, uint64) {
	return int(value / 64), uint64(1) << uint(value%64)
}

func insertSparseBitsetContainer(containers []sparseBitsetContainer, idx int, container sparseBitsetContainer) []sparseBitsetContainer {
	containers = append(containers, sparseBitsetContainer{})
	copy(containers[idx+1:], containers[idx:])
	containers[idx] = container
	return containers
}

// SparseBitsetStorage stores sparse bitset values outside the trie.
type SparseBitsetStorage struct {
	array     []sparseBitsetData
	reusables reusableIndexes
}

func CreateSparseBitsetStorage() *SparseBitsetStorage {
	return &SparseBitsetStorage{
		array: []sparseBitsetData{},
	}
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

func (ht *HatTrie) UpsertSparseBitset(key string) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsSparseBitset() {
		ht.sparseBitsets.PutData(hval.Index, newSparseBitsetData())
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.sparseBitsets.AddData(newSparseBitsetData())
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SPARSE_BITSET}.toValue()
	ht.recordWriteLocked(key)
}

func (ht *HatTrie) AddSparseBitset(key string, value uint64, values ...uint64) int {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertFreshLocation(key)
	if hval.IsLevelDBReference() {
		return 0
	}
	if hval.IsSparseBitset() {
		added := ht.sparseBitsets.array[hval.Index].AddOne(value, values...)
		*rawPtr = hval.toValue()
		if added > 0 {
			ht.recordWriteLocked(key)
		}
		return added
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.sparseBitsets.AddData(newSparseBitsetData())
	added := ht.sparseBitsets.array[idx].AddOne(value, values...)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SPARSE_BITSET}.toValue()
	ht.recordWriteLocked(key)
	return added
}

func (ht *HatTrie) RemoveSparseBitset(key string, value uint64, values ...uint64) int {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsSparseBitset() {
		ht.recordReadLocked(false, key)
		return 0
	}
	removed := ht.sparseBitsets.array[hval.Index].RemoveOne(value, values...)
	ht.recordReadLocked(removed > 0, key)
	if removed > 0 {
		ht.recordWriteLocked(key)
	}
	return removed
}

func (ht *HatTrie) HasSparseBitset(key string, value uint64) bool {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsSparseBitset() {
		ht.recordReadLocked(false, key)
		return false
	}
	hit := ht.sparseBitsets.array[hval.Index].Contains(value)
	ht.recordReadLocked(hit, key)
	return hit
}

func (ht *HatTrie) CountSparseBitset(key string) (uint64, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsSparseBitset() {
		ht.recordReadLocked(false, key)
		return 0, false
	}
	ht.recordReadLocked(true, key)
	return ht.sparseBitsets.array[hval.Index].Count(), true
}

func (ht *HatTrie) GetSparseBitset(key string) []uint64 {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsSparseBitset() {
		ht.recordReadLocked(false, key)
		return nil
	}
	ht.recordReadLocked(true, key)
	return ht.sparseBitsets.array[hval.Index].Values()
}

func (ht *HatTrie) SparseBitsetInfo(key string) (SparseBitsetInfo, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval := ht.getLocked(key)
	if !hval.IsSparseBitset() {
		ht.recordReadLocked(false, key)
		return SparseBitsetInfo{}, false
	}
	ht.recordReadLocked(true, key)
	return ht.sparseBitsets.array[hval.Index].Info(), true
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
