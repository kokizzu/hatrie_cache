package hatriecache

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math/bits"
	"sort"
)

const (
	roaringBitmapContainerBits      = 16
	roaringBitmapContainerSize      = 1 << roaringBitmapContainerBits
	roaringBitmapBitmapWords        = roaringBitmapContainerSize / 64
	roaringBitmapArrayMaxSize       = 4096
	roaringBitmapArrayShrinkSize    = roaringBitmapArrayMaxSize / 2
	roaringBitmapMaxContainerCount  = 1 << roaringBitmapContainerBits
	roaringBitmapContainerKindArray = "array"
	roaringBitmapContainerKindBits  = "bitmap"
)

// RoaringBitmapInfo reports the shape and memory footprint of an exact
// uint32 set stored as sparse arrays plus dense bitset containers.
type RoaringBitmapInfo struct {
	Cardinality      uint64 `json:"cardinality"`
	Containers       uint64 `json:"containers"`
	ArrayContainers  uint64 `json:"array_containers"`
	BitmapContainers uint64 `json:"bitmap_containers"`
	EncodedBytes     uint64 `json:"encoded_bytes"`
}

type roaringBitmapSnapshot struct {
	Cardinality uint64                           `json:"cardinality"`
	Containers  []roaringBitmapContainerSnapshot `json:"containers"`
}

type roaringBitmapContainerSnapshot struct {
	Key         uint16 `json:"key"`
	Kind        string `json:"kind"`
	Cardinality uint32 `json:"cardinality"`
	Values      string `json:"values,omitempty"`
	Bits        string `json:"bits,omitempty"`
}

type roaringBitmapData struct {
	containers []roaringBitmapContainer
	count      uint64
}

type roaringBitmapContainer struct {
	key         uint16
	values      []uint16
	bits        []uint64
	cardinality uint32
}

func newRoaringBitmapData() roaringBitmapData {
	return roaringBitmapData{}
}

func validateRoaringBitmapSnapshot(snapshot roaringBitmapSnapshot) error {
	if len(snapshot.Containers) > roaringBitmapMaxContainerCount {
		return errors.New("hatriecache: roaring bitmap has too many containers")
	}
	var total uint64
	var previous uint16
	for idx, container := range snapshot.Containers {
		if idx > 0 && container.Key <= previous {
			return errors.New("hatriecache: roaring bitmap containers must be sorted")
		}
		previous = container.Key
		cardinality, err := validateRoaringBitmapContainerSnapshot(container)
		if err != nil {
			return err
		}
		total += uint64(cardinality)
	}
	if total != snapshot.Cardinality {
		return errors.New("hatriecache: roaring bitmap cardinality does not match containers")
	}
	return nil
}

func validateRoaringBitmapContainerSnapshot(snapshot roaringBitmapContainerSnapshot) (uint32, error) {
	switch snapshot.Kind {
	case roaringBitmapContainerKindArray:
		raw, err := base64.StdEncoding.DecodeString(snapshot.Values)
		if err != nil {
			return 0, err
		}
		if len(raw)%2 != 0 {
			return 0, errors.New("hatriecache: invalid roaring bitmap array payload")
		}
		if len(raw)/2 > roaringBitmapArrayMaxSize {
			return 0, errors.New("hatriecache: roaring bitmap array container is too large")
		}
		if uint32(len(raw)/2) != snapshot.Cardinality {
			return 0, errors.New("hatriecache: roaring bitmap array cardinality mismatch")
		}
		var previous uint16
		for idx := 0; idx < len(raw)/2; idx++ {
			value := binary.LittleEndian.Uint16(raw[idx*2 : idx*2+2])
			if idx > 0 && value <= previous {
				return 0, errors.New("hatriecache: roaring bitmap array values must be sorted")
			}
			previous = value
		}
		return snapshot.Cardinality, nil
	case roaringBitmapContainerKindBits:
		raw, err := base64.StdEncoding.DecodeString(snapshot.Bits)
		if err != nil {
			return 0, err
		}
		if len(raw) != roaringBitmapBitmapWords*8 {
			return 0, errors.New("hatriecache: invalid roaring bitmap bitset payload")
		}
		var cardinality uint32
		for idx := 0; idx < roaringBitmapBitmapWords; idx++ {
			cardinality += uint32(bits.OnesCount64(binary.LittleEndian.Uint64(raw[idx*8 : idx*8+8])))
		}
		if cardinality != snapshot.Cardinality {
			return 0, errors.New("hatriecache: roaring bitmap bitset cardinality mismatch")
		}
		return cardinality, nil
	default:
		return 0, errors.New("hatriecache: unsupported roaring bitmap container kind")
	}
}

func newRoaringBitmapDataFromSnapshot(snapshot roaringBitmapSnapshot) (roaringBitmapData, error) {
	if err := validateRoaringBitmapSnapshot(snapshot); err != nil {
		return roaringBitmapData{}, err
	}
	out := roaringBitmapData{
		containers: make([]roaringBitmapContainer, 0, len(snapshot.Containers)),
		count:      snapshot.Cardinality,
	}
	for _, rawContainer := range snapshot.Containers {
		container, err := newRoaringBitmapContainerFromSnapshot(rawContainer)
		if err != nil {
			return roaringBitmapData{}, err
		}
		out.containers = append(out.containers, container)
	}
	return out, nil
}

func newRoaringBitmapContainerFromSnapshot(snapshot roaringBitmapContainerSnapshot) (roaringBitmapContainer, error) {
	container := roaringBitmapContainer{
		key:         snapshot.Key,
		cardinality: snapshot.Cardinality,
	}
	switch snapshot.Kind {
	case roaringBitmapContainerKindArray:
		raw, err := base64.StdEncoding.DecodeString(snapshot.Values)
		if err != nil {
			return roaringBitmapContainer{}, err
		}
		container.values = make([]uint16, len(raw)/2)
		for idx := range container.values {
			container.values[idx] = binary.LittleEndian.Uint16(raw[idx*2 : idx*2+2])
		}
	case roaringBitmapContainerKindBits:
		raw, err := base64.StdEncoding.DecodeString(snapshot.Bits)
		if err != nil {
			return roaringBitmapContainer{}, err
		}
		container.bits = make([]uint64, roaringBitmapBitmapWords)
		for idx := range container.bits {
			container.bits[idx] = binary.LittleEndian.Uint64(raw[idx*8 : idx*8+8])
		}
	}
	return container, nil
}

func (bitmap *roaringBitmapData) Add(value uint32) bool {
	if bitmap == nil {
		return false
	}
	key, low := roaringBitmapSplit(value)
	idx, found := bitmap.findContainer(key)
	if !found {
		container := roaringBitmapContainer{key: key}
		container.add(low)
		bitmap.containers = insertRoaringContainer(bitmap.containers, idx, container)
		bitmap.count++
		return true
	}
	if bitmap.containers[idx].add(low) {
		bitmap.count++
		return true
	}
	return false
}

func (bitmap *roaringBitmapData) AddOne(value uint32, values ...uint32) int {
	added := 0
	if bitmap.Add(value) {
		added++
	}
	for _, value := range values {
		if bitmap.Add(value) {
			added++
		}
	}
	return added
}

func (bitmap *roaringBitmapData) Remove(value uint32) bool {
	if bitmap == nil {
		return false
	}
	key, low := roaringBitmapSplit(value)
	idx, found := bitmap.findContainer(key)
	if !found {
		return false
	}
	if !bitmap.containers[idx].remove(low) {
		return false
	}
	bitmap.count--
	if bitmap.containers[idx].empty() {
		bitmap.containers[idx].clear()
		copy(bitmap.containers[idx:], bitmap.containers[idx+1:])
		bitmap.containers[len(bitmap.containers)-1] = roaringBitmapContainer{}
		bitmap.containers = bitmap.containers[:len(bitmap.containers)-1]
		if cap(bitmap.containers) > 16 && len(bitmap.containers)*4 < cap(bitmap.containers) {
			next := make([]roaringBitmapContainer, len(bitmap.containers))
			copy(next, bitmap.containers)
			bitmap.containers = next
		}
	}
	return true
}

func (bitmap *roaringBitmapData) RemoveOne(value uint32, values ...uint32) int {
	removed := 0
	if bitmap.Remove(value) {
		removed++
	}
	for _, value := range values {
		if bitmap.Remove(value) {
			removed++
		}
	}
	return removed
}

func (bitmap roaringBitmapData) Contains(value uint32) bool {
	key, low := roaringBitmapSplit(value)
	idx, found := bitmap.findContainer(key)
	return found && bitmap.containers[idx].contains(low)
}

func (bitmap roaringBitmapData) Count() uint64 {
	return bitmap.count
}

func (bitmap roaringBitmapData) Values() []uint32 {
	if bitmap.count == 0 {
		return []uint32{}
	}
	out := make([]uint32, 0, int(bitmap.count))
	for idx := range bitmap.containers {
		out = bitmap.containers[idx].appendValues(out)
	}
	return out
}

func (bitmap roaringBitmapData) Info() RoaringBitmapInfo {
	info := RoaringBitmapInfo{
		Cardinality:  bitmap.count,
		Containers:   uint64(len(bitmap.containers)),
		EncodedBytes: uint64(bitmap.EncodedSize()),
	}
	for idx := range bitmap.containers {
		if bitmap.containers[idx].isBitmap() {
			info.BitmapContainers++
		} else {
			info.ArrayContainers++
		}
	}
	return info
}

func (bitmap roaringBitmapData) Snapshot() roaringBitmapSnapshot {
	containers := make([]roaringBitmapContainerSnapshot, len(bitmap.containers))
	for idx := range bitmap.containers {
		containers[idx] = bitmap.containers[idx].Snapshot()
	}
	return roaringBitmapSnapshot{
		Cardinality: bitmap.count,
		Containers:  containers,
	}
}

func (bitmap roaringBitmapData) EncodedSize() int64 {
	var total int64
	for idx := range bitmap.containers {
		total += bitmap.containers[idx].EncodedSize()
	}
	return total
}

func (bitmap roaringBitmapData) findContainer(key uint16) (int, bool) {
	idx := sort.Search(len(bitmap.containers), func(idx int) bool {
		return bitmap.containers[idx].key >= key
	})
	return idx, idx < len(bitmap.containers) && bitmap.containers[idx].key == key
}

func (container *roaringBitmapContainer) add(value uint16) bool {
	if container.isBitmap() {
		word, mask := roaringBitmapBit(value)
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
	if len(container.values) > roaringBitmapArrayMaxSize {
		container.convertToBitmap()
	}
	return true
}

func (container *roaringBitmapContainer) remove(value uint16) bool {
	if container.isBitmap() {
		word, mask := roaringBitmapBit(value)
		if container.bits[word]&mask == 0 {
			return false
		}
		container.bits[word] &^= mask
		container.cardinality--
		if container.cardinality <= roaringBitmapArrayShrinkSize {
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

func (container roaringBitmapContainer) contains(value uint16) bool {
	if container.isBitmap() {
		word, mask := roaringBitmapBit(value)
		return container.bits[word]&mask != 0
	}
	idx := sort.Search(len(container.values), func(idx int) bool {
		return container.values[idx] >= value
	})
	return idx < len(container.values) && container.values[idx] == value
}

func (container roaringBitmapContainer) appendValues(out []uint32) []uint32 {
	prefix := uint32(container.key) << roaringBitmapContainerBits
	if container.isBitmap() {
		for wordIdx, word := range container.bits {
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				out = append(out, prefix|uint32(wordIdx*64+bit))
				word &^= uint64(1) << uint(bit)
			}
		}
		return out
	}
	for _, value := range container.values {
		out = append(out, prefix|uint32(value))
	}
	return out
}

func (container roaringBitmapContainer) Snapshot() roaringBitmapContainerSnapshot {
	snapshot := roaringBitmapContainerSnapshot{
		Key:         container.key,
		Cardinality: container.cardinality,
	}
	if container.isBitmap() {
		raw := make([]byte, len(container.bits)*8)
		for idx, word := range container.bits {
			binary.LittleEndian.PutUint64(raw[idx*8:idx*8+8], word)
		}
		snapshot.Kind = roaringBitmapContainerKindBits
		snapshot.Bits = base64.StdEncoding.EncodeToString(raw)
		return snapshot
	}
	raw := make([]byte, len(container.values)*2)
	for idx, value := range container.values {
		binary.LittleEndian.PutUint16(raw[idx*2:idx*2+2], value)
	}
	snapshot.Kind = roaringBitmapContainerKindArray
	snapshot.Values = base64.StdEncoding.EncodeToString(raw)
	return snapshot
}

func (container roaringBitmapContainer) EncodedSize() int64 {
	if container.isBitmap() {
		return roaringBitmapBitmapWords * 8
	}
	return int64(len(container.values) * 2)
}

func (container roaringBitmapContainer) empty() bool {
	return container.cardinality == 0
}

func (container roaringBitmapContainer) isBitmap() bool {
	return container.bits != nil
}

func (container *roaringBitmapContainer) convertToBitmap() {
	if container.isBitmap() {
		return
	}
	next := make([]uint64, roaringBitmapBitmapWords)
	for _, value := range container.values {
		word, mask := roaringBitmapBit(value)
		next[word] |= mask
	}
	for idx := range container.values {
		container.values[idx] = 0
	}
	container.values = nil
	container.bits = next
}

func (container *roaringBitmapContainer) convertToArray() {
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

func (container *roaringBitmapContainer) clear() {
	for idx := range container.values {
		container.values[idx] = 0
	}
	for idx := range container.bits {
		container.bits[idx] = 0
	}
	*container = roaringBitmapContainer{}
}

func roaringBitmapSplit(value uint32) (uint16, uint16) {
	return uint16(value >> roaringBitmapContainerBits), uint16(value)
}

func roaringBitmapBit(value uint16) (int, uint64) {
	return int(value / 64), uint64(1) << uint(value%64)
}

func insertRoaringContainer(containers []roaringBitmapContainer, idx int, container roaringBitmapContainer) []roaringBitmapContainer {
	containers = append(containers, roaringBitmapContainer{})
	copy(containers[idx+1:], containers[idx:])
	containers[idx] = container
	return containers
}

// RoaringBitmapStorage stores Roaring bitmap values outside the trie.
type RoaringBitmapStorage struct {
	array     []roaringBitmapData
	reusables reusableIndexes
}

func CreateRoaringBitmapStorage() *RoaringBitmapStorage {
	return &RoaringBitmapStorage{
		array: []roaringBitmapData{},
	}
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

func (ht *HatTrie) UpsertRoaringBitmap(key string) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	rawPtr, hval := ht.upsertReplacementLocation(key)
	if hval.IsRoaringBitmap() {
		ht.roaringBitmaps.PutData(hval.Index, newRoaringBitmapData())
		ht.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		ht.recordWriteLocked(key)
		return
	}

	ht.returnStorage(hval)
	ht.clearExpirationLocked(key)
	idx := ht.roaringBitmaps.AddData(newRoaringBitmapData())
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_ROARING_BITMAP}.toValue()
	ht.recordWriteLocked(key)
}

func (ht *HatTrie) AddRoaringBitmap(key string, value uint32, values ...uint32) int {
	added, _ := ht.AddRoaringBitmapChecked(key, value, values...)
	return added
}

func (ht *HatTrie) AddRoaringBitmapChecked(key string, value uint32, values ...uint32) (int, error) {
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
	out := make([]uint32, 0, len(values))
	for _, value := range values {
		parsed, err := roaringBitmapValueFromCommand(value)
		if err != nil {
			return nil, err
		}
		out = append(out, parsed)
	}
	return out, nil
}

func roaringBitmapValueFromCommand(value interface{}) (uint32, error) {
	parsed, err := commandUint64Value(value)
	if err != nil {
		return 0, errors.New("roaring bitmap value must be an unsigned 32-bit integer")
	}
	if parsed > uint64(^uint32(0)) {
		return 0, errors.New("roaring bitmap value must be an unsigned 32-bit integer")
	}
	return uint32(parsed), nil
}
