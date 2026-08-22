package hatDataStructure

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math/bits"
	"sort"
)

const (
	SparseBitsetBitmapWords       = 1 << 10
	SparseBitsetArrayMaxSize      = 4096
	SparseBitsetArrayShrinkSize   = SparseBitsetArrayMaxSize / 2
	SparseBitsetMaxContainerCount = 1 << 20
	SparseBitsetMaxContainerKey   = (uint64(1) << 48) - 1

	sparseBitsetContainerBits      = 16
	sparseBitsetBitmapWords        = SparseBitsetBitmapWords
	sparseBitsetArrayMaxSize       = SparseBitsetArrayMaxSize
	sparseBitsetArrayShrinkSize    = SparseBitsetArrayShrinkSize
	sparseBitsetMaxContainerCount  = SparseBitsetMaxContainerCount
	sparseBitsetMaxContainerKey    = SparseBitsetMaxContainerKey
	sparseBitsetContainerKindArray = "array"
	sparseBitsetContainerKindBits  = "bitmap"
)

// SparseBitsetInfo reports the representation and raw payload size of a uint64 set.
type SparseBitsetInfo struct {
	Cardinality      uint64 `json:"cardinality"`
	Containers       uint64 `json:"containers"`
	ArrayContainers  uint64 `json:"array_containers"`
	BitmapContainers uint64 `json:"bitmap_containers"`
	EncodedBytes     uint64 `json:"encoded_bytes"`
}
type SparseBitsetContainerSnapshot struct {
	Key         uint64 `json:"key"`
	Kind        string `json:"kind"`
	Cardinality uint32 `json:"cardinality"`
	Values      string `json:"values,omitempty"`
	Bits        string `json:"bits,omitempty"`
}
type SparseBitsetSnapshot struct {
	Cardinality uint64                          `json:"cardinality"`
	Containers  []SparseBitsetContainerSnapshot `json:"containers"`
}

// SparseBitset stores an exact uint64 set with inline sparse containers and dense bitsets.
type SparseBitset struct {
	containers []sparseBitsetContainer
	count      uint64
}
type sparseBitsetData = SparseBitset
type sparseBitsetContainer struct {
	key         uint64
	values      []uint16
	bits        *[sparseBitsetBitmapWords]uint64
	cardinality uint32
	inline      [2]uint16
}

func NewSparseBitset() SparseBitset         { return SparseBitset{} }
func newSparseBitsetData() sparseBitsetData { return NewSparseBitset() }

func ValidateSparseBitsetSnapshot(snapshot SparseBitsetSnapshot) error {
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

func validateSparseBitsetContainerSnapshot(snapshot SparseBitsetContainerSnapshot) (uint32, error) {
	switch snapshot.Kind {
	case sparseBitsetContainerKindArray:
		size, ok := sparseBase64DecodedSize(snapshot.Values)
		if !ok {
			return 0, errors.New("hatriecache: invalid base64 encoding")
		}
		if size%2 != 0 {
			return 0, errors.New("hatriecache: invalid sparse bitset array payload")
		}
		if size/2 > sparseBitsetArrayMaxSize {
			return 0, errors.New("hatriecache: sparse bitset array container is too large")
		}
		if uint32(size/2) != snapshot.Cardinality {
			return 0, errors.New("hatriecache: sparse bitset array cardinality mismatch")
		}
		raw, err := base64.StdEncoding.DecodeString(snapshot.Values)
		if err != nil {
			return 0, err
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
		size, ok := sparseBase64DecodedSize(snapshot.Bits)
		if !ok {
			return 0, errors.New("hatriecache: invalid base64 encoding")
		}
		if size != sparseBitsetBitmapWords*8 {
			return 0, errors.New("hatriecache: invalid sparse bitset bitset payload")
		}
		raw, err := base64.StdEncoding.DecodeString(snapshot.Bits)
		if err != nil {
			return 0, err
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

func NewSparseBitsetFromSnapshot(snapshot SparseBitsetSnapshot) (SparseBitset, error) {
	if err := ValidateSparseBitsetSnapshot(snapshot); err != nil {
		return SparseBitset{}, err
	}
	out := SparseBitset{containers: make([]sparseBitsetContainer, 0, len(snapshot.Containers)), count: snapshot.Cardinality}
	for _, raw := range snapshot.Containers {
		container, err := newSparseBitsetContainerFromSnapshot(raw)
		if err != nil {
			return SparseBitset{}, err
		}
		out.containers = append(out.containers, container)
	}
	return out, nil
}

func newSparseBitsetContainerFromSnapshot(snapshot SparseBitsetContainerSnapshot) (sparseBitsetContainer, error) {
	container := sparseBitsetContainer{key: snapshot.Key, cardinality: snapshot.Cardinality}
	switch snapshot.Kind {
	case sparseBitsetContainerKindArray:
		raw, err := base64.StdEncoding.DecodeString(snapshot.Values)
		if err != nil {
			return sparseBitsetContainer{}, err
		}
		count := len(raw) / 2
		if count <= len(container.inline) {
			for idx := 0; idx < count; idx++ {
				container.inline[idx] = binary.LittleEndian.Uint16(raw[idx*2 : idx*2+2])
			}
		} else {
			container.values = make([]uint16, count)
			for idx := range container.values {
				container.values[idx] = binary.LittleEndian.Uint16(raw[idx*2 : idx*2+2])
			}
		}
	case sparseBitsetContainerKindBits:
		raw, err := base64.StdEncoding.DecodeString(snapshot.Bits)
		if err != nil {
			return sparseBitsetContainer{}, err
		}
		container.bits = new([sparseBitsetBitmapWords]uint64)
		for idx := range container.bits {
			container.bits[idx] = binary.LittleEndian.Uint64(raw[idx*8 : idx*8+8])
		}
	}
	return container, nil
}

// Add inserts values and returns the number that were not already present.
func (bitset *SparseBitset) Add(value uint64, values ...uint64) int {
	if bitset == nil {
		return 0
	}
	added := 0
	if bitset.add(value) {
		added++
	}
	for _, value := range values {
		if bitset.add(value) {
			added++
		}
	}
	return added
}
func (bitset *SparseBitset) add(value uint64) bool {
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

// Remove deletes values and returns the number that were present.
func (bitset *SparseBitset) Remove(value uint64, values ...uint64) int {
	if bitset == nil {
		return 0
	}
	removed := 0
	if bitset.remove(value) {
		removed++
	}
	for _, value := range values {
		if bitset.remove(value) {
			removed++
		}
	}
	return removed
}
func (bitset *SparseBitset) remove(value uint64) bool {
	key, low := sparseBitsetSplit(value)
	idx, found := bitset.findContainer(key)
	if !found || !bitset.containers[idx].remove(low) {
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
func (bitset SparseBitset) Contains(value uint64) bool {
	key, low := sparseBitsetSplit(value)
	idx, found := bitset.findContainer(key)
	return found && bitset.containers[idx].contains(low)
}
func (bitset SparseBitset) Count() uint64 { return bitset.count }
func (bitset SparseBitset) Values() []uint64 {
	if bitset.count == 0 {
		return []uint64{}
	}
	out := make([]uint64, 0, int(bitset.count))
	for idx := range bitset.containers {
		out = bitset.containers[idx].appendValues(out)
	}
	return out
}
func (bitset SparseBitset) Info() SparseBitsetInfo {
	info := SparseBitsetInfo{Cardinality: bitset.count, Containers: uint64(len(bitset.containers)), EncodedBytes: uint64(bitset.EncodedSize())}
	for idx := range bitset.containers {
		if bitset.containers[idx].isBitmap() {
			info.BitmapContainers++
		} else {
			info.ArrayContainers++
		}
	}
	return info
}
func (bitset SparseBitset) Snapshot() SparseBitsetSnapshot {
	containers := make([]SparseBitsetContainerSnapshot, len(bitset.containers))
	for idx := range bitset.containers {
		containers[idx] = bitset.containers[idx].snapshot()
	}
	return SparseBitsetSnapshot{Cardinality: bitset.count, Containers: containers}
}
func (bitset SparseBitset) EncodedSize() int64 {
	var total int64
	for idx := range bitset.containers {
		total += bitset.containers[idx].encodedSize()
	}
	return total
}
func (bitset SparseBitset) ContainerCount() int { return len(bitset.containers) }

// VisitContainers exposes payloads without copying. The callback must not retain or mutate its slices.
func (bitset SparseBitset) VisitContainers(visit func(key uint64, cardinality uint32, values []uint16, bitset []uint64) bool) {
	if visit == nil {
		return
	}
	for idx := range bitset.containers {
		container := &bitset.containers[idx]
		if container.isBitmap() {
			if !visit(container.key, container.cardinality, nil, container.bits[:]) {
				return
			}
		} else {
			values := container.values
			if values == nil {
				values = container.inline[:container.cardinality]
			}
			if !visit(container.key, container.cardinality, values, nil) {
				return
			}
		}
	}
}

func (bitset SparseBitset) findContainer(key uint64) (int, bool) {
	idx := sort.Search(len(bitset.containers), func(idx int) bool { return bitset.containers[idx].key >= key })
	return idx, idx < len(bitset.containers) && bitset.containers[idx].key == key
}
func (container *sparseBitsetContainer) add(value uint16) bool {
	if backing := container.bits; backing != nil {
		bitmap := backing[:]
		word, mask := sparseBitsetBit(value)
		if bitmap[word]&mask != 0 {
			return false
		}
		bitmap[word] |= mask
		container.cardinality++
		return true
	}
	if container.values == nil {
		switch container.cardinality {
		case 0:
			container.inline[0] = value
			container.cardinality = 1
			return true
		case 1:
			if container.inline[0] == value {
				return false
			}
			if value < container.inline[0] {
				container.inline[1] = container.inline[0]
				container.inline[0] = value
			} else {
				container.inline[1] = value
			}
			container.cardinality = 2
			return true
		case 2:
			if container.inline[0] == value || container.inline[1] == value {
				return false
			}
			container.values = make([]uint16, 3)
			switch {
			case value < container.inline[0]:
				container.values[0] = value
				copy(container.values[1:], container.inline[:])
			case value < container.inline[1]:
				container.values[0] = container.inline[0]
				container.values[1] = value
				container.values[2] = container.inline[1]
			default:
				copy(container.values, container.inline[:])
				container.values[2] = value
			}
			container.inline = [2]uint16{}
			container.cardinality = 3
			return true
		}
	}
	idx := sparseBitsetSearch(container.values, value)
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
	if backing := container.bits; backing != nil {
		bitmap := backing[:]
		word, mask := sparseBitsetBit(value)
		if bitmap[word]&mask == 0 {
			return false
		}
		bitmap[word] &^= mask
		container.cardinality--
		if container.cardinality <= sparseBitsetArrayShrinkSize {
			container.convertToArray()
		}
		return true
	}
	if container.values == nil {
		switch container.cardinality {
		case 0:
			return false
		case 1:
			if container.inline[0] != value {
				return false
			}
			container.inline[0] = 0
			container.cardinality = 0
			return true
		case 2:
			switch value {
			case container.inline[0]:
				container.inline[0] = container.inline[1]
			case container.inline[1]:
			default:
				return false
			}
			container.inline[1] = 0
			container.cardinality = 1
			return true
		}
	}
	idx := sparseBitsetSearch(container.values, value)
	if idx >= len(container.values) || container.values[idx] != value {
		return false
	}
	copy(container.values[idx:], container.values[idx+1:])
	container.values[len(container.values)-1] = 0
	container.values = container.values[:len(container.values)-1]
	container.cardinality--
	if len(container.values) <= len(container.inline) {
		copy(container.inline[:], container.values)
		for idx := range container.values {
			container.values[idx] = 0
		}
		container.values = nil
		return true
	}
	if cap(container.values) > 16 && len(container.values)*4 < cap(container.values) {
		next := make([]uint16, len(container.values))
		copy(next, container.values)
		container.values = next
	}
	return true
}
func (container sparseBitsetContainer) contains(value uint16) bool {
	if backing := container.bits; backing != nil {
		bitmap := backing[:]
		word, mask := sparseBitsetBit(value)
		return bitmap[word]&mask != 0
	}
	if container.values == nil {
		switch container.cardinality {
		case 0:
			return false
		case 1:
			return container.inline[0] == value
		default:
			return container.inline[0] == value || container.inline[1] == value
		}
	}
	idx := sparseBitsetSearch(container.values, value)
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
	if container.values == nil {
		for idx := 0; idx < int(container.cardinality); idx++ {
			out = append(out, prefix|uint64(container.inline[idx]))
		}
		return out
	}
	for _, value := range container.values {
		out = append(out, prefix|uint64(value))
	}
	return out
}
func (container sparseBitsetContainer) snapshot() SparseBitsetContainerSnapshot {
	snapshot := SparseBitsetContainerSnapshot{Key: container.key, Cardinality: container.cardinality}
	if container.isBitmap() {
		raw := make([]byte, len(container.bits)*8)
		for idx, word := range container.bits {
			binary.LittleEndian.PutUint64(raw[idx*8:idx*8+8], word)
		}
		snapshot.Kind = sparseBitsetContainerKindBits
		snapshot.Bits = base64.StdEncoding.EncodeToString(raw)
		return snapshot
	}
	count := len(container.values)
	if container.values == nil {
		count = int(container.cardinality)
	}
	raw := make([]byte, count*2)
	for idx := 0; idx < count; idx++ {
		value := uint16(0)
		if container.values == nil {
			value = container.inline[idx]
		} else {
			value = container.values[idx]
		}
		binary.LittleEndian.PutUint16(raw[idx*2:idx*2+2], value)
	}
	snapshot.Kind = sparseBitsetContainerKindArray
	snapshot.Values = base64.StdEncoding.EncodeToString(raw)
	return snapshot
}

func (container sparseBitsetContainer) Snapshot() SparseBitsetContainerSnapshot {
	return container.snapshot()
}
func (container sparseBitsetContainer) encodedSize() int64 {
	if container.isBitmap() {
		return sparseBitsetBitmapWords * 8
	}
	if container.values == nil {
		return int64(container.cardinality) * 2
	}
	return int64(len(container.values) * 2)
}

func (container sparseBitsetContainer) EncodedSize() int64 { return container.encodedSize() }
func (container sparseBitsetContainer) empty() bool        { return container.cardinality == 0 }
func (container sparseBitsetContainer) isBitmap() bool     { return container.bits != nil }
func (container *sparseBitsetContainer) convertToBitmap() {
	if container.isBitmap() {
		return
	}
	backing := new([sparseBitsetBitmapWords]uint64)
	next := backing[:]
	if container.values == nil {
		for idx := 0; idx < int(container.cardinality); idx++ {
			word, mask := sparseBitsetBit(container.inline[idx])
			next[word] |= mask
		}
	} else {
		for _, value := range container.values {
			word, mask := sparseBitsetBit(value)
			next[word] |= mask
		}
	}
	for idx := range container.values {
		container.values[idx] = 0
	}
	container.values = nil
	container.inline = [2]uint16{}
	container.bits = backing
}
func (container *sparseBitsetContainer) convertToArray() {
	if !container.isBitmap() {
		return
	}
	if container.cardinality <= uint32(len(container.inline)) {
		idx := 0
		for wordIdx, word := range container.bits {
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				container.inline[idx] = uint16(wordIdx*64 + bit)
				idx++
				word &^= uint64(1) << uint(bit)
			}
		}
		for idx := range container.bits {
			container.bits[idx] = 0
		}
		container.bits = nil
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
	if container.bits != nil {
		*container.bits = [sparseBitsetBitmapWords]uint64{}
	}
	*container = sparseBitsetContainer{}
}
func sparseBitsetSplit(value uint64) (uint64, uint16) {
	return value >> sparseBitsetContainerBits, uint16(value)
}
func sparseBitsetBit(value uint16) (int, uint64) { return int(value / 64), uint64(1) << uint(value%64) }
func sparseBitsetSearch(values []uint16, value uint16) int {
	low, high := 0, len(values)
	for low < high {
		mid := int(uint(low+high) >> 1)
		if values[mid] < value {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}
func insertSparseBitsetContainer(containers []sparseBitsetContainer, idx int, container sparseBitsetContainer) []sparseBitsetContainer {
	containers = append(containers, sparseBitsetContainer{})
	copy(containers[idx+1:], containers[idx:])
	containers[idx] = container
	return containers
}
func sparseBase64DecodedSize(encoded string) (int, bool) {
	if len(encoded)%4 != 0 {
		return 0, false
	}
	padding := 0
	if len(encoded) >= 2 && encoded[len(encoded)-2:] == "==" {
		padding = 2
	} else if len(encoded) >= 1 && encoded[len(encoded)-1:] == "=" {
		padding = 1
	}
	return len(encoded)/4*3 - padding, true
}
