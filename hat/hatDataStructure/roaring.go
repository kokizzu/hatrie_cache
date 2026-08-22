package hatDataStructure

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math/bits"
	"sort"
)

const (
	// RoaringBitmapBitmapWords is the number of 64-bit words in a dense container.
	RoaringBitmapBitmapWords = 1 << 10
	// RoaringBitmapMaxContainerCount is the maximum number of high-word containers.
	RoaringBitmapMaxContainerCount = 1 << 16
	// RoaringBitmapArrayMaxSize is the threshold for dense-container conversion.
	RoaringBitmapArrayMaxSize = 4096
	// RoaringBitmapArrayShrinkSize is the threshold for sparse-container conversion.
	RoaringBitmapArrayShrinkSize = RoaringBitmapArrayMaxSize / 2

	roaringBitmapContainerBits      = 16
	roaringBitmapContainerSize      = 1 << roaringBitmapContainerBits
	roaringBitmapBitmapWords        = RoaringBitmapBitmapWords
	roaringBitmapArrayMaxSize       = RoaringBitmapArrayMaxSize
	roaringBitmapArrayShrinkSize    = RoaringBitmapArrayShrinkSize
	roaringBitmapMaxContainerCount  = RoaringBitmapMaxContainerCount
	roaringBitmapContainerKindArray = "array"
	roaringBitmapContainerKindBits  = "bitmap"
)

// RoaringBitmapInfo reports the shape and memory footprint of an exact uint32 set.
type RoaringBitmapInfo struct {
	Cardinality      uint64 `json:"cardinality"`
	Containers       uint64 `json:"containers"`
	ArrayContainers  uint64 `json:"array_containers"`
	BitmapContainers uint64 `json:"bitmap_containers"`
	EncodedBytes     uint64 `json:"encoded_bytes"`
}

// RoaringBitmapContainerSnapshot is one portable container representation.
type RoaringBitmapContainerSnapshot struct {
	Key         uint16 `json:"key"`
	Kind        string `json:"kind"`
	Cardinality uint32 `json:"cardinality"`
	Values      string `json:"values,omitempty"`
	Bits        string `json:"bits,omitempty"`
}

// RoaringBitmapSnapshot is a portable representation of a RoaringBitmap.
type RoaringBitmapSnapshot struct {
	Cardinality uint64                           `json:"cardinality"`
	Containers  []RoaringBitmapContainerSnapshot `json:"containers"`
}

// RoaringBitmap stores an exact uint32 set using sparse arrays and dense bitsets.
type RoaringBitmap struct {
	containers []roaringBitmapContainer
	count      uint64
}

// Legacy package-local names keep the representation-layout benchmarks focused
// on the same implementation as the public API.
type roaringBitmapData = RoaringBitmap

func newRoaringBitmapData() roaringBitmapData { return NewRoaringBitmap() }

type roaringBitmapContainer struct {
	key         uint16
	values      []uint16
	bits        *[roaringBitmapBitmapWords]uint64
	cardinality uint32
}

// NewRoaringBitmap creates an empty bitmap.
func NewRoaringBitmap() RoaringBitmap { return RoaringBitmap{} }

// ValidateRoaringBitmapSnapshot verifies a portable bitmap representation.
func ValidateRoaringBitmapSnapshot(snapshot RoaringBitmapSnapshot) error {
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

func validateRoaringBitmapContainerSnapshot(snapshot RoaringBitmapContainerSnapshot) (uint32, error) {
	switch snapshot.Kind {
	case roaringBitmapContainerKindArray:
		size, ok := roaringBase64DecodedSize(snapshot.Values)
		if !ok {
			return 0, errors.New("hatriecache: invalid base64 encoding")
		}
		if size%2 != 0 {
			return 0, errors.New("hatriecache: invalid roaring bitmap array payload")
		}
		if size/2 > roaringBitmapArrayMaxSize {
			return 0, errors.New("hatriecache: roaring bitmap array container is too large")
		}
		if uint32(size/2) != snapshot.Cardinality {
			return 0, errors.New("hatriecache: roaring bitmap array cardinality mismatch")
		}
		raw, err := base64.StdEncoding.DecodeString(snapshot.Values)
		if err != nil {
			return 0, err
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
		size, ok := roaringBase64DecodedSize(snapshot.Bits)
		if !ok || size != roaringBitmapBitmapWords*8 {
			return 0, errors.New("hatriecache: invalid roaring bitmap bitset payload")
		}
		raw, err := base64.StdEncoding.DecodeString(snapshot.Bits)
		if err != nil {
			return 0, err
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

// NewRoaringBitmapFromSnapshot restores a bitmap after validating its snapshot.
func NewRoaringBitmapFromSnapshot(snapshot RoaringBitmapSnapshot) (RoaringBitmap, error) {
	if err := ValidateRoaringBitmapSnapshot(snapshot); err != nil {
		return RoaringBitmap{}, err
	}
	out := RoaringBitmap{containers: make([]roaringBitmapContainer, 0, len(snapshot.Containers)), count: snapshot.Cardinality}
	for _, rawContainer := range snapshot.Containers {
		container, err := newRoaringBitmapContainerFromSnapshot(rawContainer)
		if err != nil {
			return RoaringBitmap{}, err
		}
		out.containers = append(out.containers, container)
	}
	return out, nil
}

func newRoaringBitmapContainerFromSnapshot(snapshot RoaringBitmapContainerSnapshot) (roaringBitmapContainer, error) {
	container := roaringBitmapContainer{key: snapshot.Key, cardinality: snapshot.Cardinality}
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
		container.bits = new([roaringBitmapBitmapWords]uint64)
		for idx := range container.bits {
			container.bits[idx] = binary.LittleEndian.Uint64(raw[idx*8 : idx*8+8])
		}
	}
	return container, nil
}

// Add inserts values and returns the number not already present.
func (bitmap *RoaringBitmap) Add(value uint32, values ...uint32) int {
	if bitmap == nil {
		return 0
	}
	added := 0
	if bitmap.add(value) {
		added++
	}
	for _, value := range values {
		if bitmap.add(value) {
			added++
		}
	}
	return added
}

func (bitmap *RoaringBitmap) add(value uint32) bool {
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

// Remove deletes values and returns the number that were present.
func (bitmap *RoaringBitmap) Remove(value uint32, values ...uint32) int {
	if bitmap == nil {
		return 0
	}
	removed := 0
	if bitmap.remove(value) {
		removed++
	}
	for _, value := range values {
		if bitmap.remove(value) {
			removed++
		}
	}
	return removed
}

func (bitmap *RoaringBitmap) remove(value uint32) bool {
	key, low := roaringBitmapSplit(value)
	idx, found := bitmap.findContainer(key)
	if !found || !bitmap.containers[idx].remove(low) {
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

// Contains reports whether value is present.
func (bitmap RoaringBitmap) Contains(value uint32) bool {
	key, low := roaringBitmapSplit(value)
	idx, found := bitmap.findContainer(key)
	return found && bitmap.containers[idx].contains(low)
}

// Count returns the number of stored values.
func (bitmap RoaringBitmap) Count() uint64 { return bitmap.count }

// Values returns all values in ascending order.
func (bitmap RoaringBitmap) Values() []uint32 {
	if bitmap.count == 0 {
		return []uint32{}
	}
	out := make([]uint32, 0, int(bitmap.count))
	for idx := range bitmap.containers {
		out = bitmap.containers[idx].appendValues(out)
	}
	return out
}

// Info returns cardinality, representation counts, and encoded payload bytes.
func (bitmap RoaringBitmap) Info() RoaringBitmapInfo {
	info := RoaringBitmapInfo{Cardinality: bitmap.count, Containers: uint64(len(bitmap.containers)), EncodedBytes: uint64(bitmap.EncodedSize())}
	for idx := range bitmap.containers {
		if bitmap.containers[idx].isBitmap() {
			info.BitmapContainers++
		} else {
			info.ArrayContainers++
		}
	}
	return info
}

// Snapshot returns a deep-copied portable representation.
func (bitmap RoaringBitmap) Snapshot() RoaringBitmapSnapshot {
	containers := make([]RoaringBitmapContainerSnapshot, len(bitmap.containers))
	for idx := range bitmap.containers {
		containers[idx] = bitmap.containers[idx].snapshot()
	}
	return RoaringBitmapSnapshot{Cardinality: bitmap.count, Containers: containers}
}

// EncodedSize returns the raw bytes used by portable container payloads.
func (bitmap RoaringBitmap) EncodedSize() int64 {
	var total int64
	for idx := range bitmap.containers {
		total += bitmap.containers[idx].encodedSize()
	}
	return total
}

// ContainerCount returns the number of sorted high-word containers.
func (bitmap RoaringBitmap) ContainerCount() int { return len(bitmap.containers) }

// VisitContainers visits each container without copying its payload. The callback
// must not retain or mutate values or bitset. Returning false stops iteration.
func (bitmap RoaringBitmap) VisitContainers(visit func(key uint16, cardinality uint32, values []uint16, bitset []uint64) bool) {
	if visit == nil {
		return
	}
	for idx := range bitmap.containers {
		container := &bitmap.containers[idx]
		if container.isBitmap() {
			if !visit(container.key, container.cardinality, nil, container.bits[:]) {
				return
			}
			continue
		}
		if !visit(container.key, container.cardinality, container.values, nil) {
			return
		}
	}
}

func (bitmap RoaringBitmap) findContainer(key uint16) (int, bool) {
	idx := sort.Search(len(bitmap.containers), func(idx int) bool { return bitmap.containers[idx].key >= key })
	return idx, idx < len(bitmap.containers) && bitmap.containers[idx].key == key
}

func (container *roaringBitmapContainer) add(value uint16) bool {
	if bitmap := container.bits; bitmap != nil {
		word, mask := roaringBitmapBit(value)
		if bitmap[word]&mask != 0 {
			return false
		}
		bitmap[word] |= mask
		container.cardinality++
		return true
	}
	idx := sort.Search(len(container.values), func(idx int) bool { return container.values[idx] >= value })
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
	if bitmap := container.bits; bitmap != nil {
		word, mask := roaringBitmapBit(value)
		if bitmap[word]&mask == 0 {
			return false
		}
		bitmap[word] &^= mask
		container.cardinality--
		if container.cardinality <= roaringBitmapArrayShrinkSize {
			container.convertToArray()
		}
		return true
	}
	idx := sort.Search(len(container.values), func(idx int) bool { return container.values[idx] >= value })
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
	if bitmap := container.bits; bitmap != nil {
		word, mask := roaringBitmapBit(value)
		return bitmap[word]&mask != 0
	}
	idx := sort.Search(len(container.values), func(idx int) bool { return container.values[idx] >= value })
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

func (container roaringBitmapContainer) snapshot() RoaringBitmapContainerSnapshot {
	snapshot := RoaringBitmapContainerSnapshot{Key: container.key, Cardinality: container.cardinality}
	if container.isBitmap() {
		raw := make([]byte, len(container.bits)*8)
		for idx, word := range container.bits {
			binary.LittleEndian.PutUint64(raw[idx*8:idx*8+8], word)
		}
		snapshot.Kind, snapshot.Bits = roaringBitmapContainerKindBits, base64.StdEncoding.EncodeToString(raw)
		return snapshot
	}
	raw := make([]byte, len(container.values)*2)
	for idx, value := range container.values {
		binary.LittleEndian.PutUint16(raw[idx*2:idx*2+2], value)
	}
	snapshot.Kind, snapshot.Values = roaringBitmapContainerKindArray, base64.StdEncoding.EncodeToString(raw)
	return snapshot
}

func (container roaringBitmapContainer) encodedSize() int64 {
	if container.isBitmap() {
		return roaringBitmapBitmapWords * 8
	}
	return int64(len(container.values) * 2)
}
func (container roaringBitmapContainer) empty() bool    { return container.cardinality == 0 }
func (container roaringBitmapContainer) isBitmap() bool { return container.bits != nil }

func (container *roaringBitmapContainer) convertToBitmap() {
	if container.isBitmap() {
		return
	}
	next := new([roaringBitmapBitmapWords]uint64)
	for _, value := range container.values {
		word, mask := roaringBitmapBit(value)
		next[word] |= mask
	}
	for idx := range container.values {
		container.values[idx] = 0
	}
	container.values, container.bits = nil, next
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
	container.bits, container.values = nil, values
}

func (container *roaringBitmapContainer) clear() {
	for idx := range container.values {
		container.values[idx] = 0
	}
	if container.bits != nil {
		*container.bits = [roaringBitmapBitmapWords]uint64{}
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

func roaringBase64DecodedSize(encoded string) (int, bool) {
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
