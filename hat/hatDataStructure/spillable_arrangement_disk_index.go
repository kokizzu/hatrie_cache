package hatDataStructure

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
	"os"
)

const spillableArrangementDiskIndexStride uint64 = 128

// spillableArrangementDiskIndex keeps the sorted sidecar authoritative while
// retaining only sparse record offsets. It is deliberately read-only; writes
// materialize the normal map before changing the arrangement.
type spillableArrangementDiskIndex struct {
	file        *os.File
	indexSize   int64
	payloadEnd  int64
	segmentSize int64
	entryCount  uint64
	generation  uint64
	spillRecords uint64
	anchors     []int64
}

func (arrangement *SpillableArrangement) restoreDiskResidentIndex(size int64) bool {
	if arrangement == nil {
		return false
	}
	index, ok := openSpillableArrangementDiskIndex(
		spillableArrangementIndexPath(arrangement.spillPath),
		size,
		arrangement.maxKeyBytes,
	)
	if !ok {
		return false
	}
	arrangement.diskIndex = index
	arrangement.entries = nil
	arrangement.coldEntries = int(index.entryCount)
	arrangement.hotBytes = 0
	arrangement.generation = index.generation
	arrangement.spillRecords = index.spillRecords
	arrangement.diskBytes = size
	arrangement.persistedIndex = true
	return true
}

func openSpillableArrangementDiskIndex(path string, segmentSize, maxKeyBytes int64) (*spillableArrangementDiskIndex, bool) {
	if path == "" || segmentSize < 0 {
		return nil, false
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > spillableArrangementIndexMaxBytes {
		return nil, false
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, false
	}
	fail := func() (*spillableArrangementDiskIndex, bool) {
		_ = file.Close()
		return nil, false
	}
	indexSize := info.Size()
	if indexSize < spillableArrangementIndexHeaderSize+4 {
		return fail()
	}
	reader := bufio.NewReaderSize(file, 16<<10)
	checksum := crc32.New(spillableArrangementIndexCRCTable)
	var header [spillableArrangementIndexHeaderSize]byte
	if !readSpillableArrangementIndexPayload(reader, checksum, header[:]) {
		return fail()
	}
	if header[0] != spillableArrangementIndexMagic[0] || header[1] != spillableArrangementIndexMagic[1] || header[2] != spillableArrangementIndexMagic[2] || header[3] != spillableArrangementIndexMagic[3] {
		return fail()
	}
	if binary.LittleEndian.Uint32(header[4:8]) != spillableArrangementIndexVersion || int64(binary.LittleEndian.Uint64(header[8:16])) != segmentSize {
		return fail()
	}
	entryCount := binary.LittleEndian.Uint64(header[24:32])
	if entryCount > uint64((indexSize-spillableArrangementIndexHeaderSize-4)/(4+24)) || entryCount > uint64(math.MaxInt) {
		return fail()
	}
	payloadEnd := indexSize - 4
	position := int64(spillableArrangementIndexHeaderSize)
	anchors := make([]int64, 0, (entryCount+spillableArrangementDiskIndexStride-1)/spillableArrangementDiskIndexStride)
	keyLengthBytes := make([]byte, 4)
	referenceBytes := make([]byte, 24)
	keyBuffer := make([]byte, 0, 256)
	previousKey := make([]byte, 0, 256)
	for ordinal := uint64(0); ordinal < entryCount; ordinal++ {
		if payloadEnd-position < 4+24 {
			return fail()
		}
		recordPosition := position
		if ordinal%spillableArrangementDiskIndexStride == 0 {
			anchors = append(anchors, recordPosition)
		}
		if !readSpillableArrangementIndexPayload(reader, checksum, keyLengthBytes) {
			return fail()
		}
		keyLengthValue := binary.LittleEndian.Uint32(keyLengthBytes)
		if keyLengthValue == 0 || uint64(keyLengthValue) > uint64(maxKeyBytes) || uint64(keyLengthValue) > uint64(math.MaxInt) || int64(keyLengthValue) > payloadEnd-position-4-24 {
			return fail()
		}
		keyLength := int(keyLengthValue)
		if cap(keyBuffer) < keyLength {
			keyBuffer = make([]byte, keyLength)
		} else {
			keyBuffer = keyBuffer[:keyLength]
		}
		if !readSpillableArrangementIndexPayload(reader, checksum, keyBuffer) {
			return fail()
		}
		if ordinal > 0 && bytes.Compare(previousKey, keyBuffer) >= 0 {
			return fail()
		}
		previousKey = append(previousKey[:0], keyBuffer...)
		if !readSpillableArrangementIndexPayload(reader, checksum, referenceBytes) {
			return fail()
		}
		offset := binary.LittleEndian.Uint64(referenceBytes[:8])
		total := binary.LittleEndian.Uint64(referenceBytes[8:16])
		generation := binary.LittleEndian.Uint64(referenceBytes[16:24])
		position += int64(4 + keyLength + 24)
		if offset > uint64(math.MaxInt64) || total > uint64(math.MaxInt64) || total < spillableArrangementHeaderSize || offset > uint64(segmentSize) || total > uint64(segmentSize)-offset || generation == 0 {
			return fail()
		}
	}
	if position != payloadEnd {
		return fail()
	}
	var storedChecksum [4]byte
	if _, err := io.ReadFull(reader, storedChecksum[:]); err != nil || binary.LittleEndian.Uint32(storedChecksum[:]) != checksum.Sum32() {
		return fail()
	}
	return &spillableArrangementDiskIndex{
		file:         file,
		indexSize:    indexSize,
		payloadEnd:   payloadEnd,
		segmentSize:  segmentSize,
		entryCount:   entryCount,
		generation:   binary.LittleEndian.Uint64(header[16:24]),
		spillRecords: binary.LittleEndian.Uint64(header[32:40]),
		anchors:      anchors,
	}, true
}

func (index *spillableArrangementDiskIndex) readRecordAt(position int64, maxKeyBytes int64) ([]byte, spillableArrangementRef, uint64, int64, error) {
	if index == nil || index.file == nil || position < spillableArrangementIndexHeaderSize || position > index.payloadEnd-4-24 {
		return nil, spillableArrangementRef{}, 0, 0, ErrSpillableArrangementCorrupt
	}
	var keyLengthBytes [4]byte
	if _, err := index.file.ReadAt(keyLengthBytes[:], position); err != nil {
		return nil, spillableArrangementRef{}, 0, 0, ErrSpillableArrangementCorrupt
	}
	keyLengthValue := binary.LittleEndian.Uint32(keyLengthBytes[:])
	if keyLengthValue == 0 || uint64(keyLengthValue) > uint64(maxKeyBytes) || uint64(keyLengthValue) > uint64(math.MaxInt) {
		return nil, spillableArrangementRef{}, 0, 0, ErrSpillableArrangementCorrupt
	}
	keyLength := int64(keyLengthValue)
	recordEnd := position + 4 + keyLength + 24
	if recordEnd < position || recordEnd > index.payloadEnd {
		return nil, spillableArrangementRef{}, 0, 0, ErrSpillableArrangementCorrupt
	}
	key := make([]byte, int(keyLength))
	if _, err := index.file.ReadAt(key, position+4); err != nil {
		return nil, spillableArrangementRef{}, 0, 0, ErrSpillableArrangementCorrupt
	}
	var referenceBytes [24]byte
	if _, err := index.file.ReadAt(referenceBytes[:], position+4+keyLength); err != nil {
		return nil, spillableArrangementRef{}, 0, 0, ErrSpillableArrangementCorrupt
	}
	offset := binary.LittleEndian.Uint64(referenceBytes[:8])
	total := binary.LittleEndian.Uint64(referenceBytes[8:16])
	generation := binary.LittleEndian.Uint64(referenceBytes[16:24])
	if offset > uint64(math.MaxInt64) || total > uint64(math.MaxInt64) || total < spillableArrangementHeaderSize || offset > uint64(index.segmentSize) || total > uint64(index.segmentSize)-offset || generation == 0 {
		return nil, spillableArrangementRef{}, 0, 0, ErrSpillableArrangementCorrupt
	}
	return key, spillableArrangementRef{offset: int64(offset), total: int64(total)}, generation, recordEnd, nil
}

func (index *spillableArrangementDiskIndex) lookup(key string, maxKeyBytes, _ int64) (spillableArrangementRef, uint64, bool, error) {
	if index == nil || index.entryCount == 0 {
		return spillableArrangementRef{}, 0, false, nil
	}
	target := []byte(key)
	low, high := 0, len(index.anchors)
	for low < high {
		middle := low + (high-low)/2
		candidate, _, _, _, err := index.readRecordAt(index.anchors[middle], maxKeyBytes)
		if err != nil {
			return spillableArrangementRef{}, 0, false, err
		}
		if bytes.Compare(candidate, target) <= 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	anchor := low - 1
	if anchor < 0 {
		anchor = 0
	}
	position := index.anchors[anchor]
	end := index.payloadEnd
	if anchor+1 < len(index.anchors) {
		end = index.anchors[anchor+1]
	}
	for position < end {
		candidate, reference, generation, next, err := index.readRecordAt(position, maxKeyBytes)
		if err != nil {
			return spillableArrangementRef{}, 0, false, err
		}
		comparison := bytes.Compare(candidate, target)
		if comparison == 0 {
			return reference, generation, true, nil
		}
		if comparison > 0 {
			return spillableArrangementRef{}, 0, false, nil
		}
		position = next
	}
	return spillableArrangementRef{}, 0, false, nil
}

func (index *spillableArrangementDiskIndex) materialize(maxKeyBytes int64) (map[string]*spillableArrangementEntry, error) {
	if index == nil || index.entryCount > uint64(math.MaxInt) {
		return nil, ErrSpillableArrangementCorrupt
	}
	entries := make(map[string]*spillableArrangementEntry, int(index.entryCount))
	position := int64(spillableArrangementIndexHeaderSize)
	for ordinal := uint64(0); ordinal < index.entryCount; ordinal++ {
		keyBytes, reference, generation, next, err := index.readRecordAt(position, maxKeyBytes)
		if err != nil {
			return nil, err
		}
		key := string(keyBytes)
		if _, exists := entries[key]; exists {
			return nil, ErrSpillableArrangementCorrupt
		}
		entries[key] = &spillableArrangementEntry{key: key, ref: reference, gen: generation}
		position = next
	}
	if position != index.payloadEnd {
		return nil, ErrSpillableArrangementCorrupt
	}
	return entries, nil
}

func (index *spillableArrangementDiskIndex) forEach(maxKeyBytes int64, visit func([]byte, spillableArrangementRef, uint64) error) error {
	if index == nil {
		return ErrSpillableArrangementCorrupt
	}
	position := int64(spillableArrangementIndexHeaderSize)
	for ordinal := uint64(0); ordinal < index.entryCount; ordinal++ {
		key, reference, generation, next, err := index.readRecordAt(position, maxKeyBytes)
		if err != nil {
			return err
		}
		if err := visit(key, reference, generation); err != nil {
			return err
		}
		position = next
	}
	if position != index.payloadEnd {
		return ErrSpillableArrangementCorrupt
	}
	return nil
}

func (index *spillableArrangementDiskIndex) close() error {
	if index == nil || index.file == nil {
		return nil
	}
	err := index.file.Close()
	index.file = nil
	return err
}

func (arrangement *SpillableArrangement) materializeDiskIndexLocked() error {
	if arrangement == nil || arrangement.diskIndex == nil {
		if arrangement != nil && arrangement.entries == nil {
			arrangement.entries = make(map[string]*spillableArrangementEntry)
		}
		return nil
	}
	index := arrangement.diskIndex
	entries, err := index.materialize(arrangement.maxKeyBytes)
	if err != nil {
		return err
	}
	if err := index.close(); err != nil {
		return err
	}
	arrangement.diskIndex = nil
	arrangement.entries = entries
	arrangement.coldEntries = len(entries)
	arrangement.hotBytes = 0
	arrangement.generation = index.generation
	arrangement.spillRecords = index.spillRecords
	arrangement.persistedIndex = true
	return nil
}

func (arrangement *SpillableArrangement) snapshotDiskIndexLocked() ([]SpillableArrangementEntry, error) {
	index := arrangement.diskIndex
	rows := make([]SpillableArrangementEntry, 0, int(index.entryCount))
	err := index.forEach(arrangement.maxKeyBytes, func(key []byte, reference spillableArrangementRef, generation uint64) error {
		entry := &spillableArrangementEntry{key: string(key), ref: reference, gen: generation}
		value, err := arrangement.readColdValueLocked(entry)
		if err != nil {
			return err
		}
		rows = append(rows, SpillableArrangementEntry{Key: entry.key, Value: value})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}
