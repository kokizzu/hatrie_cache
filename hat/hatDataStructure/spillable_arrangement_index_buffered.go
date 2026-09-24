package hatDataStructure

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
)

const spillableArrangementIndexReadBufferMaxBytes int64 = 1 << 20

var spillableArrangementIndexBufferPool sync.Pool

func (arrangement *SpillableArrangement) restorePersistedIndex(size int64) bool {
	if arrangement == nil || size < 0 {
		return false
	}
	path := spillableArrangementIndexPath(arrangement.spillPath)
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > spillableArrangementIndexMaxBytes {
		return false
	}
	if info.Size() > spillableArrangementIndexReadBufferMaxBytes {
		return arrangement.restorePersistedIndexStreaming(size)
	}
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	indexSize := info.Size()
	data := takeSpillableArrangementIndexBuffer(int(indexSize))
	defer releaseSpillableArrangementIndexBuffer(data)
	if _, err := io.ReadFull(file, data); err != nil {
		return false
	}
	if len(data) < spillableArrangementIndexHeaderSize+4 {
		return false
	}
	payloadLength := len(data) - 4
	if binary.LittleEndian.Uint32(data[payloadLength:]) != crc32.Checksum(data[:payloadLength], spillableArrangementIndexCRCTable) {
		return false
	}
	if data[0] != spillableArrangementIndexMagic[0] || data[1] != spillableArrangementIndexMagic[1] || data[2] != spillableArrangementIndexMagic[2] || data[3] != spillableArrangementIndexMagic[3] {
		return false
	}
	if binary.LittleEndian.Uint32(data[4:8]) != spillableArrangementIndexVersion || int64(binary.LittleEndian.Uint64(data[8:16])) != size {
		return false
	}
	generation := binary.LittleEndian.Uint64(data[16:24])
	entryCount := binary.LittleEndian.Uint64(data[24:32])
	spillRecords := binary.LittleEndian.Uint64(data[32:40])
	if entryCount > uint64(payloadLength-spillableArrangementIndexHeaderSize) || entryCount > uint64(math.MaxInt) {
		return false
	}
	entries := make(map[string]*spillableArrangementEntry, int(entryCount))
	position := spillableArrangementIndexHeaderSize
	var maxGeneration uint64
	for index := uint64(0); index < entryCount; index++ {
		if position+4 > payloadLength {
			return false
		}
		keyLength := int(binary.LittleEndian.Uint32(data[position : position+4]))
		position += 4
		if keyLength <= 0 || int64(keyLength) > arrangement.maxKeyBytes || uint64(keyLength) > uint64(math.MaxInt) || keyLength > payloadLength-position || position+keyLength+24 > payloadLength {
			return false
		}
		key := string(data[position : position+keyLength])
		position += keyLength
		offset := binary.LittleEndian.Uint64(data[position : position+8])
		total := binary.LittleEndian.Uint64(data[position+8 : position+16])
		entryGeneration := binary.LittleEndian.Uint64(data[position+16 : position+24])
		position += 24
		if offset > uint64(math.MaxInt64) || total > uint64(math.MaxInt64) || total < spillableArrangementHeaderSize || offset > uint64(size) || total > uint64(size)-offset || entryGeneration == 0 {
			return false
		}
		if _, exists := entries[key]; exists {
			return false
		}
		entries[key] = &spillableArrangementEntry{
			key:      key,
			ref:      spillableArrangementRef{offset: int64(offset), total: int64(total)},
			gen:      entryGeneration,
			valueHot: false,
		}
		if entryGeneration > maxGeneration {
			maxGeneration = entryGeneration
		}
	}
	if position != payloadLength || generation < maxGeneration {
		return false
	}
	arrangement.entries = entries
	arrangement.coldEntries = len(entries)
	arrangement.hotBytes = 0
	arrangement.generation = generation
	arrangement.spillRecords = spillRecords
	arrangement.diskBytes = size
	arrangement.persistedIndex = true
	return true
}

func takeSpillableArrangementIndexBuffer(size int) []byte {
	if candidate, ok := spillableArrangementIndexBufferPool.Get().([]byte); ok && cap(candidate) >= size {
		return candidate[:size]
	}
	return make([]byte, size)
}

func releaseSpillableArrangementIndexBuffer(data []byte) {
	if int64(cap(data)) <= spillableArrangementIndexReadBufferMaxBytes {
		spillableArrangementIndexBufferPool.Put(data[:0])
	}
}
