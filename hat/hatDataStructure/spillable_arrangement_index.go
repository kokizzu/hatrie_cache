package hatDataStructure

import (
	"bufio"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
)

const (
	spillableArrangementIndexVersion    uint32 = 1
	spillableArrangementIndexHeaderSize        = 40
	spillableArrangementIndexMaxBytes   int64  = 64 << 20
)

var spillableArrangementIndexMagic = [4]byte{'H', 'S', 'I', '1'}
var spillableArrangementIndexCRCTable = crc32.MakeTable(crc32.Castagnoli)

func spillableArrangementIndexPath(path string) string {
	return path + ".idx"
}

// restorePersistedIndex loads the latest cold-entry references without
// trusting the sidecar as the source of record integrity. Every cold read
// still validates the pointed-to segment record; any sidecar problem returns
// false so the caller can use the complete recovery scan.
func (arrangement *SpillableArrangement) restorePersistedIndexStreaming(size int64) bool {
	if arrangement == nil || size < 0 {
		return false
	}
	path := spillableArrangementIndexPath(arrangement.spillPath)
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > spillableArrangementIndexMaxBytes {
		return false
	}
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	indexSize := info.Size()
	if indexSize < spillableArrangementIndexHeaderSize+4 {
		return false
	}
	reader := bufio.NewReaderSize(file, 16<<10)
	checksum := crc32.New(spillableArrangementIndexCRCTable)
	var header [spillableArrangementIndexHeaderSize]byte
	if !readSpillableArrangementIndexPayload(reader, checksum, header[:]) {
		return false
	}
	if header[0] != spillableArrangementIndexMagic[0] || header[1] != spillableArrangementIndexMagic[1] || header[2] != spillableArrangementIndexMagic[2] || header[3] != spillableArrangementIndexMagic[3] {
		return false
	}
	if binary.LittleEndian.Uint32(header[4:8]) != spillableArrangementIndexVersion || int64(binary.LittleEndian.Uint64(header[8:16])) != size {
		return false
	}
	generation := binary.LittleEndian.Uint64(header[16:24])
	entryCount := binary.LittleEndian.Uint64(header[24:32])
	spillRecords := binary.LittleEndian.Uint64(header[32:40])
	if entryCount > uint64(indexSize/spillableArrangementIndexHeaderSize) || entryCount > uint64(math.MaxInt) {
		return false
	}
	entries := make(map[string]*spillableArrangementEntry, int(entryCount))
	position := int64(spillableArrangementIndexHeaderSize)
	var maxGeneration uint64
	var keyLengthBytes [4]byte
	var referenceBytes [24]byte
	keyBuffer := make([]byte, 0, 256)
	for index := uint64(0); index < entryCount; index++ {
		if indexSize-position < 4+24+4 {
			return false
		}
		if !readSpillableArrangementIndexPayload(reader, checksum, keyLengthBytes[:]) {
			return false
		}
		keyLengthValue := binary.LittleEndian.Uint32(keyLengthBytes[:])
		if keyLengthValue == 0 || uint64(keyLengthValue) > uint64(arrangement.maxKeyBytes) || uint64(keyLengthValue) > uint64(math.MaxInt) || int64(keyLengthValue) > indexSize-position-4-24-4 {
			return false
		}
		keyLength := int(keyLengthValue)
		if cap(keyBuffer) < keyLength {
			keyBuffer = make([]byte, keyLength)
		} else {
			keyBuffer = keyBuffer[:keyLength]
		}
		if !readSpillableArrangementIndexPayload(reader, checksum, keyBuffer) {
			return false
		}
		if !readSpillableArrangementIndexPayload(reader, checksum, referenceBytes[:]) {
			return false
		}
		offset := binary.LittleEndian.Uint64(referenceBytes[:8])
		total := binary.LittleEndian.Uint64(referenceBytes[8:16])
		entryGeneration := binary.LittleEndian.Uint64(referenceBytes[16:24])
		position += int64(4 + keyLength + 24)
		if offset > uint64(math.MaxInt64) || total > uint64(math.MaxInt64) || total < spillableArrangementHeaderSize || offset > uint64(size) || total > uint64(size)-offset || entryGeneration == 0 {
			return false
		}
		key := string(keyBuffer)
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
	if position != indexSize-4 || generation < maxGeneration {
		return false
	}
	var storedChecksum [4]byte
	if _, err := io.ReadFull(reader, storedChecksum[:]); err != nil || binary.LittleEndian.Uint32(storedChecksum[:]) != checksum.Sum32() {
		return false
	}
	if _, err := arrangement.file.Seek(0, io.SeekEnd); err != nil {
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

func readSpillableArrangementIndexPayload(reader *bufio.Reader, checksum hash.Hash, payload []byte) bool {
	if _, err := io.ReadFull(reader, payload); err != nil {
		return false
	}
	_, _ = checksum.Write(payload)
	return true
}

// persistIndexLocked writes an advisory index atomically. Data remains
// recoverable by the segment scanner when this sidecar is absent or invalid.
func (arrangement *SpillableArrangement) persistIndexLocked() error {
	if arrangement == nil {
		return ErrSpillableArrangementNil
	}
	entries := make([]*spillableArrangementEntry, 0, len(arrangement.entries))
	for _, entry := range arrangement.entries {
		if !entry.valueHot {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].key < entries[right].key })
	total := int64(spillableArrangementIndexHeaderSize + 4)
	for _, entry := range entries {
		if len(entry.key) == 0 || uint64(len(entry.key)) > uint64(math.MaxUint32) || entry.ref.offset < 0 || entry.ref.total < spillableArrangementHeaderSize || entry.ref.total > arrangement.diskBytes-entry.ref.offset || entry.gen == 0 {
			return ErrSpillableArrangementCorrupt
		}
		recordBytes := int64(4 + len(entry.key) + 24)
		if recordBytes > math.MaxInt64-total {
			return ErrSpillableArrangementCorrupt
		}
		total += recordBytes
	}
	if total > spillableArrangementIndexMaxBytes {
		return ErrSpillableArrangementCorrupt
	}
	data := make([]byte, int(total))
	copy(data[:4], spillableArrangementIndexMagic[:])
	binary.LittleEndian.PutUint32(data[4:8], spillableArrangementIndexVersion)
	binary.LittleEndian.PutUint64(data[8:16], uint64(arrangement.diskBytes))
	binary.LittleEndian.PutUint64(data[16:24], arrangement.generation)
	binary.LittleEndian.PutUint64(data[24:32], uint64(len(entries)))
	binary.LittleEndian.PutUint64(data[32:40], arrangement.spillRecords)
	position := spillableArrangementIndexHeaderSize
	for _, entry := range entries {
		binary.LittleEndian.PutUint32(data[position:position+4], uint32(len(entry.key)))
		position += 4
		copy(data[position:position+len(entry.key)], entry.key)
		position += len(entry.key)
		binary.LittleEndian.PutUint64(data[position:position+8], uint64(entry.ref.offset))
		binary.LittleEndian.PutUint64(data[position+8:position+16], uint64(entry.ref.total))
		binary.LittleEndian.PutUint64(data[position+16:position+24], entry.gen)
		position += 24
	}
	binary.LittleEndian.PutUint32(data[position:], crc32.Checksum(data[:position], spillableArrangementIndexCRCTable))
	return writeSpillableArrangementIndex(spillableArrangementIndexPath(arrangement.spillPath), data)
}

func writeSpillableArrangementIndex(path string, data []byte) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := writeSpillableArrangementAll(temporary, data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	removeTemporary = false
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func invalidateSpillableArrangementIndex(path string) error {
	err := os.Remove(spillableArrangementIndexPath(path))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
