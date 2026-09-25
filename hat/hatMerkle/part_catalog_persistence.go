package hatMerkle

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	partCatalogPersistenceVersion        uint16 = 2
	partCatalogPersistenceLegacyVersion  uint16 = 1
	partCatalogPersistenceHeaderBytes           = 16
	partCatalogPersistenceMaxBytes              = 64 << 20
	partCatalogPersistenceMaxEntries            = 100_000
	partCatalogPersistenceMaxColumns            = 65_536
	partCatalogPersistenceMaxStringBytes        = 1 << 20
)

var (
	// ErrPartCatalogSerializationInvalid reports a malformed, corrupted, or
	// unsupported catalog checkpoint.
	ErrPartCatalogSerializationInvalid = errors.New("hatriecache: invalid part catalog checkpoint")
	// ErrPartCatalogPathRequired reports a save or load call without a path.
	ErrPartCatalogPathRequired = errors.New("hatriecache: part catalog checkpoint path is required")
)

var partCatalogPersistenceCRC32CTable = crc32.MakeTable(crc32.Castagnoli)

var partCatalogPersistenceMagic = [4]byte{'H', 'P', 'C', 'K'}

// MarshalBinary returns a deterministic, bounded binary checkpoint of the
// catalog's lifecycle metadata. It stores names, locations, generations, and
// whole/column checksums, but never stores immutable part bytes.
func (catalog *PartCatalog) MarshalBinary() ([]byte, error) {
	if catalog == nil {
		return nil, ErrPartCatalogNil
	}
	generation, active, quarantined := catalog.persistenceSnapshot()
	if len(active)+len(quarantined) > partCatalogPersistenceMaxEntries {
		return nil, partCatalogPersistenceError("entry count exceeds %d", partCatalogPersistenceMaxEntries)
	}
	capacity := 32 + (len(active)+len(quarantined))*96
	for _, entry := range active {
		capacity += partCatalogPersistenceEntryCapacity(entry)
	}
	for _, entry := range quarantined {
		capacity += partCatalogPersistenceEntryCapacity(entry)
	}
	encoder := partCatalogPersistenceEncoder{
		data: make([]byte, 0, capacity),
	}
	if err := encoder.putUint64(generation); err != nil {
		return nil, err
	}
	if err := encoder.putUint32(uint32(len(active))); err != nil {
		return nil, err
	}
	if err := encoder.putUint32(uint32(len(quarantined))); err != nil {
		return nil, err
	}
	for _, entry := range active {
		if err := encoder.putEntry(entry); err != nil {
			return nil, err
		}
	}
	for _, entry := range quarantined {
		if err := encoder.putEntry(entry); err != nil {
			return nil, err
		}
	}
	return wrapPartCatalogPersistencePayload(encoder.data)
}

func partCatalogPersistenceEntryCapacity(entry PartCatalogEntry) int {
	if entry.Manifest.DeleteBitmap != nil {
		return 1 + 16 + partChecksumWireBytes
	}
	return 0
}

// RestorePartCatalog constructs a catalog from a validated binary checkpoint.
// The supplied options control the new catalog's future capacity; the
// checkpoint itself is never allowed to exceed the persistence bounds.
func RestorePartCatalog(data []byte, options PartCatalogOptions) (*PartCatalog, error) {
	if len(data) < partCatalogPersistenceHeaderBytes || len(data) > partCatalogPersistenceMaxBytes {
		return nil, partCatalogPersistenceError("checkpoint size is outside bounds")
	}
	payload, version, err := unwrapPartCatalogPersistencePayload(data)
	if err != nil {
		return nil, err
	}
	decoder := partCatalogPersistenceDecoder{data: payload}
	generation, err := decoder.uint64()
	if err != nil {
		return nil, err
	}
	activeCount, err := decoder.count()
	if err != nil {
		return nil, err
	}
	quarantinedCount, err := decoder.count()
	if err != nil {
		return nil, err
	}
	if activeCount+quarantinedCount > partCatalogPersistenceMaxEntries {
		return nil, partCatalogPersistenceError("entry count exceeds %d", partCatalogPersistenceMaxEntries)
	}
	catalog, err := NewPartCatalog(options)
	if err != nil {
		return nil, err
	}
	if activeCount+quarantinedCount > catalog.maxEntries {
		return nil, ErrPartCatalogCapacity
	}
	active := make(map[string]PartCatalogEntry, activeCount)
	quarantined := make(map[string]PartCatalogEntry, quarantinedCount)
	for index := 0; index < activeCount; index++ {
		entry, err := decoder.entry(generation, version >= partCatalogPersistenceVersion)
		if err != nil {
			return nil, err
		}
		if _, exists := active[entry.Name]; exists {
			return nil, partCatalogPersistenceError("duplicate active entry %q", entry.Name)
		}
		active[entry.Name] = entry
	}
	for index := 0; index < quarantinedCount; index++ {
		entry, err := decoder.entry(generation, version >= partCatalogPersistenceVersion)
		if err != nil {
			return nil, err
		}
		if _, exists := quarantined[entry.Name]; exists {
			return nil, partCatalogPersistenceError("duplicate quarantined entry %q", entry.Name)
		}
		quarantined[entry.Name] = entry
	}
	if !decoder.done() {
		return nil, partCatalogPersistenceError("trailing payload bytes")
	}
	catalog.generation = generation
	catalog.active = active
	catalog.quarantined = quarantined
	return catalog, nil
}

// Save atomically replaces path with a private checkpoint. The temporary file
// is created in the destination directory, synced before rename, and removed
// on every failure path.
func (catalog *PartCatalog) Save(path string) error {
	if catalog == nil {
		return ErrPartCatalogNil
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return ErrPartCatalogPathRequired
	}
	data, err := catalog.MarshalBinary()
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create part catalog checkpoint: %w", err)
	}
	temporaryName := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set part catalog checkpoint mode: %w", err)
	}
	if err := writePartCatalogCheckpoint(temporary, data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync part catalog checkpoint: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close part catalog checkpoint: %w", err)
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return fmt.Errorf("replace part catalog checkpoint: %w", err)
	}
	removeTemporary = false
	if err := syncPartCatalogDirectory(directory); err != nil {
		return fmt.Errorf("sync part catalog checkpoint directory: %w", err)
	}
	return nil
}

// LoadPartCatalog reads and validates one checkpoint without allowing a file
// larger than the bounded wire format to be loaded into memory.
func LoadPartCatalog(path string, options PartCatalogOptions) (*PartCatalog, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, ErrPartCatalogPathRequired
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, partCatalogPersistenceMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read part catalog checkpoint: %w", err)
	}
	if len(data) > partCatalogPersistenceMaxBytes {
		return nil, partCatalogPersistenceError("checkpoint exceeds %d bytes", partCatalogPersistenceMaxBytes)
	}
	return RestorePartCatalog(data, options)
}

func (catalog *PartCatalog) persistenceSnapshot() (generation uint64, active, quarantined []PartCatalogEntry) {
	catalog.mu.RLock()
	generation = catalog.generation
	active = make([]PartCatalogEntry, 0, len(catalog.active))
	for _, entry := range catalog.active {
		active = append(active, clonePartCatalogEntry(entry))
	}
	quarantined = make([]PartCatalogEntry, 0, len(catalog.quarantined))
	for _, entry := range catalog.quarantined {
		quarantined = append(quarantined, clonePartCatalogEntry(entry))
	}
	catalog.mu.RUnlock()
	sortPartCatalogEntries(active)
	sortPartCatalogEntries(quarantined)
	return generation, active, quarantined
}

type partCatalogPersistenceEncoder struct {
	data []byte
}

func (encoder *partCatalogPersistenceEncoder) putEntry(entry PartCatalogEntry) error {
	if err := validatePartCatalogPersistenceEntry(entry); err != nil {
		return err
	}
	if err := encoder.putString(entry.Name); err != nil {
		return err
	}
	if err := encoder.putString(entry.Location); err != nil {
		return err
	}
	if err := encoder.putUint64(entry.Generation); err != nil {
		return err
	}
	if err := encoder.putChecksum(entry.Manifest.Checksum); err != nil {
		return err
	}
	if err := encoder.putUint32(uint32(len(entry.Manifest.Columns))); err != nil {
		return err
	}
	for _, column := range entry.Manifest.Columns {
		if err := encoder.putString(column.Name); err != nil {
			return err
		}
		if err := encoder.putUint64(column.Offset); err != nil {
			return err
		}
		if err := encoder.putUint64(column.Size); err != nil {
			return err
		}
		if err := encoder.putChecksum(column.Checksum); err != nil {
			return err
		}
	}
	if entry.Manifest.DeleteBitmap == nil {
		return encoder.putByte(0)
	}
	if err := encoder.putByte(1); err != nil {
		return err
	}
	if err := encoder.putUint64(entry.Manifest.DeleteBitmap.RowCount); err != nil {
		return err
	}
	if err := encoder.putUint64(entry.Manifest.DeleteBitmap.DeletedCount); err != nil {
		return err
	}
	return encoder.putChecksum(entry.Manifest.DeleteBitmap.Snapshot)
}

func (encoder *partCatalogPersistenceEncoder) putChecksum(checksum PartChecksum) error {
	if err := encoder.putUint64(checksum.Size); err != nil {
		return err
	}
	return encoder.putBytes(checksum.Digest[:])
}

func (encoder *partCatalogPersistenceEncoder) putString(value string) error {
	if len(value) > partCatalogPersistenceMaxStringBytes {
		return partCatalogPersistenceError("string exceeds %d bytes", partCatalogPersistenceMaxStringBytes)
	}
	if err := encoder.putUint32(uint32(len(value))); err != nil {
		return err
	}
	return encoder.putBytes([]byte(value))
}

func (encoder *partCatalogPersistenceEncoder) putUint64(value uint64) error {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], value)
	return encoder.putBytes(data[:])
}

func (encoder *partCatalogPersistenceEncoder) putUint32(value uint32) error {
	var data [4]byte
	binary.LittleEndian.PutUint32(data[:], value)
	return encoder.putBytes(data[:])
}

func (encoder *partCatalogPersistenceEncoder) putByte(value byte) error {
	return encoder.putBytes([]byte{value})
}

func (encoder *partCatalogPersistenceEncoder) putBytes(data []byte) error {
	if len(data) > partCatalogPersistenceMaxBytes-len(encoder.data) {
		return partCatalogPersistenceError("checkpoint exceeds %d bytes", partCatalogPersistenceMaxBytes)
	}
	encoder.data = append(encoder.data, data...)
	return nil
}

type partCatalogPersistenceDecoder struct {
	data   []byte
	offset int
}

func (decoder *partCatalogPersistenceDecoder) bytes(length int) ([]byte, error) {
	if length < 0 || length > len(decoder.data)-decoder.offset {
		return nil, partCatalogPersistenceError("truncated payload")
	}
	start := decoder.offset
	decoder.offset += length
	return decoder.data[start:decoder.offset], nil
}

func (decoder *partCatalogPersistenceDecoder) uint64() (uint64, error) {
	data, err := decoder.bytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(data), nil
}

func (decoder *partCatalogPersistenceDecoder) uint32() (uint32, error) {
	data, err := decoder.bytes(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data), nil
}

func (decoder *partCatalogPersistenceDecoder) byte() (byte, error) {
	data, err := decoder.bytes(1)
	if err != nil {
		return 0, err
	}
	return data[0], nil
}

func (decoder *partCatalogPersistenceDecoder) count() (int, error) {
	value, err := decoder.uint32()
	if err != nil {
		return 0, err
	}
	if value > partCatalogPersistenceMaxEntries {
		return 0, partCatalogPersistenceError("entry count exceeds %d", partCatalogPersistenceMaxEntries)
	}
	return int(value), nil
}

func (decoder *partCatalogPersistenceDecoder) string() (string, error) {
	length, err := decoder.uint32()
	if err != nil {
		return "", err
	}
	if length > partCatalogPersistenceMaxStringBytes {
		return "", partCatalogPersistenceError("string exceeds %d bytes", partCatalogPersistenceMaxStringBytes)
	}
	data, err := decoder.bytes(int(length))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (decoder *partCatalogPersistenceDecoder) checksum() (PartChecksum, error) {
	size, err := decoder.uint64()
	if err != nil {
		return PartChecksum{}, err
	}
	digest, err := decoder.bytes(len(PartChecksum{}.Digest))
	if err != nil {
		return PartChecksum{}, err
	}
	var checksum PartChecksum
	checksum.Size = size
	copy(checksum.Digest[:], digest)
	return checksum, nil
}

func (decoder *partCatalogPersistenceDecoder) entry(catalogGeneration uint64, includeDeleteBitmap bool) (PartCatalogEntry, error) {
	name, err := decoder.string()
	if err != nil {
		return PartCatalogEntry{}, err
	}
	location, err := decoder.string()
	if err != nil {
		return PartCatalogEntry{}, err
	}
	generation, err := decoder.uint64()
	if err != nil {
		return PartCatalogEntry{}, err
	}
	checksum, err := decoder.checksum()
	if err != nil {
		return PartCatalogEntry{}, err
	}
	columnCount, err := decoder.uint32()
	if err != nil {
		return PartCatalogEntry{}, err
	}
	if columnCount > partCatalogPersistenceMaxColumns {
		return PartCatalogEntry{}, partCatalogPersistenceError("column count exceeds %d", partCatalogPersistenceMaxColumns)
	}
	entry := PartCatalogEntry{
		Name:       name,
		Location:   location,
		Manifest:   PartManifest{Checksum: checksum, Columns: make([]PartColumnChecksum, int(columnCount))},
		Generation: generation,
	}
	for index := range entry.Manifest.Columns {
		columnName, err := decoder.string()
		if err != nil {
			return PartCatalogEntry{}, err
		}
		offset, err := decoder.uint64()
		if err != nil {
			return PartCatalogEntry{}, err
		}
		size, err := decoder.uint64()
		if err != nil {
			return PartCatalogEntry{}, err
		}
		columnChecksum, err := decoder.checksum()
		if err != nil {
			return PartCatalogEntry{}, err
		}
		entry.Manifest.Columns[index] = PartColumnChecksum{
			Name:     columnName,
			Offset:   offset,
			Size:     size,
			Checksum: columnChecksum,
		}
	}
	if includeDeleteBitmap {
		present, err := decoder.byte()
		if err != nil {
			return PartCatalogEntry{}, err
		}
		if present > 1 {
			return PartCatalogEntry{}, partCatalogPersistenceError("delete bitmap flag is invalid")
		}
		if present == 1 {
			rowCount, err := decoder.uint64()
			if err != nil {
				return PartCatalogEntry{}, err
			}
			deletedCount, err := decoder.uint64()
			if err != nil {
				return PartCatalogEntry{}, err
			}
			snapshot, err := decoder.checksum()
			if err != nil {
				return PartCatalogEntry{}, err
			}
			bitmap := PartDeleteBitmap{RowCount: rowCount, DeletedCount: deletedCount, Snapshot: snapshot}
			entry.Manifest.DeleteBitmap = &bitmap
		}
	}
	if generation == 0 || generation > catalogGeneration {
		return PartCatalogEntry{}, partCatalogPersistenceError("entry generation is outside catalog generation")
	}
	if err := validatePartCatalogPersistenceEntry(entry); err != nil {
		return PartCatalogEntry{}, err
	}
	return entry, nil
}

func (decoder *partCatalogPersistenceDecoder) done() bool {
	return decoder.offset == len(decoder.data)
}

func validatePartCatalogPersistenceEntry(entry PartCatalogEntry) error {
	if entry.Name == "" || strings.TrimSpace(entry.Name) != entry.Name {
		return partCatalogPersistenceError("entry name is not canonical")
	}
	if len(entry.Name) > partCatalogPersistenceMaxStringBytes || len(entry.Location) > partCatalogPersistenceMaxStringBytes {
		return partCatalogPersistenceError("entry string exceeds %d bytes", partCatalogPersistenceMaxStringBytes)
	}
	if entry.Generation == 0 {
		return partCatalogPersistenceError("entry generation is zero")
	}
	if len(entry.Manifest.Columns) > partCatalogPersistenceMaxColumns {
		return partCatalogPersistenceError("column count exceeds %d", partCatalogPersistenceMaxColumns)
	}
	if entry.Manifest.DeleteBitmap != nil {
		if err := entry.Manifest.DeleteBitmap.Validate(); err != nil {
			return err
		}
	}
	partSize := entry.Manifest.Checksum.Size
	for index, column := range entry.Manifest.Columns {
		if strings.TrimSpace(column.Name) == "" || len(column.Name) > partCatalogPersistenceMaxStringBytes {
			return partCatalogPersistenceError("column %d name is invalid", index)
		}
		if column.Offset > partSize || column.Size > partSize-column.Offset || column.Checksum.Size != column.Size {
			return partCatalogPersistenceError("column %d range or checksum size is invalid", index)
		}
		for previous := 0; previous < index; previous++ {
			other := entry.Manifest.Columns[previous]
			if other.Name == column.Name {
				return partCatalogPersistenceError("duplicate column name %q", column.Name)
			}
			if partCatalogColumnRangesOverlap(other, column) {
				return partCatalogPersistenceError("overlapping columns %q and %q", other.Name, column.Name)
			}
		}
	}
	return nil
}

func partCatalogColumnRangesOverlap(left, right PartColumnChecksum) bool {
	if left.Size == 0 || right.Size == 0 {
		return false
	}
	if left.Offset <= right.Offset {
		return right.Offset-left.Offset < left.Size
	}
	return left.Offset-right.Offset < right.Size
}

func wrapPartCatalogPersistencePayload(payload []byte) ([]byte, error) {
	if len(payload) > partCatalogPersistenceMaxBytes-partCatalogPersistenceHeaderBytes {
		return nil, partCatalogPersistenceError("payload exceeds %d bytes", partCatalogPersistenceMaxBytes-partCatalogPersistenceHeaderBytes)
	}
	data := make([]byte, partCatalogPersistenceHeaderBytes+len(payload))
	copy(data[:4], partCatalogPersistenceMagic[:])
	binary.LittleEndian.PutUint16(data[4:6], partCatalogPersistenceVersion)
	binary.LittleEndian.PutUint32(data[8:12], uint32(len(payload)))
	binary.LittleEndian.PutUint32(data[12:16], crc32.Checksum(payload, partCatalogPersistenceCRC32CTable))
	copy(data[partCatalogPersistenceHeaderBytes:], payload)
	return data, nil
}

func unwrapPartCatalogPersistencePayload(data []byte) ([]byte, uint16, error) {
	if string(data[:4]) != string(partCatalogPersistenceMagic[:]) {
		return nil, 0, partCatalogPersistenceError("magic is invalid")
	}
	version := binary.LittleEndian.Uint16(data[4:6])
	if version != partCatalogPersistenceLegacyVersion && version != partCatalogPersistenceVersion {
		return nil, 0, partCatalogPersistenceError("version is unsupported")
	}
	if binary.LittleEndian.Uint16(data[6:8]) != 0 {
		return nil, 0, partCatalogPersistenceError("flags are unsupported")
	}
	payloadLength := binary.LittleEndian.Uint32(data[8:12])
	if uint64(payloadLength) != uint64(len(data)-partCatalogPersistenceHeaderBytes) {
		return nil, 0, partCatalogPersistenceError("payload length does not match file length")
	}
	payload := data[partCatalogPersistenceHeaderBytes:]
	if crc32.Checksum(payload, partCatalogPersistenceCRC32CTable) != binary.LittleEndian.Uint32(data[12:16]) {
		return nil, 0, partCatalogPersistenceError("payload checksum mismatch")
	}
	return payload, version, nil
}

func partCatalogPersistenceError(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", ErrPartCatalogSerializationInvalid, fmt.Sprintf(format, arguments...))
}

func writePartCatalogCheckpoint(file *os.File, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)
		if err != nil {
			return fmt.Errorf("write part catalog checkpoint: %w", err)
		}
		if written == 0 {
			return errors.New("hatriecache: zero-byte part catalog checkpoint write")
		}
		data = data[written:]
	}
	return nil
}

func syncPartCatalogDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
