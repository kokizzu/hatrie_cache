package hatCache

import (
	"bytes"
	"errors"
)

const commandDumpColdBorrowMinimumRecordBytes = 4 << 10

func (ht *HatTrie) appendCommandDumpColdBinaryEntryLocked(destination []byte, entry Entry) ([]byte, bool, error) {
	if entry.Value.Type() != DATAVALUE_TYPE_LEVELDB_REF {
		return destination, false, nil
	}
	ref, ok := ht.dbrefs.Get(entry.Value.Index)
	if !ok || ref.Store == nil || ref.Key != entry.Key || ref.RecordBytes <= 0 {
		return destination, false, nil
	}
	switch ref.Type {
	case "counter", "string", "bytes",
		"bloom_filter", "count_min_sketch", "hyperloglog", "cuckoo_filter",
		"roaring_bitmap", "sparse_bitset", "fenwick_tree", "quantile_sketch":
	default:
		return destination, false, nil
	}
	if ref.RecordBytes >= commandDumpColdBorrowMinimumRecordBytes {
		if borrower, ok := ref.Store.(persistentReferenceStoreBorrower); ok {
			result, handled, err := borrower.transformEntryData(ref.Key, commandDumpColdBinaryRecordTransformer{
				trie:        ht,
				destination: destination,
				entry:       entry,
				ref:         ref,
			})
			if err != nil {
				return destination, true, err
			}
			if !handled {
				return destination, false, nil
			}
			return result, handled, nil
		}
	}
	data, found, err := ref.Store.entryData(ref.Key)
	if err != nil {
		return destination, true, err
	}
	if !found {
		return destination, false, nil
	}
	return ht.appendCommandDumpColdBinaryRecordLocked(destination, entry, ref, data)
}

func (ht *HatTrie) appendCommandDumpColdBinaryRecordLocked(destination []byte, entry Entry, ref LevelDBReference, data []byte) ([]byte, bool, error) {
	if len(data) != ref.RecordBytes || levelDBRecordChecksum(data) != ref.RecordChecksum {
		return destination, false, nil
	}
	valueStart, valueEnd, ok, err := levelDBBinaryReplicationValueRange(data, ref.Key, ref.Type)
	if err != nil {
		return destination, true, err
	}
	if !ok {
		return destination, false, nil
	}
	expiresAt := snapshotExpiresAt(ht.expirationTimeLocked(entry.Key))
	capacity, err := replicationValueBinaryCapacity(ref.Type, int64(valueEnd-valueStart), expiresAt)
	if err != nil {
		return destination, true, err
	}
	destination = growBinaryAppendBuffer(destination, capacity)
	writer := levelDBBinaryWriter{binaryFieldWriter: binaryFieldWriter{buf: destination}}
	writer.buf = append(writer.buf, replicationValueBinaryMagic...)
	writer.writeString(ref.Type)
	writer.buf = append(writer.buf, data[valueStart:valueEnd]...)
	writer.writeTimePtr(expiresAt)
	return writer.bytes(), true, nil
}

type commandDumpColdBinaryRecordTransformer struct {
	trie        *HatTrie
	destination []byte
	entry       Entry
	ref         LevelDBReference
}

func (transformer commandDumpColdBinaryRecordTransformer) transformPersistentReferenceEntry(data []byte) ([]byte, bool, error) {
	return transformer.trie.appendCommandDumpColdBinaryRecordLocked(transformer.destination, transformer.entry, transformer.ref, data)
}

func levelDBBinaryReplicationValueRange(data []byte, key string, entryType string) (int, int, bool, error) {
	if !levelDBEntryDataIsBinary(data) {
		return 0, 0, false, nil
	}
	reader := levelDBBinaryReader{binaryFieldReader: newBinaryFieldReader(data[len(levelDBBinaryMagic):])}
	entryKey, err := reader.readBytes()
	if err != nil {
		return 0, 0, true, err
	}
	if !bytes.Equal(entryKey, []byte(key)) {
		return 0, 0, false, nil
	}
	encodedType, err := reader.readBytes()
	if err != nil {
		return 0, 0, true, err
	}
	if !bytes.Equal(encodedType, []byte(entryType)) {
		return 0, 0, false, nil
	}
	valueStart := len(levelDBBinaryMagic) + reader.off
	if err := reader.skipSnapshotEntryValue(entryType); err != nil {
		return 0, 0, true, err
	}
	valueEnd := len(levelDBBinaryMagic) + reader.off
	if err := reader.skipCommandDumpTimePtr(); err != nil {
		return 0, 0, true, err
	}
	if err := reader.skipCommandDumpKeyStatsPtr(); err != nil {
		return 0, 0, true, err
	}
	if !reader.done() {
		return 0, 0, true, errors.New("hatriecache: invalid trailing binary leveldb entry data")
	}
	return valueStart, valueEnd, true, nil
}

func (reader *levelDBBinaryReader) skipCommandDumpTimePtr() error {
	present, err := reader.readBool()
	if err != nil || !present {
		return err
	}
	_, err = reader.readVarint()
	return err
}

func (reader *levelDBBinaryReader) skipCommandDumpTime() error {
	return reader.skipCommandDumpTimePtr()
}

func (reader *levelDBBinaryReader) skipCommandDumpKeyStatsPtr() error {
	present, err := reader.readBool()
	if err != nil || !present {
		return err
	}
	for index := 0; index < 4; index++ {
		if _, err := reader.readUvarint(); err != nil {
			return err
		}
	}
	for index := 0; index < 3; index++ {
		if err := reader.skipCommandDumpTime(); err != nil {
			return err
		}
	}
	_, err = reader.readFloat64()
	return err
}
