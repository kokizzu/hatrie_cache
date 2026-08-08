package hatriecache

import (
	"errors"
	"time"
)

func appendCommandDumpCounterBinary(destination []byte, expiresAt *time.Time, value int32) ([]byte, error) {
	encodedSize := int64(binaryVarintSize(int64(value)))
	capacity, err := replicationValueBinaryCapacity("counter", encodedSize, expiresAt)
	if err != nil {
		return destination, err
	}
	destination = growBinaryAppendBuffer(destination, capacity)
	writer := levelDBBinaryWriter{binaryFieldWriter: binaryFieldWriter{buf: destination}}
	writer.buf = append(writer.buf, replicationValueBinaryMagic...)
	writer.writeString("counter")
	writer.writeVarint(int64(value))
	writer.writeTimePtr(expiresAt)
	return writer.bytes(), nil
}

func (ht *HatTrie) appendCommandDumpBytesBinaryLocked(destination []byte, entry Entry) ([]byte, error) {
	index := entry.Value.Index
	var value []byte
	var err error
	if entry.Value.OnDisk() {
		value, err = ht.disks.Get(index)
		if err != nil {
			return destination, err
		}
	} else {
		if index < 0 || int(index) >= len(ht.raws.array) || ht.raws.reusables.Has(index) {
			return destination, errors.New("hatriecache: bytes backing index is missing")
		}
		value = ht.raws.array[index]
	}
	expiresAt := snapshotExpiresAt(ht.expirationTimeLocked(entry.Key))
	encodedSize, err := binaryLengthPrefixedSize(int64(len(value)))
	if err != nil {
		return destination, err
	}
	capacity, err := replicationValueBinaryCapacity("bytes", encodedSize, expiresAt)
	if err != nil {
		return destination, err
	}
	destination = growBinaryAppendBuffer(destination, capacity)
	writer := levelDBBinaryWriter{binaryFieldWriter: binaryFieldWriter{buf: destination}}
	writer.buf = append(writer.buf, replicationValueBinaryMagic...)
	writer.writeString("bytes")
	writer.writeBytes(value)
	writer.writeTimePtr(expiresAt)
	return writer.bytes(), nil
}
