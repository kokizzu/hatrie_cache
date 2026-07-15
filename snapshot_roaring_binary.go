package hatriecache

import (
	"encoding/base64"
	"errors"
	"io"
)

const (
	snapshotRoaringBitmapBinaryArray byte = iota
	snapshotRoaringBitmapBinaryBits
)

type snapshotRoaringBitmapBinaryContainer struct {
	key         uint16
	kind        byte
	cardinality uint32
	payload     []byte
}

func marshalSnapshotRoaringBitmapValueBinary(snapshot roaringBitmapSnapshot) ([]byte, error) {
	containers, err := prepareSnapshotRoaringBitmapBinaryContainers(snapshot)
	if err != nil {
		return nil, err
	}
	size, err := snapshotValueBinaryRoaringBitmapSize(snapshot, containers)
	if err != nil {
		return nil, err
	}
	writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
	writeSnapshotValueBinaryRoaringBitmap(&writer, snapshot, containers)
	return writer.bytes(), nil
}

func prepareSnapshotRoaringBitmapBinaryContainers(snapshot roaringBitmapSnapshot) ([]snapshotRoaringBitmapBinaryContainer, error) {
	containers := make([]snapshotRoaringBitmapBinaryContainer, 0, len(snapshot.Containers))
	for _, container := range snapshot.Containers {
		out := snapshotRoaringBitmapBinaryContainer{
			key:         container.Key,
			cardinality: container.Cardinality,
		}
		switch container.Kind {
		case roaringBitmapContainerKindArray:
			payload, err := base64.StdEncoding.DecodeString(container.Values)
			if err != nil {
				return nil, err
			}
			out.kind = snapshotRoaringBitmapBinaryArray
			out.payload = payload
		case roaringBitmapContainerKindBits:
			payload, err := base64.StdEncoding.DecodeString(container.Bits)
			if err != nil {
				return nil, err
			}
			out.kind = snapshotRoaringBitmapBinaryBits
			out.payload = payload
		default:
			return nil, errors.New("hatriecache: unsupported roaring bitmap container kind")
		}
		containers = append(containers, out)
	}
	return containers, nil
}

func snapshotValueBinaryRoaringBitmapSize(snapshot roaringBitmapSnapshot, containers []snapshotRoaringBitmapBinaryContainer) (int, error) {
	total := 1 + binaryUvarintSize(snapshot.Cardinality) + binaryUvarintSize(uint64(len(containers)))
	for _, container := range containers {
		payloadSize, err := snapshotValueBinaryBytesSize(len(container.payload))
		if err != nil {
			return 0, err
		}
		itemSize := binaryUvarintSize(uint64(container.key)) +
			1 +
			binaryUvarintSize(uint64(container.cardinality)) +
			payloadSize
		total, err = snapshotValueBinaryAdd(total, itemSize)
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

func writeSnapshotValueBinaryRoaringBitmap(writer *binaryFieldWriter, snapshot roaringBitmapSnapshot, containers []snapshotRoaringBitmapBinaryContainer) {
	writer.buf = append(writer.buf, snapshotValueBinaryRoaringBitmap)
	writer.writeUvarint(snapshot.Cardinality)
	writer.writeUvarint(uint64(len(containers)))
	for _, container := range containers {
		writer.writeUvarint(uint64(container.key))
		writer.buf = append(writer.buf, container.kind)
		writer.writeUvarint(uint64(container.cardinality))
		writer.writeBytes(container.payload)
	}
}

func readSnapshotValueBinaryRoaringBitmap(reader *binaryFieldReader) (interface{}, error) {
	cardinality, err := reader.readUvarint()
	if err != nil {
		return nil, err
	}
	count, err := reader.readUvarint()
	if err != nil {
		return nil, err
	}
	if count > roaringBitmapMaxContainerCount {
		return nil, errors.New("hatriecache: binary roaring bitmap container count is too large")
	}
	capacity, err := snapshotValueBinaryInitialCapacity(count, len(reader.data)-reader.off, 4)
	if err != nil {
		return nil, err
	}
	containers := make([]roaringBitmapContainerSnapshot, 0, capacity)
	for idx := 0; idx < int(count); idx++ {
		key, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		if key > uint64(^uint16(0)) {
			return nil, errors.New("hatriecache: binary roaring bitmap container key is too large")
		}
		if reader.off >= len(reader.data) {
			return nil, io.ErrUnexpectedEOF
		}
		kind := reader.data[reader.off]
		reader.off++
		itemCardinality, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		if itemCardinality > uint64(^uint32(0)) {
			return nil, errors.New("hatriecache: binary roaring bitmap container cardinality is too large")
		}
		payload, err := reader.readBytes()
		if err != nil {
			return nil, err
		}
		container := roaringBitmapContainerSnapshot{
			Key:         uint16(key),
			Cardinality: uint32(itemCardinality),
		}
		switch kind {
		case snapshotRoaringBitmapBinaryArray:
			container.Kind = roaringBitmapContainerKindArray
			container.Values = base64.StdEncoding.EncodeToString(payload)
		case snapshotRoaringBitmapBinaryBits:
			container.Kind = roaringBitmapContainerKindBits
			container.Bits = base64.StdEncoding.EncodeToString(payload)
		default:
			return nil, errors.New("hatriecache: unsupported binary roaring bitmap container kind")
		}
		containers = append(containers, container)
	}
	return roaringBitmapSnapshot{
		Cardinality: cardinality,
		Containers:  containers,
	}, nil
}
