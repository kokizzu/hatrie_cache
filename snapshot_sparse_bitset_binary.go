package hatriecache

import (
	"encoding/base64"
	"errors"
	"io"
)

const (
	snapshotSparseBitsetBinaryArray byte = iota
	snapshotSparseBitsetBinaryBits
)

type snapshotSparseBitsetBinaryContainer struct {
	key         uint64
	kind        byte
	cardinality uint32
	payload     []byte
}

func marshalSnapshotSparseBitsetValueBinary(snapshot sparseBitsetSnapshot) ([]byte, error) {
	containers, err := prepareSnapshotSparseBitsetBinaryContainers(snapshot)
	if err != nil {
		return nil, err
	}
	size, err := snapshotValueBinarySparseBitsetSize(snapshot, containers)
	if err != nil {
		return nil, err
	}
	writer := newBinaryFieldWriter(snapshotValueBinaryMagic, len(snapshotValueBinaryMagic)+size)
	writeSnapshotValueBinarySparseBitset(&writer, snapshot, containers)
	return writer.bytes(), nil
}

func prepareSnapshotSparseBitsetBinaryContainers(snapshot sparseBitsetSnapshot) ([]snapshotSparseBitsetBinaryContainer, error) {
	containers := make([]snapshotSparseBitsetBinaryContainer, 0, len(snapshot.Containers))
	for _, container := range snapshot.Containers {
		out := snapshotSparseBitsetBinaryContainer{
			key:         container.Key,
			cardinality: container.Cardinality,
		}
		switch container.Kind {
		case sparseBitsetContainerKindArray:
			payload, err := base64.StdEncoding.DecodeString(container.Values)
			if err != nil {
				return nil, err
			}
			out.kind = snapshotSparseBitsetBinaryArray
			out.payload = payload
		case sparseBitsetContainerKindBits:
			payload, err := base64.StdEncoding.DecodeString(container.Bits)
			if err != nil {
				return nil, err
			}
			out.kind = snapshotSparseBitsetBinaryBits
			out.payload = payload
		default:
			return nil, errors.New("hatriecache: unsupported sparse bitset container kind")
		}
		containers = append(containers, out)
	}
	return containers, nil
}

func snapshotValueBinarySparseBitsetSize(snapshot sparseBitsetSnapshot, containers []snapshotSparseBitsetBinaryContainer) (int, error) {
	total := 1 + binaryUvarintSize(snapshot.Cardinality) + binaryUvarintSize(uint64(len(containers)))
	for _, container := range containers {
		payloadSize, err := snapshotValueBinaryBytesSize(len(container.payload))
		if err != nil {
			return 0, err
		}
		itemSize := binaryUvarintSize(container.key) +
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

func writeSnapshotValueBinarySparseBitset(writer *binaryFieldWriter, snapshot sparseBitsetSnapshot, containers []snapshotSparseBitsetBinaryContainer) {
	writer.buf = append(writer.buf, snapshotValueBinarySparseBitset)
	writer.writeUvarint(snapshot.Cardinality)
	writer.writeUvarint(uint64(len(containers)))
	for _, container := range containers {
		writer.writeUvarint(container.key)
		writer.buf = append(writer.buf, container.kind)
		writer.writeUvarint(uint64(container.cardinality))
		writer.writeBytes(container.payload)
	}
}

func readSnapshotValueBinarySparseBitset(reader *binaryFieldReader) (interface{}, error) {
	cardinality, err := reader.readUvarint()
	if err != nil {
		return nil, err
	}
	count, err := reader.readUvarint()
	if err != nil {
		return nil, err
	}
	if count > sparseBitsetMaxContainerCount {
		return nil, errors.New("hatriecache: binary sparse bitset container count is too large")
	}
	capacity, err := snapshotValueBinaryInitialCapacity(count, len(reader.data)-reader.off, 4)
	if err != nil {
		return nil, err
	}
	containers := make([]sparseBitsetContainerSnapshot, 0, capacity)
	for idx := 0; idx < int(count); idx++ {
		key, err := reader.readUvarint()
		if err != nil {
			return nil, err
		}
		if key > sparseBitsetMaxContainerKey {
			return nil, errors.New("hatriecache: binary sparse bitset container key is out of range")
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
			return nil, errors.New("hatriecache: binary sparse bitset container cardinality is too large")
		}
		payload, err := reader.readBytes()
		if err != nil {
			return nil, err
		}
		container := sparseBitsetContainerSnapshot{
			Key:         key,
			Cardinality: uint32(itemCardinality),
		}
		switch kind {
		case snapshotSparseBitsetBinaryArray:
			container.Kind = sparseBitsetContainerKindArray
			container.Values = base64.StdEncoding.EncodeToString(payload)
		case snapshotSparseBitsetBinaryBits:
			container.Kind = sparseBitsetContainerKindBits
			container.Bits = base64.StdEncoding.EncodeToString(payload)
		default:
			return nil, errors.New("hatriecache: unsupported binary sparse bitset container kind")
		}
		containers = append(containers, container)
	}
	return sparseBitsetSnapshot{
		Cardinality: cardinality,
		Containers:  containers,
	}, nil
}
