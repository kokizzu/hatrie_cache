package hatSql

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var (
	// ErrQuerySubscriptionProgressFrameInvalid indicates that a data batch or
	// unsupported metadata was supplied where a progress-only frame is needed.
	ErrQuerySubscriptionProgressFrameInvalid = errors.New("hatSql: query subscription progress frame is invalid")
	// ErrQuerySubscriptionProgressFrameCorrupt indicates malformed or modified
	// QPF1 bytes.
	ErrQuerySubscriptionProgressFrameCorrupt = errors.New("hatSql: query subscription progress frame is corrupt")
)

const (
	querySubscriptionProgressFrameVersion  = byte(1)
	querySubscriptionProgressFrameComplete = byte(1 << 0)
	querySubscriptionProgressFrameMaxBytes = 64
)

var querySubscriptionProgressFrameCRCTable = crc32.MakeTable(crc32.Castagnoli)

// EncodeQuerySubscriptionProgressFrame encodes a progress-only subscription
// batch as a deterministic compact QPF1 frame. The frame carries identity,
// revision, frontier, and completion metadata; it never carries row data.
func EncodeQuerySubscriptionProgressFrame(batch QuerySubscriptionDeltaBatch) ([]byte, error) {
	if !batch.Progress || batch.Reset || len(batch.Columns) != 0 || len(batch.Deltas) != 0 {
		return nil, ErrQuerySubscriptionProgressFrameInvalid
	}
	payload := make([]byte, 0, 32)
	payload = append(payload, 'Q', 'P', 'F', '1', querySubscriptionProgressFrameVersion)
	flags := byte(0)
	if batch.Complete {
		flags |= querySubscriptionProgressFrameComplete
	}
	payload = append(payload, flags)
	payload = appendQuerySubscriptionProgressFrameUvarint(payload, batch.ID)
	payload = appendQuerySubscriptionProgressFrameUvarint(payload, batch.Revision)
	payload = appendQuerySubscriptionProgressFrameUvarint(payload, batch.Frontier)
	checksum := crc32.Checksum(payload, querySubscriptionProgressFrameCRCTable)
	var checksumBytes [4]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	payload = append(payload, checksumBytes[:]...)
	return payload, nil
}

// DecodeQuerySubscriptionProgressFrame decodes a bounded QPF1 progress-only
// frame. The returned batch has Progress set and no row or column payload.
func DecodeQuerySubscriptionProgressFrame(encoded []byte) (QuerySubscriptionDeltaBatch, error) {
	if len(encoded) > querySubscriptionProgressFrameMaxBytes || len(encoded) < 10 {
		return QuerySubscriptionDeltaBatch{}, ErrQuerySubscriptionProgressFrameCorrupt
	}
	body := encoded[:len(encoded)-4]
	if string(body[:4]) != "QPF1" || body[4] != querySubscriptionProgressFrameVersion {
		return QuerySubscriptionDeltaBatch{}, ErrQuerySubscriptionProgressFrameCorrupt
	}
	if body[5]&^querySubscriptionProgressFrameComplete != 0 {
		return QuerySubscriptionDeltaBatch{}, ErrQuerySubscriptionProgressFrameCorrupt
	}
	wireChecksum := binary.LittleEndian.Uint32(encoded[len(encoded)-4:])
	if crc32.Checksum(body, querySubscriptionProgressFrameCRCTable) != wireChecksum {
		return QuerySubscriptionDeltaBatch{}, ErrQuerySubscriptionProgressFrameCorrupt
	}
	reader := querySubscriptionProgressFrameReader{data: body, offset: 6}
	id, ok := reader.readUvarint()
	if !ok {
		return QuerySubscriptionDeltaBatch{}, ErrQuerySubscriptionProgressFrameCorrupt
	}
	revision, ok := reader.readUvarint()
	if !ok {
		return QuerySubscriptionDeltaBatch{}, ErrQuerySubscriptionProgressFrameCorrupt
	}
	frontier, ok := reader.readUvarint()
	if !ok || reader.offset != len(body) {
		return QuerySubscriptionDeltaBatch{}, ErrQuerySubscriptionProgressFrameCorrupt
	}
	return QuerySubscriptionDeltaBatch{
		ID:       id,
		Revision: revision,
		Frontier: frontier,
		Progress: true,
		Complete: body[5]&querySubscriptionProgressFrameComplete != 0,
	}, nil
}

func appendQuerySubscriptionProgressFrameUvarint(destination []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], value)
	return append(destination, encoded[:length]...)
}

type querySubscriptionProgressFrameReader struct {
	data   []byte
	offset int
}

func (reader *querySubscriptionProgressFrameReader) readUvarint() (uint64, bool) {
	value, length := binary.Uvarint(reader.data[reader.offset:])
	if length <= 0 {
		return 0, false
	}
	reader.offset += length
	return value, true
}
