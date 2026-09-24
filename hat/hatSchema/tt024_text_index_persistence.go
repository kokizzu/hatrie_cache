package hatSchema

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
)

const (
	textIndexWireMagic          = "HTI1"
	textIndexWireMaxBytes       = 64 << 20
	textIndexWireMaxFieldBytes  = 1 << 20
	textIndexWireMaxTokens      = 1 << 20
	textIndexWireMaxPostings    = 8 << 20
	textIndexWireMaxPositions   = 32 << 20
	textIndexWireMaxSourceRows  = 1 << 27
	textIndexWireChecksumLength = 4
)

var (
	// ErrTextIndexPersistenceNotIndexed reports a snapshot request for a
	// field without an active positional text index.
	ErrTextIndexPersistenceNotIndexed = errors.New("hatSchema: text index is not built")
	// ErrTextIndexPersistenceInvalid reports malformed or incompatible bytes.
	ErrTextIndexPersistenceInvalid = errors.New("hatSchema: text index persistence frame is invalid")
	// ErrTextIndexPersistenceChecksum reports a corrupt persistence frame.
	ErrTextIndexPersistenceChecksum = errors.New("hatSchema: text index persistence checksum mismatch")
	// ErrTextIndexPersistenceSourceMismatch reports a frame built from another
	// source snapshot or field.
	ErrTextIndexPersistenceSourceMismatch = errors.New("hatSchema: text index persistence source mismatch")
)

var textIndexPersistenceCRC = crc32.MakeTable(crc32.Castagnoli)

// MarshalTextIndex encodes one maintained positional text index as a bounded,
// deterministic HTI1 frame. The frame includes the indexed field, row count,
// source-field digest, sorted token postings, positions, and a CRC32C trailer.
func (source *MaterializedSource) MarshalTextIndex(field string) ([]byte, error) {
	if source == nil {
		return nil, ErrMaterializedSourceNil
	}
	field = strings.TrimSpace(field)
	if field == "" {
		return nil, ErrMaterializedSourceColumnRequired
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	index := source.textIndexes[field]
	if index == nil {
		return nil, fmt.Errorf("%w: %s", ErrTextIndexPersistenceNotIndexed, field)
	}
	if len(field) > textIndexWireMaxFieldBytes || len(source.rows) > textIndexWireMaxSourceRows {
		return nil, ErrTextIndexPersistenceInvalid
	}
	tokens := make([]string, 0, len(index.postings))
	for token := range index.postings {
		tokens = append(tokens, token)
	}
	sort.Strings(tokens)
	if len(tokens) > textIndexWireMaxTokens || index.postingCount > textIndexWireMaxPostings {
		return nil, ErrTextIndexPersistenceInvalid
	}

	frame := make([]byte, 0, len(tokens)*16+index.postingCount*12+len(field)+sha256.Size+len(textIndexWireMagic)+64)
	frame = append(frame, textIndexWireMagic...)
	frame = appendTextIndexUvarint(frame, uint64(len(field)))
	frame = append(frame, field...)
	frame = appendTextIndexUvarint(frame, uint64(len(source.rows)))
	digest := materializedTextIndexSourceDigestLocked(source, field)
	frame = append(frame, digest[:]...)
	frame = appendTextIndexUvarint(frame, uint64(len(tokens)))
	frame = appendTextIndexUvarint(frame, uint64(index.postingCount))
	for _, token := range tokens {
		postings := index.postings[token]
		frame = appendTextIndexUvarint(frame, uint64(len(token)))
		frame = append(frame, token...)
		frame = appendTextIndexUvarint(frame, uint64(len(postings)))
		var previousRow int
		for postingIndex, posting := range postings {
			if posting.row < 0 || (postingIndex > 0 && posting.row <= previousRow) {
				return nil, ErrTextIndexPersistenceInvalid
			}
			if postingIndex == 0 {
				frame = appendTextIndexUvarint(frame, uint64(posting.row))
			} else {
				frame = appendTextIndexUvarint(frame, uint64(posting.row-previousRow))
			}
			previousRow = posting.row
			if len(posting.positions) == 0 {
				return nil, ErrTextIndexPersistenceInvalid
			}
			frame = appendTextIndexUvarint(frame, uint64(len(posting.positions)))
			var previousPosition int
			for positionIndex, position := range posting.positions {
				if position < 0 || (positionIndex > 0 && position <= previousPosition) {
					return nil, ErrTextIndexPersistenceInvalid
				}
				if positionIndex == 0 {
					frame = appendTextIndexUvarint(frame, uint64(position))
				} else {
					frame = appendTextIndexUvarint(frame, uint64(position-previousPosition))
				}
				previousPosition = position
			}
		}
	}
	if len(frame)+textIndexWireChecksumLength > textIndexWireMaxBytes {
		return nil, ErrTextIndexPersistenceInvalid
	}
	checksum := crc32.Checksum(frame, textIndexPersistenceCRC)
	var checksumBytes [textIndexWireChecksumLength]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	frame = append(frame, checksumBytes[:]...)
	return frame, nil
}

// RestoreTextIndex validates and installs one HTI1 frame. The source rows are
// never changed; a row-count and field digest check prevents stale postings
// from becoming active. The restored index remains maintained by later source
// inserts and upserts.
func (source *MaterializedSource) RestoreTextIndex(field string, frame []byte) error {
	if source == nil {
		return ErrMaterializedSourceNil
	}
	field = strings.TrimSpace(field)
	if field == "" {
		return ErrMaterializedSourceColumnRequired
	}
	decoded, err := decodeTextIndexPersistence(frame)
	if err != nil {
		return err
	}
	if decoded.field != field {
		return fmt.Errorf("%w: frame field %q, requested %q", ErrTextIndexPersistenceSourceMismatch, decoded.field, field)
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if !source.hasColumnLocked(field) {
		return fmt.Errorf("%w: %s", ErrMaterializedSourceColumnUnknown, field)
	}
	if decoded.rowCount != len(source.rows) || decoded.digest != materializedTextIndexSourceDigestLocked(source, field) {
		return fmt.Errorf("%w: field %s", ErrTextIndexPersistenceSourceMismatch, field)
	}
	if source.textIndexes == nil {
		source.textIndexes = make(map[string]*materializedTextIndex)
	}
	source.textIndexes[field] = decoded.index
	return nil
}

type decodedTextIndexPersistence struct {
	field    string
	rowCount int
	digest   [sha256.Size]byte
	index    *materializedTextIndex
}

func decodeTextIndexPersistence(frame []byte) (decodedTextIndexPersistence, error) {
	var decoded decodedTextIndexPersistence
	if len(frame) < len(textIndexWireMagic)+textIndexWireChecksumLength || len(frame) > textIndexWireMaxBytes {
		return decoded, ErrTextIndexPersistenceInvalid
	}
	payloadLength := len(frame) - textIndexWireChecksumLength
	if string(frame[:len(textIndexWireMagic)]) != textIndexWireMagic {
		return decoded, ErrTextIndexPersistenceInvalid
	}
	expected := binary.LittleEndian.Uint32(frame[payloadLength:])
	if crc32.Checksum(frame[:payloadLength], textIndexPersistenceCRC) != expected {
		return decoded, ErrTextIndexPersistenceChecksum
	}
	payload := frame[:payloadLength]
	offset := len(textIndexWireMagic)
	field, err := readTextIndexString(payload, &offset, textIndexWireMaxFieldBytes)
	if err != nil {
		return decoded, err
	}
	rowCount, err := readTextIndexUvarint(payload, &offset)
	if err != nil || rowCount > textIndexWireMaxSourceRows {
		return decoded, ErrTextIndexPersistenceInvalid
	}
	if len(payload)-offset < sha256.Size {
		return decoded, ErrTextIndexPersistenceInvalid
	}
	copy(decoded.digest[:], payload[offset:offset+sha256.Size])
	offset += sha256.Size
	tokenCount, err := readTextIndexUvarint(payload, &offset)
	if err != nil || tokenCount > textIndexWireMaxTokens || tokenCount > uint64(len(payload)-offset) {
		return decoded, ErrTextIndexPersistenceInvalid
	}
	declaredPostings, err := readTextIndexUvarint(payload, &offset)
	if err != nil || declaredPostings > textIndexWireMaxPostings || declaredPostings > uint64(len(payload)-offset) {
		return decoded, ErrTextIndexPersistenceInvalid
	}
	index := &materializedTextIndex{postings: make(map[string][]materializedTextPosting, int(tokenCount))}
	var previousToken string
	var totalPostings uint64
	var totalPositions uint64
	for tokenIndex := uint64(0); tokenIndex < tokenCount; tokenIndex++ {
		token, readErr := readTextIndexString(payload, &offset, textIndexWireMaxFieldBytes)
		if readErr != nil || token == "" || tokenIndex > 0 && token <= previousToken {
			return decoded, ErrTextIndexPersistenceInvalid
		}
		previousToken = token
		postingCount, readErr := readTextIndexUvarint(payload, &offset)
		if readErr != nil || postingCount == 0 || postingCount > textIndexWireMaxPostings || postingCount > uint64(len(payload)-offset) {
			return decoded, ErrTextIndexPersistenceInvalid
		}
		postings := make([]materializedTextPosting, 0, int(postingCount))
		var previousRow uint64
		for postingIndex := uint64(0); postingIndex < postingCount; postingIndex++ {
			rowDelta, readErr := readTextIndexUvarint(payload, &offset)
			if readErr != nil || postingIndex > 0 && rowDelta == 0 {
				return decoded, ErrTextIndexPersistenceInvalid
			}
			row := rowDelta
			if postingIndex > 0 {
				if ^uint64(0)-previousRow < rowDelta {
					return decoded, ErrTextIndexPersistenceInvalid
				}
				row += previousRow
			}
			if row >= rowCount {
				return decoded, ErrTextIndexPersistenceInvalid
			}
			positionCount, readErr := readTextIndexUvarint(payload, &offset)
			if readErr != nil || positionCount == 0 || positionCount > textIndexWireMaxPositions || positionCount > uint64(len(payload)-offset) {
				return decoded, ErrTextIndexPersistenceInvalid
			}
			positions := make([]int, 0, int(positionCount))
			var previousPosition uint64
			for positionIndex := uint64(0); positionIndex < positionCount; positionIndex++ {
				positionDelta, readErr := readTextIndexUvarint(payload, &offset)
				if readErr != nil || positionIndex > 0 && positionDelta == 0 {
					return decoded, ErrTextIndexPersistenceInvalid
				}
				position := positionDelta
				if positionIndex > 0 {
					if ^uint64(0)-previousPosition < positionDelta {
						return decoded, ErrTextIndexPersistenceInvalid
					}
					position += previousPosition
				}
				if position > uint64(^uint(0)>>1) {
					return decoded, ErrTextIndexPersistenceInvalid
				}
				positions = append(positions, int(position))
				previousPosition = position
				totalPositions++
				if totalPositions > textIndexWireMaxPositions {
					return decoded, ErrTextIndexPersistenceInvalid
				}
			}
			if row > uint64(^uint(0)>>1) {
				return decoded, ErrTextIndexPersistenceInvalid
			}
			postings = append(postings, materializedTextPosting{row: int(row), positions: positions})
			previousRow = row
			totalPostings++
			if totalPostings > textIndexWireMaxPostings {
				return decoded, ErrTextIndexPersistenceInvalid
			}
		}
		index.postings[token] = postings
	}
	if totalPostings != declaredPostings || offset != len(payload) {
		return decoded, ErrTextIndexPersistenceInvalid
	}
	index.postingCount = int(totalPostings)
	decoded.field = field
	decoded.rowCount = int(rowCount)
	decoded.index = index
	return decoded, nil
}

func materializedTextIndexSourceDigestLocked(source *MaterializedSource, field string) [sha256.Size]byte {
	digest := sha256.New()
	var scratch [binary.MaxVarintLen64]byte
	for position, row := range source.rows {
		n := binary.PutUvarint(scratch[:], uint64(position))
		_, _ = digest.Write(scratch[:n])
		key := materializedIndexKey(row[field])
		n = binary.PutUvarint(scratch[:], uint64(len(key)))
		_, _ = digest.Write(scratch[:n])
		_, _ = digest.Write([]byte(key))
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func appendTextIndexUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func readTextIndexUvarint(payload []byte, offset *int) (uint64, error) {
	if offset == nil || *offset < 0 || *offset >= len(payload) {
		return 0, ErrTextIndexPersistenceInvalid
	}
	value, length := binary.Uvarint(payload[*offset:])
	if length <= 0 {
		return 0, ErrTextIndexPersistenceInvalid
	}
	*offset += length
	return value, nil
}

func readTextIndexString(payload []byte, offset *int, maxLength int) (string, error) {
	length, err := readTextIndexUvarint(payload, offset)
	if err != nil || length > uint64(maxLength) || length > uint64(len(payload)-*offset) {
		return "", ErrTextIndexPersistenceInvalid
	}
	start := *offset
	*offset += int(length)
	return string(payload[start:*offset]), nil
}
