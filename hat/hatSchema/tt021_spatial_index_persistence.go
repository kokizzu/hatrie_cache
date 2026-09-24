package hatSchema

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"strings"

	"hatrie_cache/hat/hatDataStructure"
)

const (
	spatialIndexWireMagic          = "HSI1"
	spatialIndexWireMaxBytes       = 64 << 20
	spatialIndexWireMaxFieldBytes  = 1 << 20
	spatialIndexWireMaxRows        = 1 << 27
	spatialIndexWireChecksumLength = 4
)

var (
	// ErrSpatialIndexPersistenceNotIndexed reports a snapshot request without
	// an active coordinate-field spatial index.
	ErrSpatialIndexPersistenceNotIndexed = errors.New("hatSchema: spatial index is not built")
	// ErrSpatialIndexPersistenceInvalid reports malformed or incompatible bytes.
	ErrSpatialIndexPersistenceInvalid = errors.New("hatSchema: spatial index persistence frame is invalid")
	// ErrSpatialIndexPersistenceChecksum reports a corrupt persistence frame.
	ErrSpatialIndexPersistenceChecksum = errors.New("hatSchema: spatial index persistence checksum mismatch")
	// ErrSpatialIndexPersistenceSourceMismatch reports a frame built from a
	// different source snapshot or coordinate fields.
	ErrSpatialIndexPersistenceSourceMismatch = errors.New("hatSchema: spatial index persistence source mismatch")
)

var spatialIndexPersistenceCRC = crc32.MakeTable(crc32.Castagnoli)

type spatialIndexPersistencePoint struct {
	position  int
	latitude  float64
	longitude float64
}

type decodedSpatialIndexPersistence struct {
	latitudeField  string
	longitudeField string
	rowCount       int
	digest         [sha256.Size]byte
	points         []spatialIndexPersistencePoint
	fallback       []int
}

// MarshalSpatialIndex encodes one maintained point spatial index as a bounded,
// deterministic HSI1 frame. The frame stores source positions, normalized
// coordinate values, fallback positions, a source digest, and a CRC32C trailer.
func (source *MaterializedSource) MarshalSpatialIndex(latitudeField, longitudeField string) ([]byte, error) {
	if source == nil {
		return nil, ErrMaterializedSourceNil
	}
	latitudeField = strings.TrimSpace(latitudeField)
	longitudeField = strings.TrimSpace(longitudeField)
	if latitudeField == "" || longitudeField == "" {
		return nil, ErrMaterializedSourceColumnRequired
	}
	if latitudeField == longitudeField {
		return nil, fmt.Errorf("hatSchema: spatial index latitude and longitude fields must be distinct")
	}
	source.mu.RLock()
	defer source.mu.RUnlock()
	if source.spatialIndexes[spatialIndexKey(latitudeField, longitudeField)] == nil {
		return nil, fmt.Errorf("%w: %s,%s", ErrSpatialIndexPersistenceNotIndexed, latitudeField, longitudeField)
	}
	if len(source.rows) > spatialIndexWireMaxRows || len(latitudeField) > spatialIndexWireMaxFieldBytes || len(longitudeField) > spatialIndexWireMaxFieldBytes {
		return nil, ErrSpatialIndexPersistenceInvalid
	}
	points := make([]spatialIndexPersistencePoint, 0, len(source.rows))
	fallback := make([]int, 0)
	for position, row := range source.rows {
		latitude, longitude, ok := materializedSpatialPoint(row, latitudeField, longitudeField)
		if !ok {
			fallback = append(fallback, position)
			continue
		}
		points = append(points, spatialIndexPersistencePoint{position: position, latitude: latitude, longitude: longitude})
	}

	frame := make([]byte, 0, len(spatialIndexWireMagic)+len(latitudeField)+len(longitudeField)+len(source.rows)*24+sha256.Size+64)
	frame = append(frame, spatialIndexWireMagic...)
	frame = appendSpatialIndexString(frame, latitudeField)
	frame = appendSpatialIndexString(frame, longitudeField)
	frame = appendSpatialIndexUvarint(frame, uint64(len(source.rows)))
	digest := materializedSpatialIndexSourceDigestLocked(source, latitudeField, longitudeField)
	frame = append(frame, digest[:]...)
	frame = appendSpatialIndexUvarint(frame, uint64(len(points)))
	frame = appendSpatialIndexUvarint(frame, uint64(len(fallback)))
	frame = appendSpatialIndexPoints(frame, points)
	frame = appendSpatialIndexPositions(frame, fallback)
	if len(frame)+spatialIndexWireChecksumLength > spatialIndexWireMaxBytes {
		return nil, ErrSpatialIndexPersistenceInvalid
	}
	checksum := crc32.Checksum(frame, spatialIndexPersistenceCRC)
	var checksumBytes [spatialIndexWireChecksumLength]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	return append(frame, checksumBytes[:]...), nil
}

// RestoreSpatialIndex validates and installs one HSI1 frame. Source rows are
// never changed; field names, row count, source digest, coordinate classes,
// and coordinate bits are checked before publication. Later inserts and
// upserts continue maintaining the restored index.
func (source *MaterializedSource) RestoreSpatialIndex(latitudeField, longitudeField string, frame []byte) error {
	if source == nil {
		return ErrMaterializedSourceNil
	}
	latitudeField = strings.TrimSpace(latitudeField)
	longitudeField = strings.TrimSpace(longitudeField)
	if latitudeField == "" || longitudeField == "" {
		return ErrMaterializedSourceColumnRequired
	}
	if latitudeField == longitudeField {
		return fmt.Errorf("hatSchema: spatial index latitude and longitude fields must be distinct")
	}
	decoded, err := decodeSpatialIndexPersistence(frame)
	if err != nil {
		return err
	}
	if decoded.latitudeField != latitudeField || decoded.longitudeField != longitudeField {
		return fmt.Errorf("%w: frame fields %q,%q, requested %q,%q", ErrSpatialIndexPersistenceSourceMismatch, decoded.latitudeField, decoded.longitudeField, latitudeField, longitudeField)
	}

	source.mu.Lock()
	defer source.mu.Unlock()
	if !source.hasColumnLocked(latitudeField) {
		return fmt.Errorf("%w: %s", ErrMaterializedSourceColumnUnknown, latitudeField)
	}
	if !source.hasColumnLocked(longitudeField) {
		return fmt.Errorf("%w: %s", ErrMaterializedSourceColumnUnknown, longitudeField)
	}
	if decoded.rowCount != len(source.rows) {
		return fmt.Errorf("%w: fields %s,%s", ErrSpatialIndexPersistenceSourceMismatch, latitudeField, longitudeField)
	}

	tree, err := hatDataStructure.NewRTree(0)
	if err != nil {
		return err
	}
	fallback := make(map[int]struct{}, len(decoded.fallback))
	pointIndex := 0
	fallbackIndex := 0
	digest := sha256.New()
	var scratch [binary.MaxVarintLen64]byte
	for position, row := range source.rows {
		n := binary.PutUvarint(scratch[:], uint64(position))
		_, _ = digest.Write(scratch[:n])
		latitude, longitude, indexed := materializedSpatialPoint(row, latitudeField, longitudeField)
		if !indexed {
			scratch[0] = 0
			_, _ = digest.Write(scratch[:1])
		} else {
			scratch[0] = 1
			_, _ = digest.Write(scratch[:1])
			binary.LittleEndian.PutUint64(scratch[:8], math.Float64bits(latitude))
			_, _ = digest.Write(scratch[:8])
			binary.LittleEndian.PutUint64(scratch[:8], math.Float64bits(longitude))
			_, _ = digest.Write(scratch[:8])
		}
		switch {
		case pointIndex < len(decoded.points) && decoded.points[pointIndex].position == position:
			point := decoded.points[pointIndex]
			if !indexed || math.Float64bits(latitude) != math.Float64bits(point.latitude) || math.Float64bits(longitude) != math.Float64bits(point.longitude) {
				return fmt.Errorf("%w: coordinate row %d", ErrSpatialIndexPersistenceSourceMismatch, position)
			}
			if err := tree.Upsert(uint64(position)+1, materializedSpatialBounds(point.latitude, point.longitude)); err != nil {
				return err
			}
			pointIndex++
		case fallbackIndex < len(decoded.fallback) && decoded.fallback[fallbackIndex] == position:
			if indexed {
				return fmt.Errorf("%w: fallback row %d", ErrSpatialIndexPersistenceSourceMismatch, position)
			}
			fallback[position] = struct{}{}
			fallbackIndex++
		default:
			return fmt.Errorf("%w: missing row %d", ErrSpatialIndexPersistenceInvalid, position)
		}
	}
	if pointIndex != len(decoded.points) || fallbackIndex != len(decoded.fallback) {
		return ErrSpatialIndexPersistenceInvalid
	}
	var sourceDigest [sha256.Size]byte
	copy(sourceDigest[:], digest.Sum(nil))
	if sourceDigest != decoded.digest {
		return fmt.Errorf("%w: fields %s,%s", ErrSpatialIndexPersistenceSourceMismatch, latitudeField, longitudeField)
	}
	if source.spatialIndexes == nil {
		source.spatialIndexes = make(map[string]*materializedSpatialIndex)
	}
	source.spatialIndexes[spatialIndexKey(latitudeField, longitudeField)] = &materializedSpatialIndex{
		latitudeField:  latitudeField,
		longitudeField: longitudeField,
		tree:           tree,
		fallback:       fallback,
	}
	return nil
}

func decodeSpatialIndexPersistence(frame []byte) (decodedSpatialIndexPersistence, error) {
	var decoded decodedSpatialIndexPersistence
	if len(frame) < len(spatialIndexWireMagic)+spatialIndexWireChecksumLength || len(frame) > spatialIndexWireMaxBytes {
		return decoded, ErrSpatialIndexPersistenceInvalid
	}
	payloadLength := len(frame) - spatialIndexWireChecksumLength
	if string(frame[:len(spatialIndexWireMagic)]) != spatialIndexWireMagic {
		return decoded, ErrSpatialIndexPersistenceInvalid
	}
	expected := binary.LittleEndian.Uint32(frame[payloadLength:])
	if crc32.Checksum(frame[:payloadLength], spatialIndexPersistenceCRC) != expected {
		return decoded, ErrSpatialIndexPersistenceChecksum
	}
	payload := frame[:payloadLength]
	offset := len(spatialIndexWireMagic)
	var err error
	if decoded.latitudeField, err = readSpatialIndexString(payload, &offset, spatialIndexWireMaxFieldBytes); err != nil {
		return decoded, err
	}
	if decoded.longitudeField, err = readSpatialIndexString(payload, &offset, spatialIndexWireMaxFieldBytes); err != nil {
		return decoded, err
	}
	if decoded.latitudeField == "" || decoded.longitudeField == "" || decoded.latitudeField == decoded.longitudeField {
		return decoded, ErrSpatialIndexPersistenceInvalid
	}
	rowCount, err := readSpatialIndexUvarint(payload, &offset)
	if err != nil || rowCount > spatialIndexWireMaxRows {
		return decoded, ErrSpatialIndexPersistenceInvalid
	}
	if len(payload)-offset < sha256.Size {
		return decoded, ErrSpatialIndexPersistenceInvalid
	}
	copy(decoded.digest[:], payload[offset:offset+sha256.Size])
	offset += sha256.Size
	pointCount, err := readSpatialIndexUvarint(payload, &offset)
	if err != nil || pointCount > rowCount {
		return decoded, ErrSpatialIndexPersistenceInvalid
	}
	fallbackCount, err := readSpatialIndexUvarint(payload, &offset)
	if err != nil || fallbackCount > rowCount || pointCount > rowCount-fallbackCount || pointCount+fallbackCount != rowCount {
		return decoded, ErrSpatialIndexPersistenceInvalid
	}
	decoded.rowCount = int(rowCount)
	decoded.points = make([]spatialIndexPersistencePoint, 0, int(pointCount))
	var previousPosition uint64
	for index := uint64(0); index < pointCount; index++ {
		delta, readErr := readSpatialIndexUvarint(payload, &offset)
		if readErr != nil || index > 0 && delta == 0 {
			return decoded, ErrSpatialIndexPersistenceInvalid
		}
		position := delta
		if index > 0 {
			if position > math.MaxUint64-previousPosition {
				return decoded, ErrSpatialIndexPersistenceInvalid
			}
			position += previousPosition
		}
		if position >= rowCount || len(payload)-offset < 16 {
			return decoded, ErrSpatialIndexPersistenceInvalid
		}
		latitudeBits := binary.LittleEndian.Uint64(payload[offset : offset+8])
		offset += 8
		longitudeBits := binary.LittleEndian.Uint64(payload[offset : offset+8])
		offset += 8
		latitude := math.Float64frombits(latitudeBits)
		longitude := math.Float64frombits(longitudeBits)
		if !materializedSpatialCoordinateValid(latitude, longitude) {
			return decoded, ErrSpatialIndexPersistenceInvalid
		}
		decoded.points = append(decoded.points, spatialIndexPersistencePoint{position: int(position), latitude: latitude, longitude: longitude})
		previousPosition = position
	}
	previousPosition = 0
	decoded.fallback = make([]int, 0, int(fallbackCount))
	for index := uint64(0); index < fallbackCount; index++ {
		delta, readErr := readSpatialIndexUvarint(payload, &offset)
		if readErr != nil || index > 0 && delta == 0 {
			return decoded, ErrSpatialIndexPersistenceInvalid
		}
		position := delta
		if index > 0 {
			if position > math.MaxUint64-previousPosition {
				return decoded, ErrSpatialIndexPersistenceInvalid
			}
			position += previousPosition
		}
		if position >= rowCount {
			return decoded, ErrSpatialIndexPersistenceInvalid
		}
		decoded.fallback = append(decoded.fallback, int(position))
		previousPosition = position
	}
	if offset != len(payload) {
		return decoded, ErrSpatialIndexPersistenceInvalid
	}
	return decoded, nil
}

func materializedSpatialIndexSourceDigestLocked(source *MaterializedSource, latitudeField, longitudeField string) [sha256.Size]byte {
	digest := sha256.New()
	var scratch [binary.MaxVarintLen64]byte
	for position, row := range source.rows {
		n := binary.PutUvarint(scratch[:], uint64(position))
		_, _ = digest.Write(scratch[:n])
		latitude, longitude, indexed := materializedSpatialPoint(row, latitudeField, longitudeField)
		if !indexed {
			scratch[0] = 0
			_, _ = digest.Write(scratch[:1])
			continue
		}
		scratch[0] = 1
		_, _ = digest.Write(scratch[:1])
		binary.LittleEndian.PutUint64(scratch[:8], math.Float64bits(latitude))
		_, _ = digest.Write(scratch[:8])
		binary.LittleEndian.PutUint64(scratch[:8], math.Float64bits(longitude))
		_, _ = digest.Write(scratch[:8])
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func appendSpatialIndexPoints(dst []byte, points []spatialIndexPersistencePoint) []byte {
	var previousPosition int
	for index, point := range points {
		if index == 0 {
			dst = appendSpatialIndexUvarint(dst, uint64(point.position))
		} else {
			dst = appendSpatialIndexUvarint(dst, uint64(point.position-previousPosition))
		}
		var bits [16]byte
		binary.LittleEndian.PutUint64(bits[:8], math.Float64bits(point.latitude))
		binary.LittleEndian.PutUint64(bits[8:], math.Float64bits(point.longitude))
		dst = append(dst, bits[:]...)
		previousPosition = point.position
	}
	return dst
}

func appendSpatialIndexPositions(dst []byte, positions []int) []byte {
	var previousPosition int
	for index, position := range positions {
		if index == 0 {
			dst = appendSpatialIndexUvarint(dst, uint64(position))
		} else {
			dst = appendSpatialIndexUvarint(dst, uint64(position-previousPosition))
		}
		previousPosition = position
	}
	return dst
}

func appendSpatialIndexString(dst []byte, value string) []byte {
	dst = appendSpatialIndexUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendSpatialIndexUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func readSpatialIndexUvarint(payload []byte, offset *int) (uint64, error) {
	if offset == nil || *offset < 0 || *offset >= len(payload) {
		return 0, ErrSpatialIndexPersistenceInvalid
	}
	value, length := binary.Uvarint(payload[*offset:])
	if length <= 0 {
		return 0, ErrSpatialIndexPersistenceInvalid
	}
	*offset += length
	return value, nil
}

func readSpatialIndexString(payload []byte, offset *int, maxLength int) (string, error) {
	length, err := readSpatialIndexUvarint(payload, offset)
	if err != nil || length > uint64(maxLength) || length > uint64(len(payload)-*offset) {
		return "", ErrSpatialIndexPersistenceInvalid
	}
	start := *offset
	*offset += int(length)
	return string(payload[start:*offset]), nil
}
