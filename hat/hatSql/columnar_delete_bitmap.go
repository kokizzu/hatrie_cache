package hatSql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/bits"
	"sort"
)

const (
	columnarDeleteBitmapMagic       = "HDB1"
	columnarDeleteBitmapVersion     = 1
	columnarDeleteBitmapHeaderBytes = 20
	columnarDeleteBitmapCRCBytes    = 4

	// MaxSQLColumnarDeleteBitmapRows bounds allocations while decoding a
	// persisted sidecar. It supports parts with up to 67 million rows.
	MaxSQLColumnarDeleteBitmapRows = 1 << 26
	// MaxSQLColumnarDeleteBitmapBytes bounds the complete encoded sidecar.
	MaxSQLColumnarDeleteBitmapBytes = 64 << 20
)

var (
	// ErrSQLColumnarDeleteBitmapInvalid identifies malformed persisted data.
	ErrSQLColumnarDeleteBitmapInvalid = errors.New("invalid SQL columnar delete bitmap")
	// ErrSQLColumnarDeleteBitmapRow identifies an invalid row count or row ID.
	ErrSQLColumnarDeleteBitmapRow = errors.New("invalid SQL columnar delete bitmap row")
)

// SQLColumnarDeleteBitmapEncoding is the on-disk representation selected for
// a columnar part's deleted physical rows.
type SQLColumnarDeleteBitmapEncoding uint8

const (
	// SQLColumnarDeleteBitmapEncodingSparse stores sorted row IDs as delta
	// encoded unsigned varints.
	SQLColumnarDeleteBitmapEncodingSparse SQLColumnarDeleteBitmapEncoding = 1
	// SQLColumnarDeleteBitmapEncodingDense stores one bit per physical row.
	SQLColumnarDeleteBitmapEncodingDense SQLColumnarDeleteBitmapEncoding = 2
)

func (e SQLColumnarDeleteBitmapEncoding) String() string {
	switch e {
	case SQLColumnarDeleteBitmapEncodingSparse:
		return "sparse"
	case SQLColumnarDeleteBitmapEncodingDense:
		return "dense"
	default:
		return "unknown"
	}
}

// SQLColumnarDeleteBitmap is an immutable, persistent-part delete mask.
//
// The constructor chooses the smaller of a delta-coded sparse representation
// and a dense bitset. The encoded form is deterministic and includes a CRC32
// over its header and payload, so storage adapters can keep it as a sidecar
// without coupling the part's column data to the delete state.
type SQLColumnarDeleteBitmap struct {
	rowCount     uint32
	deletedCount uint32
	encoding     SQLColumnarDeleteBitmapEncoding
	sparseRows   []uint32
	denseWords   []uint64
}

// NewSQLColumnarDeleteBitmap creates an empty delete bitmap for rowCount rows.
func NewSQLColumnarDeleteBitmap(rowCount int) (*SQLColumnarDeleteBitmap, error) {
	return NewSQLColumnarDeleteBitmapFromRows(rowCount, nil)
}

// NewSQLColumnarDeleteBitmapFromRows creates a delete bitmap from physical row
// IDs. Input order is ignored and duplicate IDs are collapsed.
func NewSQLColumnarDeleteBitmapFromRows(rowCount int, deletedRows []uint32) (*SQLColumnarDeleteBitmap, error) {
	if rowCount < 0 || rowCount > MaxSQLColumnarDeleteBitmapRows {
		return nil, fmt.Errorf("%w: row count %d is outside [0,%d]", ErrSQLColumnarDeleteBitmapRow, rowCount, MaxSQLColumnarDeleteBitmapRows)
	}
	denseBytes := densePayloadBytes(rowCount)
	if len(deletedRows) > 0 && len(deletedRows) >= denseBytes {
		words, deletedCount, err := denseWordsFromRows(rowCount, deletedRows)
		if err != nil {
			return nil, err
		}
		if int(deletedCount) >= denseBytes {
			return &SQLColumnarDeleteBitmap{
				rowCount:     uint32(rowCount),
				deletedCount: deletedCount,
				encoding:     SQLColumnarDeleteBitmapEncodingDense,
				denseWords:   words,
			}, nil
		}
	}

	sortedUnique := true
	for index, row := range deletedRows {
		if uint64(row) >= uint64(rowCount) {
			return nil, fmt.Errorf("%w: row %d is outside [0,%d)", ErrSQLColumnarDeleteBitmapRow, row, rowCount)
		}
		if index > 0 && row <= deletedRows[index-1] {
			sortedUnique = false
		}
	}
	rows := deletedRows
	rowsOwned := false
	if !sortedUnique {
		rows = append([]uint32(nil), deletedRows...)
		sort.Slice(rows, func(i, j int) bool { return rows[i] < rows[j] })
		unique := rows[:0]
		for _, row := range rows {
			if len(unique) == 0 || unique[len(unique)-1] != row {
				unique = append(unique, row)
			}
		}
		rows = unique
		rowsOwned = true
	}

	bitmap := &SQLColumnarDeleteBitmap{
		rowCount:     uint32(rowCount),
		deletedCount: uint32(len(rows)),
		encoding:     SQLColumnarDeleteBitmapEncodingSparse,
	}
	if len(rows) == 0 {
		return bitmap, nil
	}

	sparseBytes := sparsePayloadBytes(rows)
	if sparseBytes < denseBytes {
		if rowsOwned {
			bitmap.sparseRows = rows
		} else {
			bitmap.sparseRows = append([]uint32(nil), rows...)
		}
		return bitmap, nil
	}

	bitmap.encoding = SQLColumnarDeleteBitmapEncodingDense
	bitmap.denseWords = make([]uint64, denseBytes/8)
	for _, row := range rows {
		bitmap.denseWords[row>>6] |= uint64(1) << (row & 63)
	}
	return bitmap, nil
}

// RowCount returns the number of physical rows covered by the bitmap.
func (b *SQLColumnarDeleteBitmap) RowCount() int {
	if b == nil {
		return 0
	}
	return int(b.rowCount)
}

// DeletedCount returns the number of deleted physical rows.
func (b *SQLColumnarDeleteBitmap) DeletedCount() int {
	if b == nil {
		return 0
	}
	return int(b.deletedCount)
}

// LiveCount returns the number of rows not covered by the bitmap.
func (b *SQLColumnarDeleteBitmap) LiveCount() int {
	if b == nil {
		return 0
	}
	return int(b.rowCount - b.deletedCount)
}

// Encoding reports whether the bitmap uses sparse or dense storage.
func (b *SQLColumnarDeleteBitmap) Encoding() SQLColumnarDeleteBitmapEncoding {
	if b == nil {
		return 0
	}
	return b.encoding
}

// IsDeleted reports whether row is deleted. Out-of-range rows are treated as
// live so callers can use it safely while scanning a larger input batch.
func (b *SQLColumnarDeleteBitmap) IsDeleted(row uint32) bool {
	if b == nil || row >= b.rowCount {
		return false
	}
	switch b.encoding {
	case SQLColumnarDeleteBitmapEncodingSparse:
		index := sort.Search(len(b.sparseRows), func(index int) bool {
			return b.sparseRows[index] >= row
		})
		return index < len(b.sparseRows) && b.sparseRows[index] == row
	case SQLColumnarDeleteBitmapEncodingDense:
		return b.denseWords[row>>6]&(uint64(1)<<(row&63)) != 0
	default:
		return false
	}
}

// AppendDeletedRows appends deleted physical row IDs in ascending order.
func (b *SQLColumnarDeleteBitmap) AppendDeletedRows(dst []uint32) []uint32 {
	if b == nil {
		return dst
	}
	if b.encoding == SQLColumnarDeleteBitmapEncodingSparse {
		return append(dst, b.sparseRows...)
	}
	for wordIndex, word := range b.denseWords {
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			dst = append(dst, uint32(wordIndex*64+bit))
			word &^= uint64(1) << bit
		}
	}
	return dst
}

// AppendLiveRows appends live physical row IDs in ascending order.
func (b *SQLColumnarDeleteBitmap) AppendLiveRows(dst []uint32) []uint32 {
	if b == nil {
		return dst
	}
	deletedIndex := 0
	for row := uint32(0); row < b.rowCount; row++ {
		if b.encoding == SQLColumnarDeleteBitmapEncodingSparse {
			for deletedIndex < len(b.sparseRows) && b.sparseRows[deletedIndex] < row {
				deletedIndex++
			}
			if deletedIndex < len(b.sparseRows) && b.sparseRows[deletedIndex] == row {
				continue
			}
		} else if b.denseWords[row>>6]&(uint64(1)<<(row&63)) != 0 {
			continue
		}
		dst = append(dst, row)
	}
	return dst
}

// FilterLiveRows removes deleted physical row IDs from rows in place and
// preserves the input order. IDs outside the bitmap's row range are removed.
func (b *SQLColumnarDeleteBitmap) FilterLiveRows(rows []uint32) []uint32 {
	if b == nil {
		return rows
	}
	write := 0
	for _, row := range rows {
		if row < b.rowCount && !b.IsDeleted(row) {
			rows[write] = row
			write++
		}
	}
	return rows[:write]
}

// MarshalBinary encodes the bitmap as a CRC-protected sidecar.
func (b *SQLColumnarDeleteBitmap) MarshalBinary() ([]byte, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	payloadBytes := b.payloadBytes()
	totalBytes := columnarDeleteBitmapHeaderBytes + payloadBytes + columnarDeleteBitmapCRCBytes
	if totalBytes > MaxSQLColumnarDeleteBitmapBytes {
		return nil, fmt.Errorf("%w: encoded size %d exceeds %d bytes", ErrSQLColumnarDeleteBitmapInvalid, totalBytes, MaxSQLColumnarDeleteBitmapBytes)
	}

	data := make([]byte, totalBytes)
	copy(data[:4], columnarDeleteBitmapMagic)
	data[4] = columnarDeleteBitmapVersion
	data[5] = byte(b.encoding)
	binary.LittleEndian.PutUint32(data[8:12], b.rowCount)
	binary.LittleEndian.PutUint32(data[12:16], b.deletedCount)
	binary.LittleEndian.PutUint32(data[16:20], uint32(payloadBytes))
	payload := data[columnarDeleteBitmapHeaderBytes : columnarDeleteBitmapHeaderBytes+payloadBytes]
	switch b.encoding {
	case SQLColumnarDeleteBitmapEncodingSparse:
		var previous uint32
		offset := 0
		for _, row := range b.sparseRows {
			offset += binary.PutUvarint(payload[offset:], uint64(row)-uint64(previous))
			previous = row
		}
	case SQLColumnarDeleteBitmapEncodingDense:
		for index, word := range b.denseWords {
			binary.LittleEndian.PutUint64(payload[index*8:index*8+8], word)
		}
	}
	binary.LittleEndian.PutUint32(data[totalBytes-columnarDeleteBitmapCRCBytes:], crc32.ChecksumIEEE(data[:totalBytes-columnarDeleteBitmapCRCBytes]))
	return data, nil
}

// UnmarshalSQLColumnarDeleteBitmap decodes and validates a persisted sidecar.
func UnmarshalSQLColumnarDeleteBitmap(data []byte) (*SQLColumnarDeleteBitmap, error) {
	if len(data) < columnarDeleteBitmapHeaderBytes+columnarDeleteBitmapCRCBytes || len(data) > MaxSQLColumnarDeleteBitmapBytes {
		return nil, fmt.Errorf("%w: encoded size %d is outside valid bounds", ErrSQLColumnarDeleteBitmapInvalid, len(data))
	}
	if !bytes.Equal(data[:4], []byte(columnarDeleteBitmapMagic)) {
		return nil, fmt.Errorf("%w: bad magic", ErrSQLColumnarDeleteBitmapInvalid)
	}
	if data[4] != columnarDeleteBitmapVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrSQLColumnarDeleteBitmapInvalid, data[4])
	}
	if data[6] != 0 || data[7] != 0 {
		return nil, fmt.Errorf("%w: reserved header bytes are not zero", ErrSQLColumnarDeleteBitmapInvalid)
	}
	encoding := SQLColumnarDeleteBitmapEncoding(data[5])
	if encoding != SQLColumnarDeleteBitmapEncodingSparse && encoding != SQLColumnarDeleteBitmapEncodingDense {
		return nil, fmt.Errorf("%w: unknown encoding %d", ErrSQLColumnarDeleteBitmapInvalid, data[5])
	}
	rowCount := binary.LittleEndian.Uint32(data[8:12])
	deletedCount := binary.LittleEndian.Uint32(data[12:16])
	if rowCount > MaxSQLColumnarDeleteBitmapRows || deletedCount > rowCount {
		return nil, fmt.Errorf("%w: row count %d and deleted count %d are inconsistent", ErrSQLColumnarDeleteBitmapInvalid, rowCount, deletedCount)
	}
	payloadBytes := binary.LittleEndian.Uint32(data[16:20])
	payloadEnd := uint64(columnarDeleteBitmapHeaderBytes) + uint64(payloadBytes)
	if payloadEnd+columnarDeleteBitmapCRCBytes != uint64(len(data)) {
		return nil, fmt.Errorf("%w: payload length %d does not match encoded size", ErrSQLColumnarDeleteBitmapInvalid, payloadBytes)
	}
	if binary.LittleEndian.Uint32(data[len(data)-columnarDeleteBitmapCRCBytes:]) != crc32.ChecksumIEEE(data[:len(data)-columnarDeleteBitmapCRCBytes]) {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrSQLColumnarDeleteBitmapInvalid)
	}

	payload := data[columnarDeleteBitmapHeaderBytes : columnarDeleteBitmapHeaderBytes+int(payloadBytes)]
	bitmap := &SQLColumnarDeleteBitmap{
		rowCount:     rowCount,
		deletedCount: deletedCount,
		encoding:     encoding,
	}
	switch encoding {
	case SQLColumnarDeleteBitmapEncodingSparse:
		rows, err := decodeSparseDeleteRows(payload, rowCount, deletedCount)
		if err != nil {
			return nil, err
		}
		bitmap.sparseRows = rows
	case SQLColumnarDeleteBitmapEncodingDense:
		words, err := decodeDenseDeleteWords(payload, rowCount, deletedCount)
		if err != nil {
			return nil, err
		}
		bitmap.denseWords = words
	}
	return bitmap, nil
}

// UnmarshalBinary replaces b with a validated decoded bitmap.
func (b *SQLColumnarDeleteBitmap) UnmarshalBinary(data []byte) error {
	decoded, err := UnmarshalSQLColumnarDeleteBitmap(data)
	if err != nil {
		return err
	}
	if b == nil {
		return fmt.Errorf("%w: nil destination", ErrSQLColumnarDeleteBitmapInvalid)
	}
	*b = *decoded
	return nil
}

func (b *SQLColumnarDeleteBitmap) validate() error {
	if b == nil {
		return fmt.Errorf("%w: nil bitmap", ErrSQLColumnarDeleteBitmapInvalid)
	}
	if b.rowCount > MaxSQLColumnarDeleteBitmapRows || b.deletedCount > b.rowCount {
		return fmt.Errorf("%w: row count %d and deleted count %d are inconsistent", ErrSQLColumnarDeleteBitmapInvalid, b.rowCount, b.deletedCount)
	}
	switch b.encoding {
	case SQLColumnarDeleteBitmapEncodingSparse:
		if len(b.sparseRows) != int(b.deletedCount) || len(b.denseWords) != 0 {
			return fmt.Errorf("%w: sparse representation lengths are inconsistent", ErrSQLColumnarDeleteBitmapInvalid)
		}
		var previous uint32
		for index, row := range b.sparseRows {
			if uint64(row) >= uint64(b.rowCount) || (index > 0 && row <= previous) {
				return fmt.Errorf("%w: sparse row order or range is invalid", ErrSQLColumnarDeleteBitmapInvalid)
			}
			previous = row
		}
	case SQLColumnarDeleteBitmapEncodingDense:
		if len(b.denseWords) != densePayloadBytes(int(b.rowCount))/8 || len(b.sparseRows) != 0 {
			return fmt.Errorf("%w: dense representation length is inconsistent", ErrSQLColumnarDeleteBitmapInvalid)
		}
		var count uint32
		for _, word := range b.denseWords {
			count += uint32(bits.OnesCount64(word))
		}
		if count != b.deletedCount {
			return fmt.Errorf("%w: dense deleted count is inconsistent", ErrSQLColumnarDeleteBitmapInvalid)
		}
		if remainder := b.rowCount & 63; remainder != 0 && len(b.denseWords) > 0 {
			mask := uint64(1)<<remainder - 1
			if b.denseWords[len(b.denseWords)-1]&^mask != 0 {
				return fmt.Errorf("%w: dense bits exceed row count", ErrSQLColumnarDeleteBitmapInvalid)
			}
		}
	default:
		return fmt.Errorf("%w: unknown encoding %d", ErrSQLColumnarDeleteBitmapInvalid, b.encoding)
	}
	return nil
}

func (b *SQLColumnarDeleteBitmap) payloadBytes() int {
	if b.encoding == SQLColumnarDeleteBitmapEncodingSparse {
		return sparsePayloadBytes(b.sparseRows)
	}
	return len(b.denseWords) * 8
}

func decodeSparseDeleteRows(payload []byte, rowCount, deletedCount uint32) ([]uint32, error) {
	if deletedCount == 0 && len(payload) != 0 {
		return nil, fmt.Errorf("%w: empty sparse bitmap has a payload", ErrSQLColumnarDeleteBitmapInvalid)
	}
	rows := make([]uint32, deletedCount)
	position := 0
	var previous uint64
	for index := uint32(0); index < deletedCount; index++ {
		if position >= len(payload) {
			return nil, fmt.Errorf("%w: sparse payload ends before row %d", ErrSQLColumnarDeleteBitmapInvalid, index)
		}
		delta, size := binary.Uvarint(payload[position:])
		if size <= 0 {
			return nil, fmt.Errorf("%w: invalid sparse varint at byte %d", ErrSQLColumnarDeleteBitmapInvalid, position)
		}
		position += size
		if index > 0 && delta == 0 {
			return nil, fmt.Errorf("%w: sparse rows are not strictly increasing", ErrSQLColumnarDeleteBitmapInvalid)
		}
		row := delta
		if index > 0 {
			if delta > ^uint64(0)-previous {
				return nil, fmt.Errorf("%w: sparse row delta overflows", ErrSQLColumnarDeleteBitmapInvalid)
			}
			row = previous + delta
		}
		if row >= uint64(rowCount) {
			return nil, fmt.Errorf("%w: sparse row %d is outside row count %d", ErrSQLColumnarDeleteBitmapInvalid, row, rowCount)
		}
		rows[index] = uint32(row)
		previous = row
	}
	if position != len(payload) {
		return nil, fmt.Errorf("%w: sparse payload has trailing bytes", ErrSQLColumnarDeleteBitmapInvalid)
	}
	return rows, nil
}

func decodeDenseDeleteWords(payload []byte, rowCount, deletedCount uint32) ([]uint64, error) {
	expectedBytes := densePayloadBytes(int(rowCount))
	if len(payload) != expectedBytes {
		return nil, fmt.Errorf("%w: dense payload size %d, want %d", ErrSQLColumnarDeleteBitmapInvalid, len(payload), expectedBytes)
	}
	words := make([]uint64, expectedBytes/8)
	var count uint32
	for index := range words {
		words[index] = binary.LittleEndian.Uint64(payload[index*8 : index*8+8])
		count += uint32(bits.OnesCount64(words[index]))
	}
	if count != deletedCount {
		return nil, fmt.Errorf("%w: dense deleted count %d, want %d", ErrSQLColumnarDeleteBitmapInvalid, count, deletedCount)
	}
	if remainder := rowCount & 63; remainder != 0 && len(words) > 0 {
		mask := uint64(1)<<remainder - 1
		if words[len(words)-1]&^mask != 0 {
			return nil, fmt.Errorf("%w: dense bits exceed row count", ErrSQLColumnarDeleteBitmapInvalid)
		}
	}
	return words, nil
}

func sparsePayloadBytes(rows []uint32) int {
	var total int
	var previous uint32
	for _, row := range rows {
		total += unsignedVarintBytes(uint64(row) - uint64(previous))
		previous = row
	}
	return total
}

func unsignedVarintBytes(value uint64) int {
	if value == 0 {
		return 1
	}
	bytes := 0
	for value != 0 {
		bytes++
		value >>= 7
	}
	return bytes
}

func densePayloadBytes(rowCount int) int {
	return ((rowCount + 63) / 64) * 8
}

func denseWordsFromRows(rowCount int, rows []uint32) ([]uint64, uint32, error) {
	words := make([]uint64, densePayloadBytes(rowCount)/8)
	for _, row := range rows {
		if uint64(row) >= uint64(rowCount) {
			return nil, 0, fmt.Errorf("%w: row %d is outside [0,%d)", ErrSQLColumnarDeleteBitmapRow, row, rowCount)
		}
		words[row>>6] |= uint64(1) << (row & 63)
	}
	var deletedCount uint32
	for _, word := range words {
		deletedCount += uint32(bits.OnesCount64(word))
	}
	return words, deletedCount, nil
}
