package hatDataStructure

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

const (
	lowCardinalityStringWireMagic   = "HLC1"
	lowCardinalityStringWireVersion = 1

	// These limits protect the binary decoder from oversized allocations. They
	// are deliberately larger than ordinary in-memory columns but bounded for
	// data received from a peer or read from an untrusted file.
	MaxLowCardinalityStringDictionaryValues = 1 << 24
	MaxLowCardinalityStringColumnRows       = 1 << 26
	MaxLowCardinalityStringWireBytes        = 1 << 30
	// DefaultLowCardinalityStringMaxDistinctValues keeps an opt-in column from
	// silently becoming a high-cardinality hash table. Use -1 in builder
	// options only when an independently measured workload justifies it.
	DefaultLowCardinalityStringMaxDistinctValues = 4096
)

var (
	ErrLowCardinalityStringBuilderBuilt  = errors.New("hatDataStructure: low-cardinality string builder already built")
	ErrLowCardinalityStringDistinctLimit = errors.New("hatDataStructure: low-cardinality string distinct-value limit exceeded")
	ErrLowCardinalityStringBinaryInvalid = errors.New("hatDataStructure: invalid low-cardinality string binary")
	ErrLowCardinalityStringBinaryLimit   = errors.New("hatDataStructure: low-cardinality string binary limit exceeded")
)

// LowCardinalityStringBuilderOptions configures one mutable dictionary
// builder. MaxDistinctValues is zero for the conservative default limit and
// negative for no distinct-value limit.
type LowCardinalityStringBuilderOptions struct {
	// InitialCapacity reserves dictionary entries and lookup capacity.
	InitialCapacity int
	// InitialRowCapacity reserves encoded row codes independently of the
	// dictionary cardinality.
	InitialRowCapacity int
	// MaxDistinctValues is zero for no explicit limit.
	MaxDistinctValues int
}

// LowCardinalityStringBuilder collects repeated string values before creating
// an immutable dictionary-encoded column. Codes returned by Append refer to
// the builder's temporary dictionary and are not stable until Build returns.
type LowCardinalityStringBuilder struct {
	dictionary  []string
	lookup      map[string]uint32
	codes       []uint32
	valid       []uint64
	maxDistinct int
	built       bool
}

// NewLowCardinalityStringBuilder creates a dictionary builder. Negative
// capacities are treated as zero so configuration cannot cause a panic.
func NewLowCardinalityStringBuilder(options LowCardinalityStringBuilderOptions) *LowCardinalityStringBuilder {
	initial := options.InitialCapacity
	if initial < 0 {
		initial = 0
	}
	maxDistinct := options.MaxDistinctValues
	if maxDistinct == 0 {
		maxDistinct = DefaultLowCardinalityStringMaxDistinctValues
	} else if maxDistinct < -1 {
		maxDistinct = -1
	}
	initialRows := options.InitialRowCapacity
	if initialRows < 0 {
		initialRows = 0
	}
	return &LowCardinalityStringBuilder{
		dictionary:  make([]string, 0, initial),
		lookup:      make(map[string]uint32, initial),
		codes:       make([]uint32, 0, initialRows),
		maxDistinct: maxDistinct,
	}
}

// Append adds one non-NULL string and returns its temporary builder code.
func (builder *LowCardinalityStringBuilder) Append(value string) (uint32, error) {
	if builder == nil {
		return 0, ErrLowCardinalityStringBuilderBuilt
	}
	if builder.built {
		return 0, ErrLowCardinalityStringBuilderBuilt
	}
	code, exists := builder.lookup[value]
	if !exists {
		if builder.maxDistinct > 0 && len(builder.dictionary) >= builder.maxDistinct {
			return 0, ErrLowCardinalityStringDistinctLimit
		}
		if len(builder.dictionary) >= int(^uint32(0)) {
			return 0, ErrLowCardinalityStringDistinctLimit
		}
		code = uint32(len(builder.dictionary))
		builder.dictionary = append(builder.dictionary, value)
		builder.lookup[value] = code
	}
	row := len(builder.codes)
	builder.codes = append(builder.codes, code)
	builder.setValid(row, true)
	return code, nil
}

// AppendNull adds one SQL-style NULL value without adding a dictionary entry.
func (builder *LowCardinalityStringBuilder) AppendNull() error {
	if builder == nil || builder.built {
		return ErrLowCardinalityStringBuilderBuilt
	}
	row := len(builder.codes)
	builder.codes = append(builder.codes, 0)
	builder.setValid(row, false)
	return nil
}

// Build sorts dictionary values so codes are usable for equality, grouping,
// and lexical ORDER BY operations, then releases the mutable lookup map.
func (builder *LowCardinalityStringBuilder) Build() (LowCardinalityStringColumn, error) {
	if builder == nil || builder.built {
		return LowCardinalityStringColumn{}, ErrLowCardinalityStringBuilderBuilt
	}
	if len(builder.dictionary) > MaxLowCardinalityStringDictionaryValues || len(builder.codes) > MaxLowCardinalityStringColumnRows {
		return LowCardinalityStringColumn{}, ErrLowCardinalityStringBinaryLimit
	}
	type dictionaryEntry struct {
		value string
		old   uint32
	}
	entries := make([]dictionaryEntry, len(builder.dictionary))
	for index, value := range builder.dictionary {
		entries[index] = dictionaryEntry{value: value, old: uint32(index)}
	}
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].value < entries[right].value
	})
	remap := make([]uint32, len(entries))
	dictionary := make([]string, len(entries))
	for index, entry := range entries {
		dictionary[index] = entry.value
		remap[entry.old] = uint32(index)
	}
	for index, code := range builder.codes {
		if code >= uint32(len(remap)) {
			return LowCardinalityStringColumn{}, ErrLowCardinalityStringBinaryInvalid
		}
		builder.codes[index] = remap[code]
	}
	column := LowCardinalityStringColumn{
		dictionary: dictionary,
		codes:      packLowCardinalityCodes(builder.codes, lowCardinalityStringCodeWidth(len(dictionary))),
		width:      uint8(lowCardinalityStringCodeWidth(len(dictionary))),
		valid:      builder.valid,
	}
	builder.dictionary = nil
	builder.lookup = nil
	builder.codes = nil
	builder.valid = nil
	builder.built = true
	return column, nil
}

func (builder *LowCardinalityStringBuilder) setValid(row int, valid bool) {
	if builder.valid == nil {
		if valid {
			return
		}
		builder.valid = make([]uint64, (row+64)/64)
		for index := 0; index < row; index++ {
			builder.valid[index>>6] |= uint64(1) << uint(index&63)
		}
		return
	}
	words := (row + 64) / 64
	if len(builder.valid) < words {
		builder.valid = append(builder.valid, make([]uint64, words-len(builder.valid))...)
	}
	if valid {
		builder.valid[row>>6] |= uint64(1) << uint(row&63)
	}
}

// LowCardinalityStringColumn is an immutable sorted-dictionary string
// column. Code order is dictionary lexical order, so CodeAt can be used as a
// compact GROUP BY key and as an ORDER BY key without materializing strings.
type LowCardinalityStringColumn struct {
	dictionary []string
	codes      []byte
	width      uint8
	valid      []uint64
}

// Len returns the number of rows in the column.
func (column LowCardinalityStringColumn) Len() int {
	width := column.codeWidth()
	if width == 0 {
		return 0
	}
	return len(column.codes) / width
}

// Cardinality returns the number of distinct non-NULL strings.
func (column LowCardinalityStringColumn) Cardinality() int { return len(column.dictionary) }

// DictionaryValues returns a detached copy of the sorted dictionary.
func (column LowCardinalityStringColumn) DictionaryValues() []string {
	return append([]string(nil), column.dictionary...)
}

// CodeAt returns the sorted dictionary code for a non-NULL row.
func (column LowCardinalityStringColumn) CodeAt(row int) (uint32, bool) {
	if row < 0 || row >= column.Len() || !column.rowValid(row) {
		return 0, false
	}
	code := column.codeAt(row)
	if code >= uint32(len(column.dictionary)) {
		return 0, false
	}
	return code, true
}

// ValueAt returns the row value and whether the row is non-NULL.
func (column LowCardinalityStringColumn) ValueAt(row int) (string, bool) {
	code, valid := column.CodeAt(row)
	if !valid {
		return "", false
	}
	return column.dictionary[code], true
}

// LookupCode finds a value using binary search over the sorted dictionary.
// It allocates no map and is intended for WHERE equality probes after Build.
func (column LowCardinalityStringColumn) LookupCode(value string) (uint32, bool) {
	index := sort.SearchStrings(column.dictionary, value)
	if index >= len(column.dictionary) || column.dictionary[index] != value {
		return 0, false
	}
	return uint32(index), true
}

// CodeWidth returns the number of bytes needed to persist each dictionary
// code. Low-cardinality dictionaries therefore use one or two bytes per row
// until their cardinality requires four.
func (column LowCardinalityStringColumn) CodeWidth() int {
	return lowCardinalityStringCodeWidth(len(column.dictionary))
}

// CountCodes counts every non-NULL dictionary code in one dense pass. The
// result is suitable for GROUP BY aggregation and avoids hashing repeated
// strings or allocating one map entry per group.
func (column LowCardinalityStringColumn) CountCodes() []uint64 {
	counts := make([]uint64, len(column.dictionary))
	for row := 0; row < column.Len(); row++ {
		if !column.rowValid(row) {
			continue
		}
		code := column.codeAt(row)
		if code < uint32(len(counts)) {
			counts[code]++
		}
	}
	return counts
}

// MemoryBytes estimates the column's retained backing bytes, including string
// headers and the nullable bitmap but excluding the Go slice headers.
func (column LowCardinalityStringColumn) MemoryBytes() int {
	bytes := len(column.codes)
	bytes += len(column.valid) * 8
	for _, value := range column.dictionary {
		bytes += 16 + len(value)
	}
	return bytes
}

func (column LowCardinalityStringColumn) rowValid(row int) bool {
	return column.valid == nil || (row < column.Len() && column.valid[row>>6]&(uint64(1)<<uint(row&63)) != 0)
}

func (column LowCardinalityStringColumn) codeWidth() int {
	if column.width != 0 {
		return int(column.width)
	}
	return 1
}

func (column LowCardinalityStringColumn) codeAt(row int) uint32 {
	offset := row * column.codeWidth()
	switch column.codeWidth() {
	case 1:
		return uint32(column.codes[offset])
	case 2:
		return uint32(binary.LittleEndian.Uint16(column.codes[offset : offset+2]))
	default:
		return binary.LittleEndian.Uint32(column.codes[offset : offset+4])
	}
}

func lowCardinalityStringCodeWidth(cardinality int) int {
	switch {
	case cardinality <= 1<<8:
		return 1
	case cardinality <= 1<<16:
		return 2
	default:
		return 4
	}
}

// MarshalBinary encodes a bounded, deterministic dictionary column.
func (column LowCardinalityStringColumn) MarshalBinary() ([]byte, error) {
	if err := column.validate(); err != nil {
		return nil, err
	}
	if len(column.dictionary) > MaxLowCardinalityStringDictionaryValues || column.Len() > MaxLowCardinalityStringColumnRows {
		return nil, ErrLowCardinalityStringBinaryLimit
	}
	validityBytes := (column.Len() + 7) / 8
	encoded := make([]byte, 0, len(lowCardinalityStringWireMagic)+32+validityBytes)
	encoded = append(encoded, lowCardinalityStringWireMagic...)
	encoded = append(encoded, lowCardinalityStringWireVersion)
	encoded = appendLowCardinalityUvarint(encoded, uint64(len(column.dictionary)))
	encoded = appendLowCardinalityUvarint(encoded, uint64(column.Len()))
	if column.valid == nil {
		encoded = appendLowCardinalityUvarint(encoded, 0)
	} else {
		encoded = appendLowCardinalityUvarint(encoded, uint64(validityBytes))
		for index := 0; index < validityBytes; index++ {
			word := column.valid[index>>3]
			encoded = append(encoded, byte(word>>uint((index&7)*8)))
		}
	}
	for _, value := range column.dictionary {
		encoded = appendLowCardinalityUvarint(encoded, uint64(len(value)))
		encoded = append(encoded, value...)
	}
	encoded = append(encoded, column.codes...)
	if len(encoded) > MaxLowCardinalityStringWireBytes {
		return nil, ErrLowCardinalityStringBinaryLimit
	}
	return encoded, nil
}

// UnmarshalLowCardinalityStringColumn decodes and strictly validates a
// LowCardinalityStringColumn binary representation.
func UnmarshalLowCardinalityStringColumn(data []byte) (LowCardinalityStringColumn, error) {
	if len(data) > MaxLowCardinalityStringWireBytes || len(data) < len(lowCardinalityStringWireMagic)+1 {
		return LowCardinalityStringColumn{}, ErrLowCardinalityStringBinaryLimit
	}
	if !bytes.Equal(data[:len(lowCardinalityStringWireMagic)], []byte(lowCardinalityStringWireMagic)) {
		return LowCardinalityStringColumn{}, ErrLowCardinalityStringBinaryInvalid
	}
	offset := len(lowCardinalityStringWireMagic)
	if data[offset] != lowCardinalityStringWireVersion {
		return LowCardinalityStringColumn{}, fmt.Errorf("%w: unsupported version %d", ErrLowCardinalityStringBinaryInvalid, data[offset])
	}
	offset++
	dictionaryCount, err := readLowCardinalityUvarint(data, &offset)
	if err != nil {
		return LowCardinalityStringColumn{}, err
	}
	rowCount, err := readLowCardinalityUvarint(data, &offset)
	if err != nil {
		return LowCardinalityStringColumn{}, err
	}
	validityCount, err := readLowCardinalityUvarint(data, &offset)
	if err != nil {
		return LowCardinalityStringColumn{}, err
	}
	if dictionaryCount > MaxLowCardinalityStringDictionaryValues || rowCount > MaxLowCardinalityStringColumnRows {
		return LowCardinalityStringColumn{}, ErrLowCardinalityStringBinaryLimit
	}
	if dictionaryCount > uint64(len(data)) || rowCount > uint64(len(data)) {
		return LowCardinalityStringColumn{}, ErrLowCardinalityStringBinaryInvalid
	}
	rowCountInt, dictionaryCountInt, ok := lowCardinalityUint64ToInts(rowCount, dictionaryCount)
	if !ok {
		return LowCardinalityStringColumn{}, ErrLowCardinalityStringBinaryLimit
	}
	expectedValidity := uint64((rowCountInt + 7) / 8)
	if validityCount != 0 && validityCount != expectedValidity {
		return LowCardinalityStringColumn{}, fmt.Errorf("%w: validity length %d, want %d", ErrLowCardinalityStringBinaryInvalid, validityCount, expectedValidity)
	}
	if validityCount > uint64(len(data)-offset) {
		return LowCardinalityStringColumn{}, ErrLowCardinalityStringBinaryInvalid
	}
	var valid []uint64
	if validityCount != 0 {
		valid = make([]uint64, (rowCountInt+63)/64)
		for index := uint64(0); index < validityCount; index++ {
			valid[index>>3] |= uint64(data[offset+int(index)]) << uint((index&7)*8)
		}
		offset += int(validityCount)
	}
	dictionary := make([]string, dictionaryCountInt)
	for index := range dictionary {
		length, lengthErr := readLowCardinalityUvarint(data, &offset)
		if lengthErr != nil {
			return LowCardinalityStringColumn{}, lengthErr
		}
		if length > uint64(len(data)-offset) {
			return LowCardinalityStringColumn{}, ErrLowCardinalityStringBinaryInvalid
		}
		dictionary[index] = string(data[offset : offset+int(length)])
		offset += int(length)
		if index > 0 && dictionary[index-1] >= dictionary[index] {
			return LowCardinalityStringColumn{}, ErrLowCardinalityStringBinaryInvalid
		}
	}
	width := lowCardinalityStringCodeWidth(dictionaryCountInt)
	codeBytes := uint64(rowCountInt) * uint64(width)
	if codeBytes > uint64(len(data)-offset) || int(codeBytes) != len(data)-offset {
		return LowCardinalityStringColumn{}, ErrLowCardinalityStringBinaryInvalid
	}
	codeBytesInt := int(codeBytes)
	codes := append([]byte(nil), data[offset:offset+codeBytesInt]...)
	for index := 0; index < rowCountInt; index++ {
		code := lowCardinalityCodeAt(codes, index, width)
		if code >= uint32(dictionaryCountInt) {
			return LowCardinalityStringColumn{}, ErrLowCardinalityStringBinaryInvalid
		}
	}
	column := LowCardinalityStringColumn{dictionary: dictionary, codes: codes, width: uint8(width), valid: valid}
	if err := column.validate(); err != nil {
		return LowCardinalityStringColumn{}, err
	}
	return column, nil
}

func (column LowCardinalityStringColumn) validate() error {
	if len(column.dictionary) > MaxLowCardinalityStringDictionaryValues || column.Len() > MaxLowCardinalityStringColumnRows {
		return ErrLowCardinalityStringBinaryLimit
	}
	for index := 1; index < len(column.dictionary); index++ {
		if column.dictionary[index-1] >= column.dictionary[index] {
			return ErrLowCardinalityStringBinaryInvalid
		}
	}
	width := column.codeWidth()
	if width != lowCardinalityStringCodeWidth(len(column.dictionary)) || len(column.codes)%width != 0 {
		return ErrLowCardinalityStringBinaryInvalid
	}
	if column.valid != nil && len(column.valid) != (column.Len()+63)/64 {
		return ErrLowCardinalityStringBinaryInvalid
	}
	if len(column.dictionary) == 0 {
		for index := 0; index < column.Len(); index++ {
			if column.rowValid(index) {
				return ErrLowCardinalityStringBinaryInvalid
			}
		}
	}
	for index := 0; index < column.Len(); index++ {
		code := column.codeAt(index)
		if code >= uint32(len(column.dictionary)) && len(column.dictionary) != 0 {
			return ErrLowCardinalityStringBinaryInvalid
		}
		if len(column.dictionary) == 0 && code != 0 {
			return ErrLowCardinalityStringBinaryInvalid
		}
	}
	return nil
}

func packLowCardinalityCodes(codes []uint32, width int) []byte {
	packed := make([]byte, len(codes)*width)
	for index, code := range codes {
		offset := index * width
		switch width {
		case 1:
			packed[offset] = byte(code)
		case 2:
			binary.LittleEndian.PutUint16(packed[offset:offset+2], uint16(code))
		default:
			binary.LittleEndian.PutUint32(packed[offset:offset+4], code)
		}
	}
	return packed
}

func lowCardinalityCodeAt(codes []byte, row, width int) uint32 {
	offset := row * width
	switch width {
	case 1:
		return uint32(codes[offset])
	case 2:
		return uint32(binary.LittleEndian.Uint16(codes[offset : offset+2]))
	default:
		return binary.LittleEndian.Uint32(codes[offset : offset+4])
	}
}

func appendLowCardinalityUvarint(destination []byte, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(buffer[:], value)
	return append(destination, buffer[:length]...)
}

func readLowCardinalityUvarint(data []byte, offset *int) (uint64, error) {
	if *offset < 0 || *offset >= len(data) {
		return 0, ErrLowCardinalityStringBinaryInvalid
	}
	value, length := binary.Uvarint(data[*offset:])
	if length == 0 || length < 0 {
		return 0, ErrLowCardinalityStringBinaryInvalid
	}
	*offset += length
	return value, nil
}

func lowCardinalityUint64ToInts(rowCount, dictionaryCount uint64) (int, int, bool) {
	maxInt := uint64(^uint(0) >> 1)
	if rowCount > maxInt || dictionaryCount > maxInt {
		return 0, 0, false
	}
	return int(rowCount), int(dictionaryCount), true
}
