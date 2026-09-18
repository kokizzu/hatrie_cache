package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sort"
)

const (
	// DefaultSealedUpsertRunMaxRecords bounds one sealed run by default.
	DefaultSealedUpsertRunMaxRecords = 1 << 20
	// DefaultSealedUpsertRunMaxKeyBytes bounds one key in a sealed run.
	DefaultSealedUpsertRunMaxKeyBytes = 1 << 20
	// DefaultSealedUpsertRunMaxValueBytes bounds one value in a sealed run.
	DefaultSealedUpsertRunMaxValueBytes = 64 << 20
	// DefaultSealedUpsertRunMaxWireBytes bounds one encoded sealed run.
	DefaultSealedUpsertRunMaxWireBytes = 256 << 20
	// DefaultSealedUpsertRunIndexStride stores one lookup restart point per 64 records.
	DefaultSealedUpsertRunIndexStride = 16
	sealedUpsertRunHeaderSize         = 32
	sealedUpsertRunVersion            = 1
)

var sealedUpsertRunMagic = [4]byte{'H', 'U', 'R', '1'}

var (
	ErrSealedUpsertRunNil           = errors.New("hatDataStructure: sealed upsert run is nil")
	ErrSealedUpsertRunOptions       = errors.New("hatDataStructure: sealed upsert run options are invalid")
	ErrSealedUpsertRunKeyRequired   = errors.New("hatDataStructure: sealed upsert run key is required")
	ErrSealedUpsertRunKeyTooLarge   = errors.New("hatDataStructure: sealed upsert run key is too large")
	ErrSealedUpsertRunValueTooLarge = errors.New("hatDataStructure: sealed upsert run value is too large")
	ErrSealedUpsertRunRecordLimit   = errors.New("hatDataStructure: sealed upsert run record limit exceeded")
	ErrSealedUpsertRunWireLimit     = errors.New("hatDataStructure: sealed upsert run wire limit exceeded")
	ErrSealedUpsertRunCorrupt       = errors.New("hatDataStructure: sealed upsert run is corrupt")
)

// SealedUpsertRunOptions bounds and tunes one immutable run. Zero values select
// conservative defaults. IndexStride controls the number of records scanned
// after a binary-search restart point during Lookup.
type SealedUpsertRunOptions struct {
	MaxRecords    int
	MaxKeyBytes   int
	MaxValueBytes int
	MaxWireBytes  int
	IndexStride   int
}

// SealedUpsertRecord is one final keyed update in a sealed run. Deleted records
// are tombstones and never expose a value.
type SealedUpsertRecord struct {
	Key     string
	Value   []byte
	Deleted bool
}

// SealedUpsertRun is a sorted, immutable, CRC-protected update run. It keeps
// front-coded records in one binary buffer and a sparse key index for bounded
// point lookup. New runs consolidate duplicate input keys with last-write-wins
// semantics before sorting and sealing.
type SealedUpsertRun struct {
	data        []byte
	index       []sealedUpsertRunIndexEntry
	recordCount int
	indexStride int
}

type sealedUpsertRunIndexEntry struct {
	key    string
	offset int
	index  int
}

type sealedUpsertRunConfig struct {
	maxRecords    int
	maxKeyBytes   int
	maxValueBytes int
	maxWireBytes  int
	indexStride   int
}

// NewSealedUpsertRun consolidates records by key, retains the last operation
// for each key, sorts keys deterministically, and seals an immutable binary run.
// Input values are read during this call and are not retained by the result.
func NewSealedUpsertRun(records []UpsertRecord[[]byte], options SealedUpsertRunOptions) (*SealedUpsertRun, error) {
	config, err := normalizeSealedUpsertRunOptions(options)
	if err != nil {
		return nil, err
	}
	if len(records) > config.maxRecords {
		return nil, ErrSealedUpsertRunRecordLimit
	}
	latest := make(map[string]UpsertRecord[[]byte], len(records))
	for _, record := range records {
		if record.Key == "" {
			return nil, ErrSealedUpsertRunKeyRequired
		}
		if len(record.Key) > config.maxKeyBytes {
			return nil, ErrSealedUpsertRunKeyTooLarge
		}
		if !record.Deleted && len(record.Value) > config.maxValueBytes {
			return nil, ErrSealedUpsertRunValueTooLarge
		}
		if record.Deleted {
			record.Value = nil
		}
		latest[record.Key] = record
	}
	unique := make([]UpsertRecord[[]byte], 0, len(latest))
	for _, record := range latest {
		unique = append(unique, record)
	}
	sort.Slice(unique, func(left, right int) bool { return unique[left].Key < unique[right].Key })
	return encodeSealedUpsertRun(unique, config)
}

// UnmarshalSealedUpsertRun copies and strictly validates an encoded immutable
// run, including all bounded lengths, sorted keys, restart points, and CRC.
func UnmarshalSealedUpsertRun(data []byte, options SealedUpsertRunOptions) (*SealedUpsertRun, error) {
	config, err := normalizeSealedUpsertRunOptions(options)
	if err != nil {
		return nil, err
	}
	if len(data) > config.maxWireBytes {
		return nil, ErrSealedUpsertRunWireLimit
	}
	if len(data) < sealedUpsertRunHeaderSize {
		return nil, ErrSealedUpsertRunCorrupt
	}
	owned := append([]byte(nil), data...)
	index, recordCount, indexStride, err := validateSealedUpsertRun(owned, config)
	if err != nil {
		return nil, err
	}
	return &SealedUpsertRun{
		data:        owned,
		index:       index,
		recordCount: recordCount,
		indexStride: indexStride,
	}, nil
}

// Len returns the number of distinct final operations in the run.
func (run *SealedUpsertRun) Len() int {
	if run == nil {
		return 0
	}
	return run.recordCount
}

// WireBytes returns the encoded run size in bytes.
func (run *SealedUpsertRun) WireBytes() int {
	if run == nil {
		return 0
	}
	return len(run.data)
}

// Lookup returns an independent record copy for key. The sparse index bounds
// the scan to at most IndexStride records for a present key.
func (run *SealedUpsertRun) Lookup(key string) (SealedUpsertRecord, bool) {
	if run == nil || key == "" || len(run.index) == 0 {
		return SealedUpsertRecord{}, false
	}
	index := sort.Search(len(run.index), func(index int) bool { return run.index[index].key > key }) - 1
	if index < 0 {
		return SealedUpsertRecord{}, false
	}
	offset := run.index[index].offset
	recordIndex := run.index[index].index
	var keyScratchStorage [256]byte
	keyScratch := keyScratchStorage[:0]
	for ; recordIndex < run.recordCount; recordIndex++ {
		decodedKey, decodedValue, deleted, nextOffset, ok := decodeSealedUpsertRunRecordInto(run.data, offset, keyScratch[:0], keyScratch, len(run.data), len(run.data), recordIndex%run.indexStride == 0)
		if !ok {
			return SealedUpsertRecord{}, false
		}
		comparison := compareSealedUpsertRunBytesString(decodedKey, key)
		if comparison == 0 {
			return SealedUpsertRecord{Key: key, Value: append([]byte(nil), decodedValue...), Deleted: deleted}, true
		}
		if comparison > 0 {
			return SealedUpsertRecord{}, false
		}
		keyScratch = decodedKey
		offset = nextOffset
	}
	return SealedUpsertRecord{}, false
}

// ForEach visits records in sorted key order. Values are copied before they
// are passed to the visitor so callers cannot mutate the sealed run.
func (run *SealedUpsertRun) ForEach(visit func(SealedUpsertRecord)) {
	if run == nil || visit == nil {
		return
	}
	offset := sealedUpsertRunHeaderSize
	previousKey := ""
	for index := 0; index < run.recordCount; index++ {
		decoded, nextOffset, ok := decodeSealedUpsertRunRecord(run.data, offset, previousKey, len(run.data), len(run.data), index%run.indexStride == 0)
		if !ok {
			return
		}
		visit(SealedUpsertRecord{Key: decoded.Key, Value: append([]byte(nil), decoded.Value...), Deleted: decoded.Deleted})
		previousKey = decoded.Key
		offset = nextOffset
	}
}

// MarshalBinary returns an independent copy of the sealed run bytes.
func (run *SealedUpsertRun) MarshalBinary() ([]byte, error) {
	if run == nil {
		return nil, ErrSealedUpsertRunNil
	}
	return append([]byte(nil), run.data...), nil
}

func normalizeSealedUpsertRunOptions(options SealedUpsertRunOptions) (sealedUpsertRunConfig, error) {
	if options.MaxRecords < 0 || options.MaxKeyBytes < 0 || options.MaxValueBytes < 0 || options.MaxWireBytes < 0 || options.IndexStride < 0 {
		return sealedUpsertRunConfig{}, ErrSealedUpsertRunOptions
	}
	config := sealedUpsertRunConfig{
		maxRecords:    options.MaxRecords,
		maxKeyBytes:   options.MaxKeyBytes,
		maxValueBytes: options.MaxValueBytes,
		maxWireBytes:  options.MaxWireBytes,
		indexStride:   options.IndexStride,
	}
	if config.maxRecords == 0 {
		config.maxRecords = DefaultSealedUpsertRunMaxRecords
	}
	if config.maxKeyBytes == 0 {
		config.maxKeyBytes = DefaultSealedUpsertRunMaxKeyBytes
	}
	if config.maxValueBytes == 0 {
		config.maxValueBytes = DefaultSealedUpsertRunMaxValueBytes
	}
	if config.maxWireBytes == 0 {
		config.maxWireBytes = DefaultSealedUpsertRunMaxWireBytes
	}
	if config.indexStride == 0 {
		config.indexStride = DefaultSealedUpsertRunIndexStride
	}
	if uint64(config.maxRecords) > uint64(^uint32(0)) || uint64(config.indexStride) > uint64(^uint32(0)) {
		return sealedUpsertRunConfig{}, ErrSealedUpsertRunOptions
	}
	return config, nil
}

func encodeSealedUpsertRun(records []UpsertRecord[[]byte], config sealedUpsertRunConfig) (*SealedUpsertRun, error) {
	if len(records) > config.maxRecords {
		return nil, ErrSealedUpsertRunRecordLimit
	}
	bodyBytes, err := sealedUpsertRunBodyBytes(records, config)
	if err != nil {
		return nil, err
	}
	wireBytes := sealedUpsertRunHeaderSize + bodyBytes
	if wireBytes > config.maxWireBytes {
		return nil, ErrSealedUpsertRunWireLimit
	}
	data := make([]byte, wireBytes)
	copy(data[:4], sealedUpsertRunMagic[:])
	binary.LittleEndian.PutUint16(data[4:6], sealedUpsertRunVersion)
	binary.LittleEndian.PutUint32(data[8:12], uint32(len(records)))
	binary.LittleEndian.PutUint32(data[12:16], uint32(config.indexStride))
	binary.LittleEndian.PutUint64(data[16:24], uint64(bodyBytes))
	offset := sealedUpsertRunHeaderSize
	previousKey := ""
	index := make([]sealedUpsertRunIndexEntry, 0, (len(records)+config.indexStride-1)/config.indexStride)
	for recordIndex, record := range records {
		recordOffset := offset
		restart := recordIndex%config.indexStride == 0
		prefix := 0
		if !restart {
			prefix = sealedUpsertRunCommonPrefix(previousKey, record.Key)
		}
		offset = putSealedUpsertRunUvarint(data, offset, uint64(prefix))
		offset = putSealedUpsertRunUvarint(data, offset, uint64(len(record.Key)-prefix))
		valueLength := len(record.Value)
		if record.Deleted {
			valueLength = 0
		}
		offset = putSealedUpsertRunUvarint(data, offset, uint64(valueLength))
		if record.Deleted {
			data[offset] = 1
		} else {
			data[offset] = 0
		}
		offset++
		copy(data[offset:], record.Key[prefix:])
		offset += len(record.Key) - prefix
		if valueLength > 0 {
			copy(data[offset:], record.Value)
			offset += valueLength
		}
		if restart {
			index = append(index, sealedUpsertRunIndexEntry{key: record.Key, offset: recordOffset, index: recordIndex})
		}
		previousKey = record.Key
	}
	binary.LittleEndian.PutUint32(data[24:28], crc32.ChecksumIEEE(data[sealedUpsertRunHeaderSize:]))
	return &SealedUpsertRun{data: data, index: index, recordCount: len(records), indexStride: config.indexStride}, nil
}

func sealedUpsertRunBodyBytes(records []UpsertRecord[[]byte], config sealedUpsertRunConfig) (int, error) {
	bodyBytes := 0
	previousKey := ""
	for index, record := range records {
		if record.Key == "" {
			return 0, ErrSealedUpsertRunKeyRequired
		}
		if len(record.Key) > config.maxKeyBytes {
			return 0, ErrSealedUpsertRunKeyTooLarge
		}
		valueLength := len(record.Value)
		if record.Deleted {
			valueLength = 0
		} else if valueLength > config.maxValueBytes {
			return 0, ErrSealedUpsertRunValueTooLarge
		}
		prefix := 0
		if index%config.indexStride != 0 {
			prefix = sealedUpsertRunCommonPrefix(previousKey, record.Key)
		}
		recordBytes := sealedUpsertRunUvarintSize(uint64(prefix)) +
			sealedUpsertRunUvarintSize(uint64(len(record.Key)-prefix)) +
			sealedUpsertRunUvarintSize(uint64(valueLength)) + 1 + len(record.Key) - prefix + valueLength
		if recordBytes < 0 || bodyBytes > config.maxWireBytes-sealedUpsertRunHeaderSize-recordBytes {
			return 0, ErrSealedUpsertRunWireLimit
		}
		bodyBytes += recordBytes
		previousKey = record.Key
	}
	return bodyBytes, nil
}

func validateSealedUpsertRun(data []byte, config sealedUpsertRunConfig) ([]sealedUpsertRunIndexEntry, int, int, error) {
	if len(data) > config.maxWireBytes {
		return nil, 0, 0, ErrSealedUpsertRunWireLimit
	}
	if len(data) < sealedUpsertRunHeaderSize || !equalSealedUpsertRunMagic(data[:4]) {
		return nil, 0, 0, ErrSealedUpsertRunCorrupt
	}
	if binary.LittleEndian.Uint16(data[4:6]) != sealedUpsertRunVersion || binary.LittleEndian.Uint16(data[6:8]) != 0 || binary.LittleEndian.Uint32(data[28:32]) != 0 {
		return nil, 0, 0, ErrSealedUpsertRunCorrupt
	}
	recordCount64 := uint64(binary.LittleEndian.Uint32(data[8:12]))
	if recordCount64 > uint64(config.maxRecords) {
		return nil, 0, 0, ErrSealedUpsertRunRecordLimit
	}
	indexStride64 := uint64(binary.LittleEndian.Uint32(data[12:16]))
	if indexStride64 == 0 {
		return nil, 0, 0, ErrSealedUpsertRunCorrupt
	}
	bodyBytes := uint64(binary.LittleEndian.Uint64(data[16:24]))
	if bodyBytes != uint64(len(data)-sealedUpsertRunHeaderSize) || binary.LittleEndian.Uint32(data[24:28]) != crc32.ChecksumIEEE(data[sealedUpsertRunHeaderSize:]) {
		return nil, 0, 0, ErrSealedUpsertRunCorrupt
	}
	recordCount := int(recordCount64)
	indexStride := int(indexStride64)
	index := make([]sealedUpsertRunIndexEntry, 0, (recordCount+indexStride-1)/indexStride)
	offset := sealedUpsertRunHeaderSize
	var firstKeyStorage [256]byte
	var secondKeyStorage [256]byte
	previousKey := firstKeyStorage[:0]
	currentKey := secondKeyStorage[:0]
	for recordIndex := 0; recordIndex < recordCount; recordIndex++ {
		recordOffset := offset
		restart := recordIndex%indexStride == 0
		key, value, deleted, nextOffset, ok := decodeSealedUpsertRunRecordInto(data, offset, currentKey[:0], previousKey, config.maxKeyBytes, config.maxValueBytes, restart)
		if !ok || (recordIndex > 0 && compareSealedUpsertRunBytes(key, previousKey) <= 0) {
			return nil, 0, 0, ErrSealedUpsertRunCorrupt
		}
		if restart {
			index = append(index, sealedUpsertRunIndexEntry{key: string(key), offset: recordOffset, index: recordIndex})
		}
		_ = value
		_ = deleted
		previousKey, currentKey = key, previousKey
		offset = nextOffset
	}
	if offset != len(data) {
		return nil, 0, 0, ErrSealedUpsertRunCorrupt
	}
	return index, recordCount, indexStride, nil
}

func decodeSealedUpsertRunRecord(data []byte, offset int, previousKey string, maxKeyBytes, maxValueBytes int, restart bool) (SealedUpsertRecord, int, bool) {
	key, value, deleted, nextOffset, ok := decodeSealedUpsertRunRecordWithOffset(data, offset, previousKey, maxKeyBytes, maxValueBytes, restart)
	if !ok {
		return SealedUpsertRecord{}, offset, false
	}
	return SealedUpsertRecord{Key: key, Value: value, Deleted: deleted}, nextOffset, true
}

func decodeSealedUpsertRunRecordInto(data []byte, offset int, destination, previousKey []byte, maxKeyBytes, maxValueBytes int, restart bool) ([]byte, []byte, bool, int, bool) {
	prefix, ok := readSealedUpsertRunUvarint(data, &offset)
	if !ok || (restart && prefix != 0) || prefix > uint64(len(previousKey)) || prefix > uint64(maxKeyBytes) {
		return nil, nil, false, offset, false
	}
	suffixLength, ok := readSealedUpsertRunUvarint(data, &offset)
	if !ok || suffixLength > uint64(maxKeyBytes)-prefix || suffixLength > uint64(len(data)-offset) {
		return nil, nil, false, offset, false
	}
	valueLength, ok := readSealedUpsertRunUvarint(data, &offset)
	if !ok || valueLength > uint64(maxValueBytes) || valueLength > uint64(len(data)-offset-1) {
		return nil, nil, false, offset, false
	}
	if offset >= len(data) {
		return nil, nil, false, offset, false
	}
	flags := data[offset]
	offset++
	if flags > 1 || (flags == 1 && valueLength != 0) || valueLength > uint64(len(data)-offset)-suffixLength {
		return nil, nil, false, offset, false
	}
	key := append(destination, previousKey[:int(prefix)]...)
	key = append(key, data[offset:offset+int(suffixLength)]...)
	offset += int(suffixLength)
	value := data[offset : offset+int(valueLength)]
	offset += int(valueLength)
	if len(key) == 0 {
		return nil, nil, false, offset, false
	}
	return key, value, flags == 1, offset, true
}

func compareSealedUpsertRunBytesString(left []byte, right string) int {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}
	for index := 0; index < limit; index++ {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	if len(left) < len(right) {
		return -1
	}
	if len(left) > len(right) {
		return 1
	}
	return 0
}

func compareSealedUpsertRunBytes(left, right []byte) int {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}
	for index := 0; index < limit; index++ {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	if len(left) < len(right) {
		return -1
	}
	if len(left) > len(right) {
		return 1
	}
	return 0
}

func decodeSealedUpsertRunRecordWithOffset(data []byte, offset int, previousKey string, maxKeyBytes, maxValueBytes int, restart bool) (string, []byte, bool, int, bool) {
	prefix, ok := readSealedUpsertRunUvarint(data, &offset)
	if !ok || (restart && prefix != 0) || prefix > uint64(len(previousKey)) {
		return "", nil, false, offset, false
	}
	suffixLength, ok := readSealedUpsertRunUvarint(data, &offset)
	if !ok || suffixLength > uint64(maxKeyBytes)-prefix || suffixLength > uint64(len(data)-offset) {
		return "", nil, false, offset, false
	}
	valueLength, ok := readSealedUpsertRunUvarint(data, &offset)
	if !ok || valueLength > uint64(maxValueBytes) || valueLength > uint64(len(data)-offset-1) {
		return "", nil, false, offset, false
	}
	if offset >= len(data) {
		return "", nil, false, offset, false
	}
	flags := data[offset]
	offset++
	if flags > 1 || (flags == 1 && valueLength != 0) || valueLength > uint64(len(data)-offset)-suffixLength {
		return "", nil, false, offset, false
	}
	key := previousKey[:int(prefix)] + string(data[offset:offset+int(suffixLength)])
	offset += int(suffixLength)
	value := data[offset : offset+int(valueLength)]
	offset += int(valueLength)
	if key == "" {
		return "", nil, false, offset, false
	}
	return key, value, flags == 1, offset, true
}

func equalSealedUpsertRunMagic(value []byte) bool {
	return len(value) >= len(sealedUpsertRunMagic) && value[0] == sealedUpsertRunMagic[0] && value[1] == sealedUpsertRunMagic[1] && value[2] == sealedUpsertRunMagic[2] && value[3] == sealedUpsertRunMagic[3]
}

func sealedUpsertRunCommonPrefix(left, right string) int {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}
	index := 0
	for index < limit && left[index] == right[index] {
		index++
	}
	return index
}

func sealedUpsertRunUvarintSize(value uint64) int {
	size := 1
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}

func putSealedUpsertRunUvarint(data []byte, offset int, value uint64) int {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], value)
	copy(data[offset:], encoded[:length])
	return offset + length
}

func readSealedUpsertRunUvarint(data []byte, offset *int) (uint64, bool) {
	if *offset >= len(data) {
		return 0, false
	}
	value, length := binary.Uvarint(data[*offset:])
	if length <= 0 {
		return 0, false
	}
	*offset += length
	return value, true
}
