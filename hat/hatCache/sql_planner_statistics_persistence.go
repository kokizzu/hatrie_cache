package hatCache

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync/atomic"
)

const (
	sqlPlannerStatisticsFileMagic       = "HPS1"
	sqlPlannerStatisticsFileVersion     = 1
	sqlPlannerStatisticsMaxFileBytes    = 16 << 20
	sqlPlannerStatisticsMaxFields       = 4096
	sqlPlannerStatisticsMaxHistogram    = 1024
	sqlPlannerStatisticsFileHeaderBytes = len(sqlPlannerStatisticsFileMagic) + 1 + 4
)

// SQLPlannerStatisticsLoadReport describes an explicit planner-statistics
// restore. Entries whose source is absent or whose source bytes changed are
// skipped instead of becoming unsafe estimates.
type SQLPlannerStatisticsLoadReport struct {
	Loaded  int `json:"loaded"`
	Skipped int `json:"skipped"`
}

type sqlPlannerStatisticsPersistedEntry struct {
	key    string
	digest [sha256.Size]byte
	value  SQLWhatIfSourceStatistics
}

// SaveSQLPlannerStatistics writes the current explicit ANALYZE cache to a
// bounded, CRC-protected binary file. It is not called by normal mutations or
// queries; callers can schedule it alongside their durability workflow.
func (ht *HatTrie) SaveSQLPlannerStatistics(path string) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	entries, epoch, err := ht.snapshotSQLPlannerStatisticsForPersistence()
	if err != nil {
		return err
	}
	payload, err := encodeSQLPlannerStatisticsPayload(entries)
	if err != nil {
		return err
	}
	if len(payload) > sqlPlannerStatisticsMaxFileBytes-sqlPlannerStatisticsFileHeaderBytes-4 {
		return fmt.Errorf("hatriecache: planner statistics file exceeds %d bytes", sqlPlannerStatisticsMaxFileBytes)
	}
	return writeFileAtomicStream(path, func(writer io.Writer) error {
		if atomic.LoadUint64(&ht.mutationEpoch) != epoch {
			return errors.New("hatriecache: source changed while saving planner statistics")
		}
		var header [sqlPlannerStatisticsFileHeaderBytes]byte
		copy(header[:len(sqlPlannerStatisticsFileMagic)], sqlPlannerStatisticsFileMagic)
		header[len(sqlPlannerStatisticsFileMagic)] = sqlPlannerStatisticsFileVersion
		binary.LittleEndian.PutUint32(header[len(sqlPlannerStatisticsFileMagic)+1:], uint32(len(payload)))
		if _, err := writer.Write(header[:]); err != nil {
			return err
		}
		if _, err := writer.Write(payload); err != nil {
			return err
		}
		var checksum [4]byte
		binary.LittleEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(payload))
		_, err := writer.Write(checksum[:])
		return err
	})
}

// LoadSQLPlannerStatistics loads explicit planner statistics after the cache
// source has been restored. Each entry is checked against the current raw
// source bytes, so a file from another cache state is harmlessly ignored.
func (ht *HatTrie) LoadSQLPlannerStatistics(path string) (SQLPlannerStatisticsLoadReport, error) {
	if ht == nil {
		return SQLPlannerStatisticsLoadReport{}, ErrNilHatTrie
	}
	data, err := readSQLPlannerStatisticsFile(path)
	if err != nil {
		return SQLPlannerStatisticsLoadReport{}, err
	}
	persisted, err := decodeSQLPlannerStatisticsPayload(data)
	if err != nil {
		return SQLPlannerStatisticsLoadReport{}, err
	}
	startEpoch := atomic.LoadUint64(&ht.mutationEpoch)
	loaded := make(map[string]sqlPlannerStatisticsEntry, len(persisted))
	report := SQLPlannerStatisticsLoadReport{}
	for _, entry := range persisted {
		name, key, ok := splitSQLPlannerStatisticsKey(entry.key)
		if !ok || name != "CACHE" || !ht.Exists(key) {
			report.Skipped++
			continue
		}
		raw, err := ht.GetBytesChecked(key)
		if err != nil {
			return SQLPlannerStatisticsLoadReport{}, err
		}
		if sha256.Sum256(raw) != entry.digest {
			report.Skipped++
			continue
		}
		if atomic.LoadUint64(&ht.mutationEpoch) != startEpoch {
			return SQLPlannerStatisticsLoadReport{}, errors.New("hatriecache: source changed while loading planner statistics")
		}
		loaded[entry.key] = sqlPlannerStatisticsEntry{epoch: startEpoch, value: entry.value}
		report.Loaded++
	}
	ht.sqlPlannerStatisticsMu.Lock()
	defer ht.sqlPlannerStatisticsMu.Unlock()
	if atomic.LoadUint64(&ht.mutationEpoch) != startEpoch {
		return SQLPlannerStatisticsLoadReport{}, errors.New("hatriecache: source changed while loading planner statistics")
	}
	ht.sqlPlannerStatistics = loaded
	return report, nil
}

func (ht *HatTrie) snapshotSQLPlannerStatisticsForPersistence() ([]sqlPlannerStatisticsPersistedEntry, uint64, error) {
	epoch := atomic.LoadUint64(&ht.mutationEpoch)
	ht.sqlPlannerStatisticsMu.RLock()
	entries := make([]sqlPlannerStatisticsPersistedEntry, 0, len(ht.sqlPlannerStatistics))
	for key, entry := range ht.sqlPlannerStatistics {
		entries = append(entries, sqlPlannerStatisticsPersistedEntry{
			key:   key,
			value: cloneSQLWhatIfSourceStatistics(entry.value),
		})
	}
	ht.sqlPlannerStatisticsMu.RUnlock()
	sort.Slice(entries, func(left, right int) bool { return entries[left].key < entries[right].key })
	for index := range entries {
		name, key, ok := splitSQLPlannerStatisticsKey(entries[index].key)
		if !ok || name != "CACHE" || !ht.Exists(key) {
			return nil, 0, fmt.Errorf("hatriecache: planner statistics source %q disappeared", entries[index].key)
		}
		raw, err := ht.GetBytesChecked(key)
		if err != nil {
			return nil, 0, err
		}
		entries[index].digest = sha256.Sum256(raw)
		if atomic.LoadUint64(&ht.mutationEpoch) != epoch {
			return nil, 0, errors.New("hatriecache: source changed while saving planner statistics")
		}
	}
	return entries, epoch, nil
}

func splitSQLPlannerStatisticsKey(value string) (string, string, bool) {
	separator := strings.IndexByte(value, 0)
	if separator <= 0 || separator == len(value)-1 {
		return "", "", false
	}
	return value[:separator], value[separator+1:], true
}

func encodeSQLPlannerStatisticsPayload(entries []sqlPlannerStatisticsPersistedEntry) ([]byte, error) {
	if len(entries) > maxSQLPlannerStatisticsEntries {
		return nil, fmt.Errorf("hatriecache: planner statistics entry count %d exceeds %d", len(entries), maxSQLPlannerStatisticsEntries)
	}
	payload := make([]byte, 0, 256)
	payload = appendSQLPlannerStatisticsUvarint(payload, uint64(len(entries)))
	for _, entry := range entries {
		payload = appendSQLPlannerStatisticsString(payload, entry.key)
		payload = append(payload, entry.digest[:]...)
		if err := validateSQLPlannerStatisticsValue(entry.value); err != nil {
			return nil, err
		}
		payload = appendSQLPlannerStatisticsUvarint(payload, uint64(entry.value.Rows))
		payload = appendSQLPlannerStatisticsUvarint(payload, uint64(entry.value.Bytes))
		fields := make([]string, 0, len(entry.value.Fields))
		for field := range entry.value.Fields {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		if len(fields) > sqlPlannerStatisticsMaxFields {
			return nil, fmt.Errorf("hatriecache: planner statistics field count %d exceeds %d", len(fields), sqlPlannerStatisticsMaxFields)
		}
		payload = appendSQLPlannerStatisticsUvarint(payload, uint64(len(fields)))
		for _, field := range fields {
			statistics := entry.value.Fields[field]
			payload = appendSQLPlannerStatisticsString(payload, field)
			payload = appendSQLPlannerStatisticsUvarint(payload, uint64(statistics.Rows))
			payload = appendSQLPlannerStatisticsUvarint(payload, uint64(statistics.NullRows))
			payload = appendSQLPlannerStatisticsUvarint(payload, uint64(statistics.DistinctValues))
			payload = appendSQLPlannerStatisticsUvarint(payload, uint64(statistics.AverageValueBytes))
			var bounds byte
			if statistics.Minimum != nil {
				bounds |= 1
			}
			if statistics.Maximum != nil {
				bounds |= 2
			}
			payload = append(payload, bounds)
			for _, bound := range []interface{}{statistics.Minimum, statistics.Maximum} {
				if bound == nil {
					continue
				}
				value, ok := bound.(float64)
				if !ok || math.IsNaN(value) {
					return nil, fmt.Errorf("hatriecache: planner statistics field %q has unsupported numeric bound", field)
				}
				var encoded [8]byte
				binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(value))
				payload = append(payload, encoded[:]...)
			}
			if len(statistics.FrequencyHistogram) > sqlPlannerStatisticsMaxHistogram {
				return nil, fmt.Errorf("hatriecache: planner statistics field %q histogram exceeds %d buckets", field, sqlPlannerStatisticsMaxHistogram)
			}
			payload = appendSQLPlannerStatisticsUvarint(payload, uint64(len(statistics.FrequencyHistogram)))
			for _, bucket := range statistics.FrequencyHistogram {
				payload = appendSQLPlannerStatisticsUvarint(payload, uint64(bucket.RowsPerValue))
				payload = appendSQLPlannerStatisticsUvarint(payload, uint64(bucket.DistinctValues))
			}
		}
	}
	return payload, nil
}

func validateSQLPlannerStatisticsValue(value SQLWhatIfSourceStatistics) error {
	if value.Rows < 0 || value.Bytes < 0 || len(value.Fields) > sqlPlannerStatisticsMaxFields {
		return errors.New("hatriecache: invalid planner statistics counts")
	}
	for field, statistics := range value.Fields {
		if statistics.Rows < 0 || statistics.NullRows < 0 || statistics.DistinctValues < 0 || statistics.AverageValueBytes < 0 {
			return fmt.Errorf("hatriecache: invalid planner statistics field %q counts", field)
		}
		if statistics.Rows+statistics.NullRows != value.Rows || statistics.DistinctValues > statistics.Rows {
			return fmt.Errorf("hatriecache: inconsistent planner statistics field %q counts", field)
		}
		if statistics.Minimum != nil {
			if bound, ok := statistics.Minimum.(float64); !ok || math.IsNaN(bound) {
				return fmt.Errorf("hatriecache: unsupported planner statistics minimum for field %q", field)
			}
		}
		if statistics.Maximum != nil {
			if bound, ok := statistics.Maximum.(float64); !ok || math.IsNaN(bound) {
				return fmt.Errorf("hatriecache: unsupported planner statistics maximum for field %q", field)
			}
		}
		if statistics.Minimum != nil && statistics.Maximum != nil && statistics.Minimum.(float64) > statistics.Maximum.(float64) {
			return fmt.Errorf("hatriecache: inverted planner statistics bounds for field %q", field)
		}
		if len(statistics.FrequencyHistogram) > sqlPlannerStatisticsMaxHistogram {
			return fmt.Errorf("hatriecache: planner statistics field %q histogram is too large", field)
		}
		distinct := 0
		for _, bucket := range statistics.FrequencyHistogram {
			if bucket.RowsPerValue <= 0 || bucket.DistinctValues <= 0 {
				return fmt.Errorf("hatriecache: invalid planner statistics histogram for field %q", field)
			}
			if bucket.RowsPerValue > statistics.Rows {
				return fmt.Errorf("hatriecache: planner statistics histogram frequency exceeds field rows for %q", field)
			}
			if distinct > math.MaxInt-bucket.DistinctValues {
				return fmt.Errorf("hatriecache: planner statistics histogram overflows for field %q", field)
			}
			distinct += bucket.DistinctValues
		}
		if distinct != statistics.DistinctValues {
			return fmt.Errorf("hatriecache: planner statistics histogram does not cover field %q", field)
		}
	}
	return nil
}

func appendSQLPlannerStatisticsUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	return append(dst, encoded[:binary.PutUvarint(encoded[:], value)]...)
}

func appendSQLPlannerStatisticsString(dst []byte, value string) []byte {
	dst = appendSQLPlannerStatisticsUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func readSQLPlannerStatisticsFile(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Size() < int64(sqlPlannerStatisticsFileHeaderBytes+4) || info.Size() > int64(sqlPlannerStatisticsMaxFileBytes) {
		return nil, fmt.Errorf("hatriecache: planner statistics file size %d is outside bounds", info.Size())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < sqlPlannerStatisticsFileHeaderBytes+4 || string(data[:len(sqlPlannerStatisticsFileMagic)]) != sqlPlannerStatisticsFileMagic {
		return nil, errors.New("hatriecache: invalid planner statistics file header")
	}
	versionOffset := len(sqlPlannerStatisticsFileMagic)
	if data[versionOffset] != sqlPlannerStatisticsFileVersion {
		return nil, fmt.Errorf("hatriecache: unsupported planner statistics file version %d", data[versionOffset])
	}
	lengthOffset := versionOffset + 1
	payloadLength := int(binary.LittleEndian.Uint32(data[lengthOffset : lengthOffset+4]))
	payloadStart := sqlPlannerStatisticsFileHeaderBytes
	payloadEnd := payloadStart + payloadLength
	if payloadLength < 0 || payloadEnd+4 != len(data) {
		return nil, errors.New("hatriecache: invalid planner statistics payload length")
	}
	payload := data[payloadStart:payloadEnd]
	if binary.LittleEndian.Uint32(data[payloadEnd:]) != crc32.ChecksumIEEE(payload) {
		return nil, errors.New("hatriecache: planner statistics checksum mismatch")
	}
	return payload, nil
}

type sqlPlannerStatisticsPayloadReader struct {
	data []byte
	off  int
}

func (reader *sqlPlannerStatisticsPayloadReader) readUvarint(label string) (uint64, error) {
	if reader.off >= len(reader.data) {
		return 0, fmt.Errorf("hatriecache: planner statistics %s is truncated", label)
	}
	value, width := binary.Uvarint(reader.data[reader.off:])
	if width <= 0 {
		return 0, fmt.Errorf("hatriecache: invalid planner statistics %s", label)
	}
	reader.off += width
	return value, nil
}

func (reader *sqlPlannerStatisticsPayloadReader) readInt(label string) (int, error) {
	value, err := reader.readUvarint(label)
	if err != nil {
		return 0, err
	}
	maxInt := uint64(^uint(0) >> 1)
	if value > maxInt {
		return 0, fmt.Errorf("hatriecache: planner statistics %s exceeds int", label)
	}
	return int(value), nil
}

func (reader *sqlPlannerStatisticsPayloadReader) readString(label string, maxBytes int) (string, error) {
	length, err := reader.readInt(label + " length")
	if err != nil {
		return "", err
	}
	if length > maxBytes || length > len(reader.data)-reader.off {
		return "", fmt.Errorf("hatriecache: planner statistics %s is too large", label)
	}
	value := string(reader.data[reader.off : reader.off+length])
	reader.off += length
	return value, nil
}

func (reader *sqlPlannerStatisticsPayloadReader) readByte(label string) (byte, error) {
	if reader.off >= len(reader.data) {
		return 0, fmt.Errorf("hatriecache: planner statistics %s is truncated", label)
	}
	value := reader.data[reader.off]
	reader.off++
	return value, nil
}

func (reader *sqlPlannerStatisticsPayloadReader) readFloat64(label string) (float64, error) {
	if len(reader.data)-reader.off < 8 {
		return 0, fmt.Errorf("hatriecache: planner statistics %s is truncated", label)
	}
	value := math.Float64frombits(binary.LittleEndian.Uint64(reader.data[reader.off : reader.off+8]))
	reader.off += 8
	if math.IsNaN(value) {
		return 0, fmt.Errorf("hatriecache: planner statistics %s is NaN", label)
	}
	return value, nil
}

func decodeSQLPlannerStatisticsPayload(payload []byte) ([]sqlPlannerStatisticsPersistedEntry, error) {
	reader := sqlPlannerStatisticsPayloadReader{data: payload}
	count, err := reader.readInt("entry count")
	if err != nil {
		return nil, err
	}
	if count > maxSQLPlannerStatisticsEntries {
		return nil, fmt.Errorf("hatriecache: planner statistics entry count %d exceeds %d", count, maxSQLPlannerStatisticsEntries)
	}
	entries := make([]sqlPlannerStatisticsPersistedEntry, 0, count)
	seen := make(map[string]struct{}, count)
	for index := 0; index < count; index++ {
		key, err := reader.readString("entry key", sqlPlannerStatisticsMaxFileBytes)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("hatriecache: duplicate planner statistics entry %q", key)
		}
		seen[key] = struct{}{}
		if len(reader.data)-reader.off < sha256.Size {
			return nil, errors.New("hatriecache: planner statistics entry digest is truncated")
		}
		var digest [sha256.Size]byte
		copy(digest[:], reader.data[reader.off:reader.off+sha256.Size])
		reader.off += sha256.Size
		rows, err := reader.readInt("row count")
		if err != nil {
			return nil, err
		}
		bytes, err := reader.readInt("byte count")
		if err != nil {
			return nil, err
		}
		fieldCount, err := reader.readInt("field count")
		if err != nil {
			return nil, err
		}
		if fieldCount > sqlPlannerStatisticsMaxFields {
			return nil, fmt.Errorf("hatriecache: planner statistics field count %d exceeds %d", fieldCount, sqlPlannerStatisticsMaxFields)
		}
		value := SQLWhatIfSourceStatistics{Rows: rows, Bytes: bytes, Fields: make(map[string]SQLWhatIfFieldStatistics, fieldCount)}
		name, sourceKey, validKey := splitSQLPlannerStatisticsKey(key)
		if !validKey || name != "CACHE" {
			return nil, fmt.Errorf("hatriecache: invalid planner statistics source key %q", key)
		}
		value.Source = name + "(" + sourceKey + ")"
		for fieldIndex := 0; fieldIndex < fieldCount; fieldIndex++ {
			field, err := reader.readString("field name", sqlPlannerStatisticsMaxFileBytes)
			if err != nil {
				return nil, err
			}
			if _, exists := value.Fields[field]; exists {
				return nil, fmt.Errorf("hatriecache: duplicate planner statistics field %q", field)
			}
			fieldStatistics := SQLWhatIfFieldStatistics{}
			if fieldStatistics.Rows, err = reader.readInt("field row count"); err != nil {
				return nil, err
			}
			if fieldStatistics.NullRows, err = reader.readInt("field null count"); err != nil {
				return nil, err
			}
			if fieldStatistics.DistinctValues, err = reader.readInt("field distinct count"); err != nil {
				return nil, err
			}
			if fieldStatistics.AverageValueBytes, err = reader.readInt("field average byte count"); err != nil {
				return nil, err
			}
			bounds, err := reader.readByte("field bounds")
			if err != nil {
				return nil, err
			}
			if bounds&^byte(3) != 0 {
				return nil, fmt.Errorf("hatriecache: invalid planner statistics bounds for field %q", field)
			}
			if bounds&1 != 0 {
				minimum, readErr := reader.readFloat64("field minimum")
				if readErr != nil {
					return nil, readErr
				}
				fieldStatistics.Minimum = minimum
			}
			if bounds&2 != 0 {
				maximum, readErr := reader.readFloat64("field maximum")
				if readErr != nil {
					return nil, readErr
				}
				fieldStatistics.Maximum = maximum
			}
			histogramCount, err := reader.readInt("histogram count")
			if err != nil {
				return nil, err
			}
			if histogramCount > sqlPlannerStatisticsMaxHistogram {
				return nil, fmt.Errorf("hatriecache: planner statistics histogram count %d exceeds %d", histogramCount, sqlPlannerStatisticsMaxHistogram)
			}
			if histogramCount > 0 {
				fieldStatistics.FrequencyHistogram = make([]SQLWhatIfFrequencyBucket, histogramCount)
			}
			for histogramIndex := range fieldStatistics.FrequencyHistogram {
				if fieldStatistics.FrequencyHistogram[histogramIndex].RowsPerValue, err = reader.readInt("histogram frequency"); err != nil {
					return nil, err
				}
				if fieldStatistics.FrequencyHistogram[histogramIndex].DistinctValues, err = reader.readInt("histogram distinct count"); err != nil {
					return nil, err
				}
			}
			value.Fields[field] = fieldStatistics
		}
		if err := validateSQLPlannerStatisticsValue(value); err != nil {
			return nil, err
		}
		entries = append(entries, sqlPlannerStatisticsPersistedEntry{key: key, digest: digest, value: value})
	}
	if reader.off != len(reader.data) {
		return nil, errors.New("hatriecache: trailing planner statistics payload")
	}
	return entries, nil
}
