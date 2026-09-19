package hatSql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"unicode/utf8"
)

const (
	// SQLPreparedQueryCachePersistenceFormat identifies the durable cache
	// envelope. The persisted representation contains only parsed-query source
	// and schema namespaces; bound values are never written.
	SQLPreparedQueryCachePersistenceFormat = "hatrie-cache-sql-prepared-query/v1"

	// DefaultSQLPreparedQueryCachePersistenceMaxEntries bounds one snapshot by
	// default without changing the in-memory cache capacity.
	DefaultSQLPreparedQueryCachePersistenceMaxEntries = 256
	// MaxSQLPreparedQueryCachePersistenceMaxEntries prevents an accidental or
	// hostile restore from creating an unbounded parse workload.
	MaxSQLPreparedQueryCachePersistenceMaxEntries = 4096
	// DefaultSQLPreparedQueryCachePersistenceMaxBytes bounds one snapshot by
	// default.
	DefaultSQLPreparedQueryCachePersistenceMaxBytes = 8 << 20
	// MaxSQLPreparedQueryCachePersistenceMaxBytes is the hard upper bound for a
	// persisted prepared-query snapshot.
	MaxSQLPreparedQueryCachePersistenceMaxBytes = 64 << 20
	// MaxSQLPreparedQueryCachePersistenceSourceBytes bounds one SQL source.
	MaxSQLPreparedQueryCachePersistenceSourceBytes = 1 << 20
	// MaxSQLPreparedQueryCachePersistenceSchemaVersionBytes bounds one schema
	// namespace token.
	MaxSQLPreparedQueryCachePersistenceSchemaVersionBytes = 256
)

var (
	// ErrSQLPreparedQueryCachePersistenceInvalid identifies malformed or
	// unsupported persisted cache data.
	ErrSQLPreparedQueryCachePersistenceInvalid = errors.New("invalid SQL prepared-query cache persistence")
	// ErrSQLPreparedQueryCachePersistenceLimitExceeded identifies a snapshot
	// that exceeds a configured or hard safety bound.
	ErrSQLPreparedQueryCachePersistenceLimitExceeded = errors.New("SQL prepared-query cache persistence limit exceeded")
)

// SQLPreparedQueryCachePersistenceOptions controls durable cache snapshots.
// SchemaVersion is an exact opaque dependency token supplied by the caller;
// callers should derive it from every source schema, index, and projection
// dependency that can change query behavior.
type SQLPreparedQueryCachePersistenceOptions struct {
	MaxEntries int
	MaxBytes   int

	// SchemaVersion filters Save and is required for versioned Load entries.
	// An empty value matches only unversioned entries when AllowUnversioned is
	// true; this prevents accidentally restoring plans without validation.
	SchemaVersion string
	// AllowUnversioned explicitly permits loading entries with an empty schema
	// namespace. It is false by default for restart safety.
	AllowUnversioned bool
}

// SQLPreparedQueryCacheLoadReport describes a validated restore attempt.
type SQLPreparedQueryCacheLoadReport struct {
	Format        string
	Bytes         int
	Loaded        int
	SkippedSchema int
}

type sqlPreparedQueryCachePersistenceRecord struct {
	source        string
	schemaVersion string
}

type sqlPreparedQueryCachePersistenceParsedRecord struct {
	sqlPreparedQueryCachePersistenceRecord
	query *sqlQuery
	key   string
}

var sqlPreparedQueryCachePersistenceMagic = []byte("HATSQLPC\x01")

// Save writes a bounded, deterministic snapshot using atomic replacement. The
// newest records are retained when the configured entry bound is below the
// in-memory cache size.
func (cache *SQLPreparedQueryCache) Save(path string, options SQLPreparedQueryCachePersistenceOptions) error {
	maxEntries, maxBytes, err := normalizeSQLPreparedQueryCachePersistenceOptions(options)
	if err != nil {
		return err
	}
	if cache == nil {
		return fmt.Errorf("%w: cache is nil", ErrSQLPreparedQueryCachePersistenceInvalid)
	}
	if path == "" {
		return fmt.Errorf("%w: path is required", ErrSQLPreparedQueryCachePersistenceInvalid)
	}

	cache.mu.Lock()
	records := make([]sqlPreparedQueryCachePersistenceRecord, 0, len(cache.entries))
	for element := cache.order.Front(); element != nil; element = element.Next() {
		key, ok := element.Value.(string)
		if !ok {
			continue
		}
		entry, ok := cache.entries[key]
		if !ok || entry.query == nil {
			continue
		}
		if options.SchemaVersion != "" && entry.lookupKey.schemaVersion != options.SchemaVersion {
			continue
		}
		records = append(records, sqlPreparedQueryCachePersistenceRecord{
			source:        entry.lookupKey.source,
			schemaVersion: entry.lookupKey.schemaVersion,
		})
	}
	cache.mu.Unlock()

	if len(records) > maxEntries {
		records = records[len(records)-maxEntries:]
	}
	payload, err := encodeSQLPreparedQueryCachePersistence(records, maxBytes)
	if err != nil {
		return err
	}
	return writeSQLPreparedQueryCachePersistence(path, payload)
}

// Load validates and restores a snapshot. Parsing happens completely before
// the target cache is invalidated or modified, so malformed files cannot leave
// a partially restored namespace behind. Only an exact SchemaVersion is
// admitted; callers must explicitly opt in to unversioned entries.
func (cache *SQLPreparedQueryCache) Load(path string, options SQLPreparedQueryCachePersistenceOptions) (SQLPreparedQueryCacheLoadReport, error) {
	maxEntries, maxBytes, err := normalizeSQLPreparedQueryCachePersistenceOptions(options)
	if err != nil {
		return SQLPreparedQueryCacheLoadReport{}, err
	}
	report := SQLPreparedQueryCacheLoadReport{
		Format: SQLPreparedQueryCachePersistenceFormat,
	}
	if cache == nil {
		return report, fmt.Errorf("%w: cache is nil", ErrSQLPreparedQueryCachePersistenceInvalid)
	}
	if path == "" {
		return report, fmt.Errorf("%w: path is required", ErrSQLPreparedQueryCachePersistenceInvalid)
	}
	if options.SchemaVersion == "" && !options.AllowUnversioned {
		// Keep this an explicit no-op rather than accepting unvalidated plans.
		return report, nil
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return report, err
	}
	report.Bytes = len(payload)
	if len(payload) > maxBytes {
		return report, fmt.Errorf("%w: snapshot is %d bytes, maximum is %d", ErrSQLPreparedQueryCachePersistenceLimitExceeded, len(payload), maxBytes)
	}
	records, err := decodeSQLPreparedQueryCachePersistence(payload, maxEntries)
	if err != nil {
		return report, err
	}
	selected := make([]sqlPreparedQueryCachePersistenceParsedRecord, 0, len(records))
	for _, record := range records {
		if record.schemaVersion != options.SchemaVersion {
			report.SkippedSchema++
			continue
		}
		query, err := parseSQLQueryTemplate(record.source)
		if err != nil {
			return report, fmt.Errorf("%w: source %q: %v", ErrSQLPreparedQueryCachePersistenceInvalid, record.source, err)
		}
		key, err := sqlPreparedQueryCacheKey(record.source, record.schemaVersion)
		if err != nil {
			return report, fmt.Errorf("%w: source %q: %v", ErrSQLPreparedQueryCachePersistenceInvalid, record.source, err)
		}
		selected = append(selected, sqlPreparedQueryCachePersistenceParsedRecord{
			sqlPreparedQueryCachePersistenceRecord: record,
			query:                                  query,
			key:                                    key,
		})
	}

	cache.InvalidateSchemaVersion(options.SchemaVersion)
	if cache.capacity <= 0 {
		return report, nil
	}
	if len(selected) > cache.capacity {
		selected = selected[len(selected)-cache.capacity:]
	}
	cache.mu.Lock()
	for _, record := range selected {
		cache.insertPreparedQueryCacheEntryLocked(record.key, record.source, record.schemaVersion, record.query)
	}
	cache.mu.Unlock()
	report.Loaded = len(selected)
	return report, nil
}

func (cache *SQLPreparedQueryCache) insertPreparedQueryCacheEntryLocked(key, source, schemaVersion string, query *sqlQuery) {
	if existing, ok := cache.entries[key]; ok {
		cache.order.Remove(existing.order)
		sqlPreparedQueryCacheDeleteExactEntry(cache, existing.lookupKey)
	}
	if len(cache.entries) >= cache.capacity {
		oldest := cache.order.Front()
		if oldest != nil {
			evictedKey := oldest.Value.(string)
			evicted := cache.entries[evictedKey]
			cache.order.Remove(oldest)
			delete(cache.entries, evictedKey)
			sqlPreparedQueryCacheDeleteExactEntry(cache, evicted.lookupKey)
		}
	}
	lookupKey := sqlPreparedQueryCacheLookupKey{source: source, schemaVersion: schemaVersion}
	entry := sqlPreparedQueryCacheEntry{
		query:     query,
		order:     cache.order.PushBack(key),
		lookupKey: lookupKey,
	}
	cache.entries[key] = entry
	sqlPreparedQueryCacheSetExactEntry(cache, lookupKey, entry)
}

func normalizeSQLPreparedQueryCachePersistenceOptions(options SQLPreparedQueryCachePersistenceOptions) (int, int, error) {
	maxEntries := options.MaxEntries
	if maxEntries == 0 {
		maxEntries = DefaultSQLPreparedQueryCachePersistenceMaxEntries
	}
	if maxEntries < 0 || maxEntries > MaxSQLPreparedQueryCachePersistenceMaxEntries {
		return 0, 0, fmt.Errorf("%w: max entries %d", ErrSQLPreparedQueryCachePersistenceLimitExceeded, maxEntries)
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultSQLPreparedQueryCachePersistenceMaxBytes
	}
	if maxBytes < len(sqlPreparedQueryCachePersistenceMagic)+5 || maxBytes > MaxSQLPreparedQueryCachePersistenceMaxBytes {
		return 0, 0, fmt.Errorf("%w: max bytes %d", ErrSQLPreparedQueryCachePersistenceLimitExceeded, maxBytes)
	}
	if len(options.SchemaVersion) > MaxSQLPreparedQueryCachePersistenceSchemaVersionBytes || !utf8.ValidString(options.SchemaVersion) {
		return 0, 0, fmt.Errorf("%w: schema version is invalid", ErrSQLPreparedQueryCachePersistenceInvalid)
	}
	return maxEntries, maxBytes, nil
}

func encodeSQLPreparedQueryCachePersistence(records []sqlPreparedQueryCachePersistenceRecord, maxBytes int) ([]byte, error) {
	payload := make([]byte, 0, len(sqlPreparedQueryCachePersistenceMagic)+len(records)*32+4)
	payload = append(payload, sqlPreparedQueryCachePersistenceMagic...)
	payload = appendSQLPreparedQueryCachePersistenceUvarint(payload, uint64(len(records)))
	for _, record := range records {
		if len(record.source) == 0 || len(record.source) > MaxSQLPreparedQueryCachePersistenceSourceBytes || !utf8.ValidString(record.source) {
			return nil, fmt.Errorf("%w: source length or encoding is invalid", ErrSQLPreparedQueryCachePersistenceInvalid)
		}
		if len(record.schemaVersion) > MaxSQLPreparedQueryCachePersistenceSchemaVersionBytes || !utf8.ValidString(record.schemaVersion) {
			return nil, fmt.Errorf("%w: schema version length or encoding is invalid", ErrSQLPreparedQueryCachePersistenceInvalid)
		}
		payload = appendSQLPreparedQueryCachePersistenceString(payload, record.source)
		payload = appendSQLPreparedQueryCachePersistenceString(payload, record.schemaVersion)
		if len(payload)+4 > maxBytes {
			return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrSQLPreparedQueryCachePersistenceLimitExceeded, maxBytes)
		}
	}
	checksum := crc32.ChecksumIEEE(payload)
	var checksumBytes [4]byte
	binary.BigEndian.PutUint32(checksumBytes[:], checksum)
	payload = append(payload, checksumBytes[:]...)
	return payload, nil
}

func decodeSQLPreparedQueryCachePersistence(payload []byte, maxEntries int) ([]sqlPreparedQueryCachePersistenceRecord, error) {
	if len(payload) < len(sqlPreparedQueryCachePersistenceMagic)+1+4 || !bytes.Equal(payload[:len(sqlPreparedQueryCachePersistenceMagic)], sqlPreparedQueryCachePersistenceMagic) {
		return nil, fmt.Errorf("%w: unsupported format", ErrSQLPreparedQueryCachePersistenceInvalid)
	}
	content := payload[:len(payload)-4]
	expectedChecksum := binary.BigEndian.Uint32(payload[len(payload)-4:])
	if crc32.ChecksumIEEE(content) != expectedChecksum {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrSQLPreparedQueryCachePersistenceInvalid)
	}
	reader := sqlPreparedQueryCachePersistenceReader{payload: content, offset: len(sqlPreparedQueryCachePersistenceMagic)}
	count, err := reader.readUvarint()
	if err != nil {
		return nil, fmt.Errorf("%w: entry count: %v", ErrSQLPreparedQueryCachePersistenceInvalid, err)
	}
	if count > uint64(maxEntries) {
		return nil, fmt.Errorf("%w: %d entries exceeds maximum %d", ErrSQLPreparedQueryCachePersistenceLimitExceeded, count, maxEntries)
	}
	records := make([]sqlPreparedQueryCachePersistenceRecord, 0, int(count))
	for index := uint64(0); index < count; index++ {
		source, err := reader.readString(MaxSQLPreparedQueryCachePersistenceSourceBytes)
		if err != nil {
			return nil, fmt.Errorf("%w: entry %d source: %v", ErrSQLPreparedQueryCachePersistenceInvalid, index, err)
		}
		schemaVersion, err := reader.readString(MaxSQLPreparedQueryCachePersistenceSchemaVersionBytes)
		if err != nil {
			return nil, fmt.Errorf("%w: entry %d schema version: %v", ErrSQLPreparedQueryCachePersistenceInvalid, index, err)
		}
		if source == "" {
			return nil, fmt.Errorf("%w: entry %d source is empty", ErrSQLPreparedQueryCachePersistenceInvalid, index)
		}
		records = append(records, sqlPreparedQueryCachePersistenceRecord{source: source, schemaVersion: schemaVersion})
	}
	if reader.offset != len(content) {
		return nil, fmt.Errorf("%w: trailing bytes", ErrSQLPreparedQueryCachePersistenceInvalid)
	}
	return records, nil
}

func appendSQLPreparedQueryCachePersistenceUvarint(destination []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(destination, encoded[:n]...)
}

func appendSQLPreparedQueryCachePersistenceString(destination []byte, value string) []byte {
	destination = appendSQLPreparedQueryCachePersistenceUvarint(destination, uint64(len(value)))
	return append(destination, value...)
}

func writeSQLPreparedQueryCachePersistence(path string, payload []byte) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".hatrie-sql-prepared-cache-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

type sqlPreparedQueryCachePersistenceReader struct {
	payload []byte
	offset  int
}

func (reader *sqlPreparedQueryCachePersistenceReader) readUvarint() (uint64, error) {
	if reader.offset >= len(reader.payload) {
		return 0, io.ErrUnexpectedEOF
	}
	value, count := binary.Uvarint(reader.payload[reader.offset:])
	if count == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if count < 0 {
		return 0, errors.New("varint overflow")
	}
	reader.offset += count
	return value, nil
}

func (reader *sqlPreparedQueryCachePersistenceReader) readString(maxBytes int) (string, error) {
	length, err := reader.readUvarint()
	if err != nil {
		return "", err
	}
	if length > uint64(maxBytes) || length > uint64(len(reader.payload)-reader.offset) {
		return "", fmt.Errorf("length %d exceeds bound", length)
	}
	start := reader.offset
	reader.offset += int(length)
	value := string(reader.payload[start:reader.offset])
	if !utf8.ValidString(value) {
		return "", errors.New("invalid UTF-8")
	}
	return value, nil
}
