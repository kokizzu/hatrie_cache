package hatSql

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSQLFinalSchemaStoreMaxDefinitions bounds the number of durable
	// source contracts when the caller does not provide a limit.
	DefaultSQLFinalSchemaStoreMaxDefinitions = 256
	// DefaultSQLFinalSchemaStoreMaxBytes bounds one durable metadata snapshot.
	DefaultSQLFinalSchemaStoreMaxBytes int64 = 1 << 20
	// MaxSQLFinalSchemaStoreMaxDefinitions prevents accidental unbounded state.
	MaxSQLFinalSchemaStoreMaxDefinitions = 65536
	// MaxSQLFinalSchemaStoreMaxBytes prevents oversized metadata allocations.
	MaxSQLFinalSchemaStoreMaxBytes int64 = 64 << 20

	sqlFinalSchemaStoreMagic        = "HFS1"
	sqlFinalSchemaStoreHeaderBytes  = 8
	sqlFinalSchemaStoreTrailerBytes = 4
)

var (
	// ErrSQLFinalSchemaStoreOptionsInvalid means store bounds are invalid.
	ErrSQLFinalSchemaStoreOptionsInvalid = errors.New("hatSql: invalid FINAL schema store options")
	// ErrSQLFinalSchemaStoreCorrupt means a durable snapshot failed validation.
	ErrSQLFinalSchemaStoreCorrupt = errors.New("hatSql: corrupt FINAL schema store snapshot")
	// ErrSQLFinalSchemaStoreNil means a required store was not supplied.
	ErrSQLFinalSchemaStoreNil = errors.New("hatSql: FINAL schema store is nil")
)

var sqlFinalSchemaStoreChecksumTable = crc32.MakeTable(crc32.Castagnoli)

// SQLFinalSchemaStoreOptions bounds a file-backed FINAL schema snapshot. Zero
// values select conservative defaults.
type SQLFinalSchemaStoreOptions struct {
	MaxDefinitions int
	MaxBytes       int64
}

// SQLFinalSchemaStore persists FINAL source contracts independently from row
// data. Callers explicitly save and restore the registry so durability never
// adds an allocation or filesystem cost to the normal query path.
type SQLFinalSchemaStore interface {
	LoadSQLFinalSchemaRegistrations(context.Context) ([]SQLFinalSchemaRegistration, error)
	SaveSQLFinalSchemaRegistrations(context.Context, []SQLFinalSchemaRegistration) error
}

// FileSQLFinalSchemaStore stores deterministic, CRC-protected HFS1 snapshots.
// A save is published by atomic rename and the final file is mode 0600.
type FileSQLFinalSchemaStore struct {
	mu             sync.Mutex
	path           string
	maxDefinitions int
	maxBytes       int64
}

// NewFileSQLFinalSchemaStore creates a file-backed FINAL schema store using
// the default safety limits.
func NewFileSQLFinalSchemaStore(path string) (*FileSQLFinalSchemaStore, error) {
	return NewFileSQLFinalSchemaStoreWithOptions(path, SQLFinalSchemaStoreOptions{})
}

// NewFileSQLFinalSchemaStoreWithOptions creates a bounded file-backed FINAL
// schema store.
func NewFileSQLFinalSchemaStoreWithOptions(path string, options SQLFinalSchemaStoreOptions) (*FileSQLFinalSchemaStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("%w: path is required", ErrSQLFinalSchemaStoreOptionsInvalid)
	}
	maxDefinitions := options.MaxDefinitions
	if maxDefinitions == 0 {
		maxDefinitions = DefaultSQLFinalSchemaStoreMaxDefinitions
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultSQLFinalSchemaStoreMaxBytes
	}
	minimumBytes := int64(sqlFinalSchemaStoreHeaderBytes + sqlFinalSchemaStoreTrailerBytes + 1)
	if maxDefinitions < 1 || maxDefinitions > MaxSQLFinalSchemaStoreMaxDefinitions || maxBytes < minimumBytes || maxBytes > MaxSQLFinalSchemaStoreMaxBytes {
		return nil, fmt.Errorf("%w: MaxDefinitions=%d MaxBytes=%d", ErrSQLFinalSchemaStoreOptionsInvalid, maxDefinitions, maxBytes)
	}
	return &FileSQLFinalSchemaStore{
		path:           filepath.Clean(path),
		maxDefinitions: maxDefinitions,
		maxBytes:       maxBytes,
	}, nil
}

// LoadSQLFinalSchemaRegistrations loads one validated snapshot. A missing file
// is treated as an empty registry so first boot does not require provisioning.
func (store *FileSQLFinalSchemaStore) LoadSQLFinalSchemaRegistrations(ctx context.Context) ([]SQLFinalSchemaRegistration, error) {
	if store == nil {
		return nil, ErrSQLFinalSchemaStoreNil
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	file, err := os.Open(store.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("open FINAL schema store: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, store.maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read FINAL schema store: %w", err)
	}
	if int64(len(data)) > store.maxBytes {
		return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrSQLFinalSchemaStoreCorrupt, store.maxBytes)
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	return decodeSQLFinalSchemaRegistrations(data, store.maxDefinitions, store.maxBytes)
}

// SaveSQLFinalSchemaRegistrations validates and atomically publishes one
// deterministic snapshot. Invalid input never replaces an existing file.
func (store *FileSQLFinalSchemaStore) SaveSQLFinalSchemaRegistrations(ctx context.Context, registrations []SQLFinalSchemaRegistration) error {
	if store == nil {
		return ErrSQLFinalSchemaStoreNil
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	data, err := encodeSQLFinalSchemaRegistrations(ctx, registrations, store.maxDefinitions, store.maxBytes)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	directory := filepath.Dir(store.path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create FINAL schema store directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(store.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create FINAL schema store temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		_ = temporary.Close()
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("set FINAL schema store permissions: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		return fmt.Errorf("write FINAL schema store: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync FINAL schema store: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close FINAL schema store: %w", err)
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return fmt.Errorf("publish FINAL schema store: %w", err)
	}
	removeTemporary = false
	return nil
}

// SaveToSQLFinalSchemaStore persists the registry's detached snapshot.
func (registry *SQLFinalSchemaRegistry) SaveToSQLFinalSchemaStore(ctx context.Context, store SQLFinalSchemaStore) error {
	if registry == nil {
		return ErrSQLFinalSchemaStoreNil
	}
	if store == nil {
		return ErrSQLFinalSchemaStoreNil
	}
	return store.SaveSQLFinalSchemaRegistrations(ctx, registry.Snapshot())
}

// NewSQLFinalSchemaRegistryFromStore restores a registry from a durable
// snapshot. The returned registry is fully validated before it is exposed.
func NewSQLFinalSchemaRegistryFromStore(ctx context.Context, store SQLFinalSchemaStore, options SQLFinalSchemaRegistryOptions) (*SQLFinalSchemaRegistry, error) {
	if store == nil {
		return nil, ErrSQLFinalSchemaStoreNil
	}
	registrations, err := store.LoadSQLFinalSchemaRegistrations(ctx)
	if err != nil {
		return nil, err
	}
	registry, err := NewSQLFinalSchemaRegistry(options)
	if err != nil {
		return nil, err
	}
	for _, registration := range registrations {
		if err := registry.Upsert(registration.Kind, registration.Key, registration.Definition); err != nil {
			return nil, fmt.Errorf("restore FINAL schema %q/%q: %w", registration.Kind, registration.Key, err)
		}
	}
	return registry, nil
}

func encodeSQLFinalSchemaRegistrations(ctx context.Context, registrations []SQLFinalSchemaRegistration, maxDefinitions int, maxBytes int64) ([]byte, error) {
	if len(registrations) > maxDefinitions {
		return nil, fmt.Errorf("FINAL schema registration count %d exceeds maximum %d", len(registrations), maxDefinitions)
	}
	normalized := make([]SQLFinalSchemaRegistration, 0, len(registrations))
	seen := make(map[sqlFinalSchemaRegistryKey]struct{}, len(registrations))
	for _, registration := range registrations {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		kind, err := normalizeSQLFinalSchemaKind(registration.Kind)
		if err != nil {
			return nil, err
		}
		key, err := normalizeSQLFinalSchemaName(registration.Key)
		if err != nil {
			return nil, err
		}
		definition, err := normalizeSQLFinalSchemaDefinition(registration.Definition)
		if err != nil {
			return nil, err
		}
		registryKey := sqlFinalSchemaRegistryKey{kind: kind, key: key}
		if _, exists := seen[registryKey]; exists {
			return nil, fmt.Errorf("duplicate FINAL schema registration %q/%q", kind, key)
		}
		seen[registryKey] = struct{}{}
		normalized = append(normalized, SQLFinalSchemaRegistration{Kind: kind, Key: key, Definition: definition})
	}
	sort.Slice(normalized, func(left, right int) bool {
		if normalized[left].Kind != normalized[right].Kind {
			return normalized[left].Kind < normalized[right].Kind
		}
		return normalized[left].Key < normalized[right].Key
	})

	var payload bytes.Buffer
	appendSQLFinalSchemaStoreUvarint(&payload, uint64(len(normalized)))
	for _, registration := range normalized {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		appendSQLFinalSchemaStoreString(&payload, registration.Kind)
		appendSQLFinalSchemaStoreString(&payload, registration.Key)
		payload.WriteByte(byte(registration.Definition.Mode))
		appendSQLFinalSchemaStoreUvarint(&payload, uint64(len(registration.Definition.KeyFields)))
		for _, field := range registration.Definition.KeyFields {
			appendSQLFinalSchemaStoreString(&payload, field)
		}
		appendSQLFinalSchemaStoreString(&payload, registration.Definition.VersionField)
		appendSQLFinalSchemaStoreString(&payload, registration.Definition.SignField)
	}
	if int64(payload.Len()) > maxBytes-sqlFinalSchemaStoreHeaderBytes-sqlFinalSchemaStoreTrailerBytes || uint64(payload.Len()) > math.MaxUint32 {
		return nil, fmt.Errorf("FINAL schema snapshot exceeds %d bytes", maxBytes)
	}
	data := make([]byte, sqlFinalSchemaStoreHeaderBytes+payload.Len()+sqlFinalSchemaStoreTrailerBytes)
	copy(data, sqlFinalSchemaStoreMagic)
	binary.BigEndian.PutUint32(data[4:8], uint32(payload.Len()))
	copy(data[sqlFinalSchemaStoreHeaderBytes:], payload.Bytes())
	binary.BigEndian.PutUint32(data[len(data)-sqlFinalSchemaStoreTrailerBytes:], crc32.Checksum(payload.Bytes(), sqlFinalSchemaStoreChecksumTable))
	return data, nil
}

func decodeSQLFinalSchemaRegistrations(data []byte, maxDefinitions int, maxBytes int64) ([]SQLFinalSchemaRegistration, error) {
	minimumBytes := sqlFinalSchemaStoreHeaderBytes + sqlFinalSchemaStoreTrailerBytes
	if len(data) < minimumBytes || string(data[:len(sqlFinalSchemaStoreMagic)]) != sqlFinalSchemaStoreMagic {
		return nil, fmt.Errorf("%w: invalid header", ErrSQLFinalSchemaStoreCorrupt)
	}
	payloadLength := int(binary.BigEndian.Uint32(data[4:8]))
	if payloadLength != len(data)-minimumBytes || int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%w: invalid payload length", ErrSQLFinalSchemaStoreCorrupt)
	}
	payloadStart := sqlFinalSchemaStoreHeaderBytes
	payloadEnd := payloadStart + payloadLength
	wantChecksum := binary.BigEndian.Uint32(data[payloadEnd:])
	if gotChecksum := crc32.Checksum(data[payloadStart:payloadEnd], sqlFinalSchemaStoreChecksumTable); gotChecksum != wantChecksum {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrSQLFinalSchemaStoreCorrupt)
	}
	reader := bytes.NewReader(data[payloadStart:payloadEnd])
	count, err := binary.ReadUvarint(reader)
	if err != nil || count > uint64(maxDefinitions) {
		return nil, fmt.Errorf("%w: invalid registration count", ErrSQLFinalSchemaStoreCorrupt)
	}
	registrations := make([]SQLFinalSchemaRegistration, 0, int(count))
	seen := make(map[sqlFinalSchemaRegistryKey]struct{}, int(count))
	for index := uint64(0); index < count; index++ {
		kind, err := readSQLFinalSchemaStoreString(reader)
		if err != nil {
			return nil, fmt.Errorf("%w: registration %d kind", ErrSQLFinalSchemaStoreCorrupt, index)
		}
		key, err := readSQLFinalSchemaStoreString(reader)
		if err != nil {
			return nil, fmt.Errorf("%w: registration %d key", ErrSQLFinalSchemaStoreCorrupt, index)
		}
		mode, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("%w: registration %d mode", ErrSQLFinalSchemaStoreCorrupt, index)
		}
		keyFieldCount, err := binary.ReadUvarint(reader)
		if err != nil || keyFieldCount > MaxSQLFinalSchemaKeyFields {
			return nil, fmt.Errorf("%w: registration %d key fields", ErrSQLFinalSchemaStoreCorrupt, index)
		}
		keyFields := make([]string, 0, int(keyFieldCount))
		for fieldIndex := uint64(0); fieldIndex < keyFieldCount; fieldIndex++ {
			field, err := readSQLFinalSchemaStoreString(reader)
			if err != nil {
				return nil, fmt.Errorf("%w: registration %d key field %d", ErrSQLFinalSchemaStoreCorrupt, index, fieldIndex)
			}
			keyFields = append(keyFields, field)
		}
		versionField, err := readSQLFinalSchemaStoreString(reader)
		if err != nil {
			return nil, fmt.Errorf("%w: registration %d version field", ErrSQLFinalSchemaStoreCorrupt, index)
		}
		signField, err := readSQLFinalSchemaStoreString(reader)
		if err != nil {
			return nil, fmt.Errorf("%w: registration %d sign field", ErrSQLFinalSchemaStoreCorrupt, index)
		}
		normalizedKind, err := normalizeSQLFinalSchemaKind(kind)
		if err != nil {
			return nil, fmt.Errorf("%w: registration %d kind", ErrSQLFinalSchemaStoreCorrupt, index)
		}
		normalizedKey, err := normalizeSQLFinalSchemaName(key)
		if err != nil {
			return nil, fmt.Errorf("%w: registration %d key", ErrSQLFinalSchemaStoreCorrupt, index)
		}
		definition, err := normalizeSQLFinalSchemaDefinition(SQLFinalSchemaDefinition{
			Mode:         SQLFinalMode(mode),
			KeyFields:    keyFields,
			VersionField: versionField,
			SignField:    signField,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: registration %d definition", ErrSQLFinalSchemaStoreCorrupt, index)
		}
		registryKey := sqlFinalSchemaRegistryKey{kind: normalizedKind, key: normalizedKey}
		if _, exists := seen[registryKey]; exists {
			return nil, fmt.Errorf("%w: duplicate registration %q/%q", ErrSQLFinalSchemaStoreCorrupt, normalizedKind, normalizedKey)
		}
		seen[registryKey] = struct{}{}
		registrations = append(registrations, SQLFinalSchemaRegistration{Kind: normalizedKind, Key: normalizedKey, Definition: definition})
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("%w: trailing payload", ErrSQLFinalSchemaStoreCorrupt)
	}
	sort.Slice(registrations, func(left, right int) bool {
		if registrations[left].Kind != registrations[right].Kind {
			return registrations[left].Kind < registrations[right].Kind
		}
		return registrations[left].Key < registrations[right].Key
	})
	return registrations, nil
}

func appendSQLFinalSchemaStoreUvarint(buffer *bytes.Buffer, value uint64) {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], value)
	_, _ = buffer.Write(encoded[:length])
}

func appendSQLFinalSchemaStoreString(buffer *bytes.Buffer, value string) {
	appendSQLFinalSchemaStoreUvarint(buffer, uint64(len(value)))
	_, _ = buffer.WriteString(value)
}

func readSQLFinalSchemaStoreString(reader *bytes.Reader) (string, error) {
	length, err := binary.ReadUvarint(reader)
	if err != nil || length > uint64(reader.Len()) {
		return "", io.ErrUnexpectedEOF
	}
	value := make([]byte, int(length))
	if _, err := io.ReadFull(reader, value); err != nil {
		return "", err
	}
	return string(value), nil
}
