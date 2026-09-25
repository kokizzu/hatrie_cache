package hatSql

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	sqlProjectionDefinitionStoreMaxPayloadBytes       = int64(1<<32 - 1)
	sqlProjectionDefinitionStoreMagic                 = "HSP1"
	sqlProjectionDefinitionStoreHeaderBytes           = 8
	sqlProjectionDefinitionStoreTrailerBytes          = 4
	sqlProjectionDefinitionStoreDefaultMaxDefinitions = 4096
	sqlProjectionDefinitionStoreDefaultMaxBytes       = 8 << 20
)

var ErrSQLProjectionDefinitionStoreCorrupt = errors.New("SQL projection definition store is corrupt")

// SQLProjectionDefinitionStore persists projection definitions independently
// from their materialized rows. Implementations must replace one snapshot
// atomically from the caller's perspective.
type SQLProjectionDefinitionStore interface {
	LoadSQLProjectionDefinitions(context.Context) ([]MaterializedViewDefinition, error)
	SaveSQLProjectionDefinitions(context.Context, []MaterializedViewDefinition) error
}

// SQLProjectionDefinitionStoreOptions bounds a file-backed definition
// snapshot. Zero values select conservative defaults.
type SQLProjectionDefinitionStoreOptions struct {
	MaxDefinitions int
	MaxBytes       int64
}

// FileSQLProjectionDefinitionStore stores definitions in a compact,
// deterministic, CRC-protected snapshot. It does not store materialized rows;
// a session rebuilds those rows against its current source versions.
type FileSQLProjectionDefinitionStore struct {
	mu             sync.Mutex
	path           string
	maxDefinitions int
	maxBytes       int64
}

// NewFileSQLProjectionDefinitionStore creates an opt-in durable projection
// definition store using the default safety limits.
func NewFileSQLProjectionDefinitionStore(path string) (*FileSQLProjectionDefinitionStore, error) {
	return NewFileSQLProjectionDefinitionStoreWithOptions(path, SQLProjectionDefinitionStoreOptions{})
}

// NewFileSQLProjectionDefinitionStoreWithOptions creates a file-backed store
// with explicit definition and encoded-byte limits.
func NewFileSQLProjectionDefinitionStoreWithOptions(path string, options SQLProjectionDefinitionStoreOptions) (*FileSQLProjectionDefinitionStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("SQL projection definition store path is required")
	}
	if options.MaxDefinitions == 0 {
		options.MaxDefinitions = sqlProjectionDefinitionStoreDefaultMaxDefinitions
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = sqlProjectionDefinitionStoreDefaultMaxBytes
	}
	if options.MaxDefinitions < 0 || options.MaxBytes < sqlProjectionDefinitionStoreHeaderBytes+sqlProjectionDefinitionStoreTrailerBytes+1 || options.MaxBytes > sqlProjectionDefinitionStoreMaxPayloadBytes+sqlProjectionDefinitionStoreHeaderBytes+sqlProjectionDefinitionStoreTrailerBytes {
		return nil, fmt.Errorf("invalid SQL projection definition store limits")
	}
	return &FileSQLProjectionDefinitionStore{
		path:           filepath.Clean(path),
		maxDefinitions: options.MaxDefinitions,
		maxBytes:       options.MaxBytes,
	}, nil
}

func (store *FileSQLProjectionDefinitionStore) LoadSQLProjectionDefinitions(ctx context.Context) ([]MaterializedViewDefinition, error) {
	if store == nil {
		return nil, fmt.Errorf("SQL projection definition store is nil")
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
		return nil, fmt.Errorf("open SQL projection definition store: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, store.maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read SQL projection definition store: %w", err)
	}
	if int64(len(data)) > store.maxBytes {
		return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrSQLProjectionDefinitionStoreCorrupt, store.maxBytes)
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	return decodeSQLProjectionDefinitions(data, store.maxDefinitions, store.maxBytes)
}

func (store *FileSQLProjectionDefinitionStore) SaveSQLProjectionDefinitions(ctx context.Context, definitions []MaterializedViewDefinition) error {
	if store == nil {
		return fmt.Errorf("SQL projection definition store is nil")
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	data, err := encodeSQLProjectionDefinitions(ctx, definitions, store.maxDefinitions, store.maxBytes)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	directory := filepath.Dir(store.path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create SQL projection definition store directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(store.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create SQL projection definition store temporary file: %w", err)
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
		return fmt.Errorf("set SQL projection definition store permissions: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		return fmt.Errorf("write SQL projection definition store: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync SQL projection definition store: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close SQL projection definition store: %w", err)
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return fmt.Errorf("publish SQL projection definition store: %w", err)
	}
	removeTemporary = false
	return nil
}

func encodeSQLProjectionDefinitions(ctx context.Context, definitions []MaterializedViewDefinition, maxDefinitions int, maxBytes int64) ([]byte, error) {
	if len(definitions) > maxDefinitions {
		return nil, fmt.Errorf("SQL projection definition count %d exceeds maximum %d", len(definitions), maxDefinitions)
	}
	normalized := make([]MaterializedViewDefinition, 0, len(definitions))
	seen := make(map[string]struct{}, len(definitions))
	for _, definition := range definitions {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		normalizedDefinition, err := normalizeMaterializedViewDefinition(definition)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[normalizedDefinition.Name]; exists {
			return nil, fmt.Errorf("duplicate SQL projection definition %q", normalizedDefinition.Name)
		}
		seen[normalizedDefinition.Name] = struct{}{}
		normalized = append(normalized, normalizedDefinition)
	}
	sort.Slice(normalized, func(left, right int) bool {
		return normalized[left].Name < normalized[right].Name
	})
	var payload bytes.Buffer
	writeSQLProjectionUvarint(&payload, uint64(len(normalized)))
	for _, definition := range normalized {
		if err := writeSQLProjectionString(&payload, definition.Name); err != nil {
			return nil, err
		}
		if err := writeSQLProjectionString(&payload, definition.Query); err != nil {
			return nil, err
		}
		writeSQLProjectionUvarint(&payload, uint64(len(definition.Dependencies)))
		for _, dependency := range definition.Dependencies {
			if err := writeSQLProjectionString(&payload, dependency); err != nil {
				return nil, err
			}
		}
	}
	if int64(payload.Len()) > maxBytes-sqlProjectionDefinitionStoreHeaderBytes-sqlProjectionDefinitionStoreTrailerBytes {
		return nil, fmt.Errorf("SQL projection definition snapshot exceeds %d bytes", maxBytes)
	}
	data := make([]byte, sqlProjectionDefinitionStoreHeaderBytes+payload.Len()+sqlProjectionDefinitionStoreTrailerBytes)
	copy(data, sqlProjectionDefinitionStoreMagic)
	binary.BigEndian.PutUint32(data[4:8], uint32(payload.Len()))
	copy(data[sqlProjectionDefinitionStoreHeaderBytes:], payload.Bytes())
	binary.BigEndian.PutUint32(data[len(data)-sqlProjectionDefinitionStoreTrailerBytes:], crc32.ChecksumIEEE(payload.Bytes()))
	return data, nil
}

func decodeSQLProjectionDefinitions(data []byte, maxDefinitions int, maxBytes int64) ([]MaterializedViewDefinition, error) {
	if len(data) < sqlProjectionDefinitionStoreHeaderBytes+sqlProjectionDefinitionStoreTrailerBytes || string(data[:4]) != sqlProjectionDefinitionStoreMagic {
		return nil, fmt.Errorf("%w: invalid header", ErrSQLProjectionDefinitionStoreCorrupt)
	}
	payloadLength := int(binary.BigEndian.Uint32(data[4:8]))
	if payloadLength != len(data)-sqlProjectionDefinitionStoreHeaderBytes-sqlProjectionDefinitionStoreTrailerBytes || int64(payloadLength) > maxBytes {
		return nil, fmt.Errorf("%w: invalid payload length", ErrSQLProjectionDefinitionStoreCorrupt)
	}
	payloadStart := sqlProjectionDefinitionStoreHeaderBytes
	payloadEnd := payloadStart + payloadLength
	payload := data[payloadStart:payloadEnd]
	wantCRC := binary.BigEndian.Uint32(data[payloadEnd:])
	if gotCRC := crc32.ChecksumIEEE(payload); gotCRC != wantCRC {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrSQLProjectionDefinitionStoreCorrupt)
	}
	reader := bytes.NewReader(payload)
	count, err := binary.ReadUvarint(reader)
	if err != nil || count > uint64(maxDefinitions) {
		return nil, fmt.Errorf("%w: invalid definition count", ErrSQLProjectionDefinitionStoreCorrupt)
	}
	definitions := make([]MaterializedViewDefinition, 0, int(count))
	seen := make(map[string]struct{}, int(count))
	for index := uint64(0); index < count; index++ {
		name, err := readSQLProjectionString(reader, maxBytes)
		if err != nil {
			return nil, fmt.Errorf("%w: definition %d name: %v", ErrSQLProjectionDefinitionStoreCorrupt, index, err)
		}
		query, err := readSQLProjectionString(reader, maxBytes)
		if err != nil {
			return nil, fmt.Errorf("%w: definition %d query: %v", ErrSQLProjectionDefinitionStoreCorrupt, index, err)
		}
		dependencyCount, err := binary.ReadUvarint(reader)
		if err != nil || dependencyCount > uint64(maxDefinitions) {
			return nil, fmt.Errorf("%w: definition %d dependency count", ErrSQLProjectionDefinitionStoreCorrupt, index)
		}
		dependencies := make([]string, 0, int(dependencyCount))
		for dependencyIndex := uint64(0); dependencyIndex < dependencyCount; dependencyIndex++ {
			dependency, err := readSQLProjectionString(reader, maxBytes)
			if err != nil {
				return nil, fmt.Errorf("%w: definition %d dependency %d: %v", ErrSQLProjectionDefinitionStoreCorrupt, index, dependencyIndex, err)
			}
			dependencies = append(dependencies, dependency)
		}
		definition, err := normalizeMaterializedViewDefinition(MaterializedViewDefinition{Name: name, Query: query, Dependencies: dependencies})
		if err != nil {
			return nil, fmt.Errorf("%w: definition %d: %v", ErrSQLProjectionDefinitionStoreCorrupt, index, err)
		}
		if _, exists := seen[definition.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate definition %q", ErrSQLProjectionDefinitionStoreCorrupt, definition.Name)
		}
		seen[definition.Name] = struct{}{}
		definitions = append(definitions, definition)
	}
	if reader.Len() != 0 {
		return nil, fmt.Errorf("%w: trailing payload bytes", ErrSQLProjectionDefinitionStoreCorrupt)
	}
	sort.Slice(definitions, func(left, right int) bool {
		return definitions[left].Name < definitions[right].Name
	})
	return definitions, nil
}

func writeSQLProjectionUvarint(buffer *bytes.Buffer, value uint64) {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	_, _ = buffer.Write(encoded[:n])
}

func writeSQLProjectionString(buffer *bytes.Buffer, value string) error {
	writeSQLProjectionUvarint(buffer, uint64(len(value)))
	_, _ = buffer.WriteString(value)
	return nil
}

func readSQLProjectionString(reader *bytes.Reader, maxBytes int64) (string, error) {
	length, err := binary.ReadUvarint(reader)
	if err != nil {
		return "", err
	}
	if length > uint64(maxBytes) || length > uint64(reader.Len()) {
		return "", fmt.Errorf("string length %d is invalid", length)
	}
	value := make([]byte, int(length))
	if _, err := io.ReadFull(reader, value); err != nil {
		return "", err
	}
	return string(value), nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
