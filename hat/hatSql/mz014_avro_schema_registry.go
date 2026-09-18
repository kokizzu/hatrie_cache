package hatSql

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// DefaultAvroSchemaRegistryMaxSchemas bounds the number of cached schema
	// IDs when no application-specific limit is supplied.
	DefaultAvroSchemaRegistryMaxSchemas = 256
	// DefaultAvroSchemaRegistryMaxSchemaBytes bounds one fetched schema.
	DefaultAvroSchemaRegistryMaxSchemaBytes = 1 << 20
	// MaxAvroSchemaRegistryMaxSchemas prevents an accidental unbounded cache.
	MaxAvroSchemaRegistryMaxSchemas = 100000
	// MaxAvroSchemaRegistryMaxSchemaBytes prevents oversized registry payloads.
	MaxAvroSchemaRegistryMaxSchemaBytes = 16 << 20
)

var (
	// ErrAvroSchemaRegistryNil reports a nil registry receiver.
	ErrAvroSchemaRegistryNil = errors.New("Avro schema registry is nil")
	// ErrAvroSchemaRegistryInvalid reports malformed registry configuration.
	ErrAvroSchemaRegistryInvalid = errors.New("Avro schema registry is invalid")
	// ErrAvroSchemaRegistryFetcherRequired reports a missing schema fetcher.
	ErrAvroSchemaRegistryFetcherRequired = errors.New("Avro schema registry fetcher is required")
	// ErrAvroSchemaNotFound reports an unknown schema ID.
	ErrAvroSchemaNotFound = errors.New("Avro schema was not found")
	// ErrAvroSchemaTooLarge reports a schema over the configured size bound.
	ErrAvroSchemaTooLarge = errors.New("Avro schema is too large")
	// ErrAvroSchemaInvalid reports an empty or malformed fetched schema.
	ErrAvroSchemaInvalid = errors.New("Avro schema is invalid")
	// ErrConfluentAvroPayloadInvalid reports malformed Confluent framing.
	ErrConfluentAvroPayloadInvalid = errors.New("Confluent Avro payload is invalid")
	// ErrAvroSchemaCompatibilityInvalid reports malformed or unsupported schema
	// shapes supplied to the compatibility checker.
	ErrAvroSchemaCompatibilityInvalid = errors.New("Avro schema compatibility input is invalid")
	// ErrAvroSchemaIncompatible reports a valid schema evolution that violates
	// the selected compatibility direction.
	ErrAvroSchemaIncompatible = errors.New("Avro schemas are incompatible")
	// ErrAvroPayloadDecoderRequired reports a missing payload decoder.
	ErrAvroPayloadDecoderRequired = errors.New("Avro payload decoder is required")
)

// AvroSchemaFetcher retrieves the schema bytes for one registry ID. The
// registry copies the returned bytes before caching them.
type AvroSchemaFetcher func(schemaID uint32) ([]byte, error)

// AvroSchemaRegistryOptions configures the bounded schema cache.
type AvroSchemaRegistryOptions struct {
	Fetch          AvroSchemaFetcher
	MaxSchemas     int
	MaxSchemaBytes int
}

// AvroSchemaRegistryStats reports cache activity.
type AvroSchemaRegistryStats struct {
	Entries   int
	Hits      uint64
	Misses    uint64
	Fetches   uint64
	Evictions uint64
}

type avroSchemaCacheEntry struct {
	bytes    []byte
	lastUsed uint64
}

type avroSchemaFetch struct {
	done chan struct{}
	err  error
}

// AvroSchemaRegistry is a bounded, concurrent cache for immutable schema
// bytes. Borrowed bytes must be treated as read-only; Schema returns an owned
// copy for callers that need to retain or mutate the result.
type AvroSchemaRegistry struct {
	mu             sync.Mutex
	fetch          AvroSchemaFetcher
	maxSchemas     int
	maxSchemaBytes int
	clock          uint64
	entries        map[uint32]avroSchemaCacheEntry
	inFlight       map[uint32]*avroSchemaFetch
	stats          AvroSchemaRegistryStats
}

// NewAvroSchemaRegistry creates a bounded schema cache.
func NewAvroSchemaRegistry(options AvroSchemaRegistryOptions) (*AvroSchemaRegistry, error) {
	if options.Fetch == nil {
		return nil, ErrAvroSchemaRegistryFetcherRequired
	}
	maxSchemas := options.MaxSchemas
	if maxSchemas == 0 {
		maxSchemas = DefaultAvroSchemaRegistryMaxSchemas
	}
	if maxSchemas < 1 || maxSchemas > MaxAvroSchemaRegistryMaxSchemas {
		return nil, fmt.Errorf("%w: max schemas %d", ErrAvroSchemaRegistryInvalid, maxSchemas)
	}
	maxSchemaBytes := options.MaxSchemaBytes
	if maxSchemaBytes == 0 {
		maxSchemaBytes = DefaultAvroSchemaRegistryMaxSchemaBytes
	}
	if maxSchemaBytes < 1 || maxSchemaBytes > MaxAvroSchemaRegistryMaxSchemaBytes {
		return nil, fmt.Errorf("%w: max schema bytes %d", ErrAvroSchemaRegistryInvalid, maxSchemaBytes)
	}
	return &AvroSchemaRegistry{
		fetch:          options.Fetch,
		maxSchemas:     maxSchemas,
		maxSchemaBytes: maxSchemaBytes,
		entries:        make(map[uint32]avroSchemaCacheEntry),
		inFlight:       make(map[uint32]*avroSchemaFetch),
	}, nil
}

// Borrow returns cached schema bytes without copying. The returned slice is
// immutable for the lifetime of the registry and must not be modified.
func (registry *AvroSchemaRegistry) Borrow(schemaID uint32) ([]byte, error) {
	if registry == nil {
		return nil, ErrAvroSchemaRegistryNil
	}
	for {
		registry.mu.Lock()
		if entry, found := registry.entries[schemaID]; found {
			registry.clock++
			entry.lastUsed = registry.clock
			registry.entries[schemaID] = entry
			registry.stats.Hits++
			bytes := entry.bytes
			registry.mu.Unlock()
			return bytes, nil
		}
		registry.stats.Misses++
		if pending, found := registry.inFlight[schemaID]; found {
			registry.mu.Unlock()
			<-pending.done
			if pending.err != nil {
				return nil, pending.err
			}
			continue
		}
		pending := &avroSchemaFetch{done: make(chan struct{})}
		registry.inFlight[schemaID] = pending
		registry.mu.Unlock()

		fetched, fetchErr := registry.fetch(schemaID)
		var cached []byte
		if fetchErr == nil {
			if len(fetched) == 0 {
				fetchErr = ErrAvroSchemaInvalid
			} else if len(fetched) > registry.maxSchemaBytes {
				fetchErr = fmt.Errorf("%w: %d bytes", ErrAvroSchemaTooLarge, len(fetched))
			} else {
				cached = append([]byte(nil), fetched...)
			}
		}

		registry.mu.Lock()
		delete(registry.inFlight, schemaID)
		registry.stats.Fetches++
		pending.err = fetchErr
		if fetchErr == nil {
			registry.clock++
			if len(registry.entries) >= registry.maxSchemas {
				registry.evictOneLocked()
			}
			registry.entries[schemaID] = avroSchemaCacheEntry{bytes: cached, lastUsed: registry.clock}
		}
		close(pending.done)
		registry.mu.Unlock()
		if fetchErr != nil {
			return nil, fetchErr
		}
		return cached, nil
	}
}

// Schema returns an owned schema copy.
func (registry *AvroSchemaRegistry) Schema(schemaID uint32) ([]byte, error) {
	bytes, err := registry.Borrow(schemaID)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), bytes...), nil
}

// Stats returns a point-in-time cache summary.
func (registry *AvroSchemaRegistry) Stats() AvroSchemaRegistryStats {
	if registry == nil {
		return AvroSchemaRegistryStats{}
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	stats := registry.stats
	stats.Entries = len(registry.entries)
	return stats
}

func (registry *AvroSchemaRegistry) evictOneLocked() {
	var evictID uint32
	var oldest uint64
	first := true
	for schemaID, entry := range registry.entries {
		if first || entry.lastUsed < oldest {
			evictID = schemaID
			oldest = entry.lastUsed
			first = false
		}
	}
	if !first {
		delete(registry.entries, evictID)
		registry.stats.Evictions++
	}
}

// ParseConfluentAvroPayload extracts the schema ID and datum from the
// Confluent wire format: one zero magic byte, four big-endian ID bytes, then
// the Avro datum.
func ParseConfluentAvroPayload(payload []byte) (uint32, []byte, error) {
	if len(payload) < 5 || payload[0] != 0 {
		return 0, nil, ErrConfluentAvroPayloadInvalid
	}
	return binary.BigEndian.Uint32(payload[1:5]), payload[5:], nil
}

// AvroPayloadDecoder decodes one schema-resolved datum. Schema bytes are
// borrowed and must be treated as read-only.
type AvroPayloadDecoder func(schemaID uint32, schema, payload []byte, message KafkaTableMessage) (KafkaTableChange, error)

// NewAvroKafkaTableDecoder creates a KafkaTableDecoder that resolves
// Confluent schema IDs through registry before invoking decode. Tombstones do
// not require a schema lookup.
func NewAvroKafkaTableDecoder(registry *AvroSchemaRegistry, decode AvroPayloadDecoder) (KafkaTableDecoder, error) {
	if registry == nil {
		return nil, ErrAvroSchemaRegistryNil
	}
	if decode == nil {
		return nil, ErrAvroPayloadDecoderRequired
	}
	return func(message KafkaTableMessage) (KafkaTableChange, error) {
		if message.Value == nil {
			return KafkaTableChange{Key: message.Key, Operation: KafkaTableDelete}, nil
		}
		schemaID, datum, err := ParseConfluentAvroPayload(message.Value)
		if err != nil {
			return KafkaTableChange{}, err
		}
		schema, err := registry.Borrow(schemaID)
		if err != nil {
			return KafkaTableChange{}, fmt.Errorf("schema %d: %w", schemaID, err)
		}
		return decode(schemaID, schema, datum, message)
	}, nil
}

// AvroSchemaCompatibilityMode selects the reader/writer direction checked by
// ValidateAvroSchemaCompatibility.
type AvroSchemaCompatibilityMode uint8

const (
	AvroCompatibilityBackward AvroSchemaCompatibilityMode = iota + 1
	AvroCompatibilityForward
	AvroCompatibilityFull
)

type avroSchemaRecord struct {
	name   string
	fields map[string]avroSchemaField
}

type avroSchemaField struct {
	typeSchema json.RawMessage
	hasDefault bool
}

// ValidateAvroSchemaCompatibility validates primitive-field Avro records.
// It supports primitive unions and numeric promotion; unsupported nested or
// named-type changes fail closed with ErrAvroSchemaCompatibilityInvalid.
func ValidateAvroSchemaCompatibility(previous, current []byte, mode AvroSchemaCompatibilityMode) error {
	if mode != AvroCompatibilityBackward && mode != AvroCompatibilityForward && mode != AvroCompatibilityFull {
		return ErrAvroSchemaCompatibilityInvalid
	}
	oldSchema, err := parseAvroSchemaRecord(previous)
	if err != nil {
		return err
	}
	newSchema, err := parseAvroSchemaRecord(current)
	if err != nil {
		return err
	}
	if oldSchema.name != newSchema.name {
		return fmt.Errorf("%w: record name changed from %q to %q", ErrAvroSchemaIncompatible, oldSchema.name, newSchema.name)
	}
	if mode == AvroCompatibilityBackward || mode == AvroCompatibilityFull {
		if err := validateAvroReaderAgainstWriter(oldSchema, newSchema); err != nil {
			return fmt.Errorf("backward: %w", err)
		}
	}
	if mode == AvroCompatibilityForward || mode == AvroCompatibilityFull {
		if err := validateAvroReaderAgainstWriter(newSchema, oldSchema); err != nil {
			return fmt.Errorf("forward: %w", err)
		}
	}
	return nil
}

func parseAvroSchemaRecord(schema []byte) (avroSchemaRecord, error) {
	var document struct {
		Type   json.RawMessage `json:"type"`
		Name   string          `json:"name"`
		Fields []struct {
			Name    string          `json:"name"`
			Type    json.RawMessage `json:"type"`
			Default json.RawMessage `json:"default"`
		} `json:"fields"`
	}
	if err := json.Unmarshal(schema, &document); err != nil || len(document.Type) == 0 || document.Name == "" {
		return avroSchemaRecord{}, fmt.Errorf("%w: record JSON", ErrAvroSchemaCompatibilityInvalid)
	}
	typeNames, err := avroTypeNames(document.Type)
	if err != nil || len(typeNames) != 1 || typeNames[0] != "record" {
		return avroSchemaRecord{}, fmt.Errorf("%w: top-level type", ErrAvroSchemaCompatibilityInvalid)
	}
	record := avroSchemaRecord{name: document.Name, fields: make(map[string]avroSchemaField, len(document.Fields))}
	for _, field := range document.Fields {
		if field.Name == "" || strings.IndexByte(field.Name, 0) >= 0 || len(field.Type) == 0 {
			return avroSchemaRecord{}, fmt.Errorf("%w: field definition", ErrAvroSchemaCompatibilityInvalid)
		}
		if _, found := record.fields[field.Name]; found {
			return avroSchemaRecord{}, fmt.Errorf("%w: duplicate field %q", ErrAvroSchemaCompatibilityInvalid, field.Name)
		}
		if _, err := avroTypeNames(field.Type); err != nil {
			return avroSchemaRecord{}, err
		}
		record.fields[field.Name] = avroSchemaField{typeSchema: append(json.RawMessage(nil), field.Type...), hasDefault: len(field.Default) > 0}
	}
	return record, nil
}

func avroTypeNames(schema json.RawMessage) ([]string, error) {
	var name string
	if err := json.Unmarshal(schema, &name); err == nil {
		if name == "" {
			return nil, ErrAvroSchemaCompatibilityInvalid
		}
		return []string{name}, nil
	}
	var union []json.RawMessage
	if err := json.Unmarshal(schema, &union); err == nil {
		if len(union) == 0 {
			return nil, ErrAvroSchemaCompatibilityInvalid
		}
		names := make([]string, 0, len(union))
		for _, branch := range union {
			branchNames, err := avroTypeNames(branch)
			if err != nil || len(branchNames) != 1 {
				return nil, ErrAvroSchemaCompatibilityInvalid
			}
			names = append(names, branchNames[0])
		}
		return names, nil
	}
	var object struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(schema, &object); err != nil || object.Type == "" {
		return nil, ErrAvroSchemaCompatibilityInvalid
	}
	if object.Type == "record" || object.Type == "array" || object.Type == "map" || object.Type == "enum" || object.Type == "fixed" {
		return nil, fmt.Errorf("%w: nested type %q", ErrAvroSchemaCompatibilityInvalid, object.Type)
	}
	return []string{object.Type}, nil
}

func validateAvroReaderAgainstWriter(writer, reader avroSchemaRecord) error {
	for name, writerField := range writer.fields {
		readerField, found := reader.fields[name]
		if !found {
			continue
		}
		writerTypes, err := avroTypeNames(writerField.typeSchema)
		if err != nil {
			return err
		}
		readerTypes, err := avroTypeNames(readerField.typeSchema)
		if err != nil {
			return err
		}
		if !avroWriterTypesFitReader(writerTypes, readerTypes) {
			return fmt.Errorf("%w: field %q type changed", ErrAvroSchemaIncompatible, name)
		}
	}
	for name, readerField := range reader.fields {
		if _, found := writer.fields[name]; !found && !readerField.hasDefault {
			return fmt.Errorf("%w: new required field %q has no default", ErrAvroSchemaIncompatible, name)
		}
	}
	return nil
}

func avroWriterTypesFitReader(writer, reader []string) bool {
	for _, writerType := range writer {
		fits := false
		for _, readerType := range reader {
			if writerType == readerType ||
				(writerType == "int" && (readerType == "long" || readerType == "float" || readerType == "double")) ||
				(writerType == "long" && (readerType == "float" || readerType == "double")) ||
				(writerType == "float" && readerType == "double") {
				fits = true
				break
			}
		}
		if !fits {
			return false
		}
	}
	return true
}
