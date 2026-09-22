package hatReplication

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	MaxChangefeedSchemaFields    = 4096
	MaxChangefeedSchemaFieldName = 256
	MaxChangefeedSchemaFieldType = 128
	MaxChangefeedSchemaVersion   = MaxSpaceChangefeedNameBytes
)

var (
	ErrChangefeedSchemaInvalid                   = errors.New("hatriecache: changefeed schema is invalid")
	ErrChangefeedSchemaIncompatible              = errors.New("hatriecache: changefeed schema is incompatible")
	ErrChangefeedSchemaVersionConflict           = errors.New("hatriecache: changefeed schema version conflict")
	ErrSpaceChangefeedSchemaEvolutionUnavailable = errors.New("hatriecache: typed schema evolution is unavailable")
)

// ChangefeedSchemaField describes one named value carried by a changefeed.
// Existing fields are immutable across additive schema versions. New fields
// must be nullable or have a default so old producers and consumers remain
// readable.
type ChangefeedSchemaField struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Nullable   bool   `json:"nullable"`
	HasDefault bool   `json:"has_default"`
}

// ChangefeedSchema is the typed schema identity for one source version.
type ChangefeedSchema struct {
	Version string                  `json:"version"`
	Fields  []ChangefeedSchemaField `json:"fields"`
}

// ChangefeedSchemaCompatibility reports an accepted additive transition.
type ChangefeedSchemaCompatibility struct {
	PreviousVersion string
	NextVersion     string
	AddedFields     []string
}

// ChangefeedSchemaRegistry stores one atomically replaceable current schema.
// It is useful for producers and consumers that do not use SpaceChangefeed.
type ChangefeedSchemaRegistry struct {
	mu      sync.RWMutex
	current ChangefeedSchema
}

// NewChangefeedSchemaRegistry creates a registry with an initial schema.
func NewChangefeedSchemaRegistry(initial ChangefeedSchema) (*ChangefeedSchemaRegistry, error) {
	normalized, err := normalizeChangefeedSchema(initial)
	if err != nil {
		return nil, err
	}
	return &ChangefeedSchemaRegistry{current: normalized}, nil
}

// Current returns a copy of the registry's current schema.
func (registry *ChangefeedSchemaRegistry) Current() ChangefeedSchema {
	if registry == nil {
		return ChangefeedSchema{}
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return cloneChangefeedSchema(registry.current)
}

// Evolve atomically installs an additive next schema.
func (registry *ChangefeedSchemaRegistry) Evolve(next ChangefeedSchema) (ChangefeedSchemaCompatibility, error) {
	if registry == nil {
		return ChangefeedSchemaCompatibility{}, ErrChangefeedSchemaInvalid
	}
	normalized, err := normalizeChangefeedSchema(next)
	if err != nil {
		return ChangefeedSchemaCompatibility{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	compatibility, err := checkChangefeedSchemaEvolutionNormalized(registry.current, normalized)
	if err != nil {
		return ChangefeedSchemaCompatibility{}, err
	}
	registry.current = normalized
	return compatibility, nil
}

// Accepts reports whether consumer can read records produced with the current
// schema. Unknown producer fields are ignored; missing consumer fields must be
// nullable or have a default.
func (registry *ChangefeedSchemaRegistry) Accepts(consumer ChangefeedSchema) error {
	if registry == nil {
		return ErrChangefeedSchemaInvalid
	}
	normalized, err := normalizeChangefeedSchema(consumer)
	if err != nil {
		return err
	}
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return checkChangefeedSchemaCompatibilityNormalized(registry.current, normalized)
}

// CheckChangefeedSchemaEvolution validates that next only adds safe fields.
func CheckChangefeedSchemaEvolution(previous, next ChangefeedSchema) (ChangefeedSchemaCompatibility, error) {
	previousNormalized, err := normalizeChangefeedSchema(previous)
	if err != nil {
		return ChangefeedSchemaCompatibility{}, err
	}
	nextNormalized, err := normalizeChangefeedSchema(next)
	if err != nil {
		return ChangefeedSchemaCompatibility{}, err
	}
	return checkChangefeedSchemaEvolutionNormalized(previousNormalized, nextNormalized)
}

// CheckChangefeedSchemaCompatibility validates a producer/consumer pairing.
func CheckChangefeedSchemaCompatibility(producer, consumer ChangefeedSchema) error {
	producerNormalized, err := normalizeChangefeedSchema(producer)
	if err != nil {
		return err
	}
	consumerNormalized, err := normalizeChangefeedSchema(consumer)
	if err != nil {
		return err
	}
	return checkChangefeedSchemaCompatibilityNormalized(producerNormalized, consumerNormalized)
}

func checkChangefeedSchemaEvolutionNormalized(previous, next ChangefeedSchema) (ChangefeedSchemaCompatibility, error) {
	if previous.Version == next.Version && !equalChangefeedSchema(previous, next) {
		return ChangefeedSchemaCompatibility{}, ErrChangefeedSchemaVersionConflict
	}
	previousFields := changefeedSchemaFieldMap(previous.Fields)
	nextFields := changefeedSchemaFieldMap(next.Fields)
	for name, previousField := range previousFields {
		nextField, ok := nextFields[name]
		if !ok || !equalChangefeedSchemaField(previousField, nextField) {
			return ChangefeedSchemaCompatibility{}, fmt.Errorf("%w: field=%q", ErrChangefeedSchemaIncompatible, name)
		}
	}
	added := make([]string, 0, len(next.Fields)-len(previous.Fields))
	for _, field := range next.Fields {
		if _, existed := previousFields[field.Name]; existed {
			continue
		}
		if !field.Nullable && !field.HasDefault {
			return ChangefeedSchemaCompatibility{}, fmt.Errorf("%w: required added field=%q", ErrChangefeedSchemaIncompatible, field.Name)
		}
		added = append(added, field.Name)
	}
	return ChangefeedSchemaCompatibility{
		PreviousVersion: previous.Version,
		NextVersion:     next.Version,
		AddedFields:     added,
	}, nil
}

func checkChangefeedSchemaCompatibilityNormalized(producer, consumer ChangefeedSchema) error {
	if producer.Version == consumer.Version && !equalChangefeedSchema(producer, consumer) {
		return ErrChangefeedSchemaVersionConflict
	}
	producerFields := changefeedSchemaFieldMap(producer.Fields)
	for _, consumerField := range consumer.Fields {
		producerField, exists := producerFields[consumerField.Name]
		if !exists {
			if consumerField.Nullable || consumerField.HasDefault {
				continue
			}
			return fmt.Errorf("%w: missing required field=%q", ErrChangefeedSchemaIncompatible, consumerField.Name)
		}
		if !equalChangefeedSchemaField(producerField, consumerField) {
			return fmt.Errorf("%w: field=%q", ErrChangefeedSchemaIncompatible, consumerField.Name)
		}
	}
	return nil
}

func normalizeChangefeedSchema(schema ChangefeedSchema) (ChangefeedSchema, error) {
	version := strings.TrimSpace(schema.Version)
	if version == "" || len(version) > MaxChangefeedSchemaVersion || !utf8.ValidString(version) || strings.IndexByte(version, 0) >= 0 {
		return ChangefeedSchema{}, ErrChangefeedSchemaInvalid
	}
	if len(schema.Fields) > MaxChangefeedSchemaFields {
		return ChangefeedSchema{}, ErrChangefeedSchemaInvalid
	}
	normalized := ChangefeedSchema{
		Version: version,
		Fields:  make([]ChangefeedSchemaField, len(schema.Fields)),
	}
	seen := make(map[string]struct{}, len(schema.Fields))
	for index, field := range schema.Fields {
		field.Name = strings.TrimSpace(field.Name)
		field.Type = strings.ToLower(strings.TrimSpace(field.Type))
		if field.Name == "" || len(field.Name) > MaxChangefeedSchemaFieldName || !utf8.ValidString(field.Name) || strings.IndexByte(field.Name, 0) >= 0 || field.Type == "" || len(field.Type) > MaxChangefeedSchemaFieldType || !utf8.ValidString(field.Type) || strings.IndexByte(field.Type, 0) >= 0 {
			return ChangefeedSchema{}, ErrChangefeedSchemaInvalid
		}
		if _, exists := seen[field.Name]; exists {
			return ChangefeedSchema{}, fmt.Errorf("%w: duplicate field=%q", ErrChangefeedSchemaInvalid, field.Name)
		}
		seen[field.Name] = struct{}{}
		normalized.Fields[index] = field
	}
	return normalized, nil
}

func changefeedSchemaFieldMap(fields []ChangefeedSchemaField) map[string]ChangefeedSchemaField {
	indexed := make(map[string]ChangefeedSchemaField, len(fields))
	for _, field := range fields {
		indexed[field.Name] = field
	}
	return indexed
}

func equalChangefeedSchema(left, right ChangefeedSchema) bool {
	if left.Version != right.Version || len(left.Fields) != len(right.Fields) {
		return false
	}
	for index := range left.Fields {
		if !equalChangefeedSchemaField(left.Fields[index], right.Fields[index]) {
			return false
		}
	}
	return true
}

func equalChangefeedSchemaField(left, right ChangefeedSchemaField) bool {
	return left.Name == right.Name && left.Type == right.Type && left.Nullable == right.Nullable && left.HasDefault == right.HasDefault
}

func cloneChangefeedSchema(schema ChangefeedSchema) ChangefeedSchema {
	schema.Fields = append([]ChangefeedSchemaField(nil), schema.Fields...)
	return schema
}
