package hatSql

import (
	"fmt"
	"strings"
)

const (
	// MaxSQLPartitionDeclarationFields bounds the metadata retained for one
	// source declaration. It prevents malformed catalogs from creating an
	// unbounded explain or information_schema response.
	MaxSQLPartitionDeclarationFields = 64
	maxSQLPartitionDeclarationText   = 256
)

// SQLPartitionOrder describes one source ordering column. Desc and
// NullsFirst use the same direction semantics as SQL ORDER BY planning.
type SQLPartitionOrder struct {
	Field      string `json:"field"`
	Desc       bool   `json:"desc,omitempty"`
	NullsFirst bool   `json:"nulls_first,omitempty"`
}

// SQLPartitionDeclaration describes the physical layout a source resolver
// exposes to query planning. It is advisory metadata: source resolvers remain
// responsible for preserving SQL semantics when they implement partition
// pruning or ordered partition reads.
type SQLPartitionDeclaration struct {
	Namespace      string              `json:"namespace,omitempty"`
	Source         string              `json:"source"`
	Kind           string              `json:"kind,omitempty"`
	PartitionBy    []string            `json:"partition_by,omitempty"`
	OrderBy        []SQLPartitionOrder `json:"order_by,omitempty"`
	PartitionCount int                 `json:"partition_count,omitempty"`
}

// SQLPartitionDeclarationResolver optionally exposes a stable partition and
// ordering declaration for one logical SQL source.
type SQLPartitionDeclarationResolver interface {
	ResolveSQLPartitionDeclaration(name string, key string) (SQLPartitionDeclaration, bool, error)
}

// ExplainPartitioningAnnotation associates one source-plan step with its
// physical layout declaration without expanding every ExplainStep value.
type ExplainPartitioningAnnotation struct {
	StepIndex   int                     `json:"step_index"`
	Declaration SQLPartitionDeclaration `json:"declaration"`
}

// ValidateSQLPartitionDeclaration validates metadata before it is published
// through a catalog, resolver, or explain plan.
func ValidateSQLPartitionDeclaration(declaration SQLPartitionDeclaration) error {
	_, err := normalizeSQLPartitionDeclaration(declaration)
	return err
}

func normalizeSQLPartitionDeclaration(declaration SQLPartitionDeclaration) (SQLPartitionDeclaration, error) {
	declaration.Namespace = strings.TrimSpace(declaration.Namespace)
	declaration.Source = strings.TrimSpace(declaration.Source)
	declaration.Kind = strings.TrimSpace(declaration.Kind)
	if declaration.Source == "" {
		return SQLPartitionDeclaration{}, fmt.Errorf("SQL partition declaration source is required")
	}
	if len(declaration.Source) > maxSQLPartitionDeclarationText {
		return SQLPartitionDeclaration{}, fmt.Errorf("SQL partition declaration source exceeds %d bytes", maxSQLPartitionDeclarationText)
	}
	if len(declaration.Namespace) > maxSQLPartitionDeclarationText {
		return SQLPartitionDeclaration{}, fmt.Errorf("SQL partition declaration namespace exceeds %d bytes", maxSQLPartitionDeclarationText)
	}
	if len(declaration.Kind) > maxSQLPartitionDeclarationText {
		return SQLPartitionDeclaration{}, fmt.Errorf("SQL partition declaration kind exceeds %d bytes", maxSQLPartitionDeclarationText)
	}
	if declaration.PartitionCount < 0 {
		return SQLPartitionDeclaration{}, fmt.Errorf("SQL partition declaration partition count cannot be negative")
	}
	if len(declaration.PartitionBy)+len(declaration.OrderBy) > MaxSQLPartitionDeclarationFields {
		return SQLPartitionDeclaration{}, fmt.Errorf("SQL partition declaration has too many fields")
	}
	declaration.PartitionBy = append([]string(nil), declaration.PartitionBy...)
	seenPartition := make(map[string]struct{}, len(declaration.PartitionBy))
	for index, field := range declaration.PartitionBy {
		field, err := normalizeSQLPartitionField(field)
		if err != nil {
			return SQLPartitionDeclaration{}, fmt.Errorf("partition field %d: %w", index, err)
		}
		key := strings.ToLower(field)
		if _, exists := seenPartition[key]; exists {
			return SQLPartitionDeclaration{}, fmt.Errorf("duplicate SQL partition declaration field %q", field)
		}
		seenPartition[key] = struct{}{}
		declaration.PartitionBy[index] = field
	}
	declaration.OrderBy = append([]SQLPartitionOrder(nil), declaration.OrderBy...)
	seenOrder := make(map[string]struct{}, len(declaration.OrderBy))
	for index := range declaration.OrderBy {
		field, err := normalizeSQLPartitionField(declaration.OrderBy[index].Field)
		if err != nil {
			return SQLPartitionDeclaration{}, fmt.Errorf("order field %d: %w", index, err)
		}
		key := strings.ToLower(field)
		if _, exists := seenOrder[key]; exists {
			return SQLPartitionDeclaration{}, fmt.Errorf("duplicate SQL partition declaration field %q", field)
		}
		seenOrder[key] = struct{}{}
		declaration.OrderBy[index].Field = field
	}
	return declaration, nil
}

func normalizeSQLPartitionField(field string) (string, error) {
	field = strings.TrimSpace(field)
	if field == "" {
		return "", fmt.Errorf("field is required")
	}
	if len(field) > maxSQLPartitionDeclarationText {
		return "", fmt.Errorf("field exceeds %d bytes", maxSQLPartitionDeclarationText)
	}
	for _, character := range field {
		if character <= ' ' || character == '\'' || character == '"' || character == ';' {
			return "", fmt.Errorf("field %q contains an invalid character", field)
		}
	}
	return field, nil
}

func cloneSQLPartitionDeclaration(declaration SQLPartitionDeclaration) SQLPartitionDeclaration {
	declaration.PartitionBy = append([]string(nil), declaration.PartitionBy...)
	declaration.OrderBy = append([]SQLPartitionOrder(nil), declaration.OrderBy...)
	return declaration
}

func cloneSQLPartitionDeclarationPointer(declaration *SQLPartitionDeclaration) *SQLPartitionDeclaration {
	if declaration == nil {
		return nil
	}
	clone := cloneSQLPartitionDeclaration(*declaration)
	return &clone
}

func resolveSQLPartitionDeclarationForSource(resolver SQLSourceResolver, source sqlSource) *SQLPartitionDeclaration {
	if resolver == nil || source.kind != "CACHE" && source.kind != "KEYS" {
		return nil
	}
	declarations, ok := resolver.(SQLPartitionDeclarationResolver)
	if !ok {
		return nil
	}
	declaration, available, err := declarations.ResolveSQLPartitionDeclaration(source.kind, source.key)
	if err != nil || !available {
		return nil
	}
	declaration, err = normalizeSQLPartitionDeclaration(declaration)
	if err != nil {
		return nil
	}
	return cloneSQLPartitionDeclarationPointer(&declaration)
}

func sqlExplainPartitioningForStep(annotations []ExplainPartitioningAnnotation, stepIndex int) *SQLPartitionDeclaration {
	for _, annotation := range annotations {
		if annotation.StepIndex != stepIndex {
			continue
		}
		declaration := cloneSQLPartitionDeclaration(annotation.Declaration)
		return &declaration
	}
	return nil
}
