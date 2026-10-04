package hatSql

import (
	"errors"
	"strings"
)

const (
	maxSQLSourceLayoutFields     = 16
	maxSQLSourceLayoutFieldBytes = 256
)

// ErrSQLSourceLayoutInvalid indicates that an optional partition/order
// declaration is empty, duplicated, or exceeds the bounded metadata limits.
var ErrSQLSourceLayoutInvalid = errors.New("hatSql: SQL source layout is invalid")

// SQLSourceOrder describes one declared source ordering key. A false value
// for both NULL flags leaves null ordering to the source's normal semantics.
type SQLSourceOrder struct {
	Field      string `json:"field"`
	Descending bool   `json:"descending,omitempty"`
	NullsFirst bool   `json:"nulls_first,omitempty"`
	NullsLast  bool   `json:"nulls_last,omitempty"`
}

// SQLSourceLayout is optional physical-layout metadata for one logical SQL
// source. PartitionBy describes stable partition keys; OrderBy describes the
// order each partition is expected to maintain. It is a planning contract and
// never changes query semantics by itself.
type SQLSourceLayout struct {
	PartitionBy []string         `json:"partition_by,omitempty"`
	OrderBy     []SQLSourceOrder `json:"order_by,omitempty"`
}

// SQLSourceLayoutResolver optionally declares source partitioning and order
// metadata. The executor uses it for EXPLAIN and planning diagnostics only;
// sources that do not implement this contract retain the existing path.
type SQLSourceLayoutResolver interface {
	ResolveSQLSourceLayout(name string, key string) (SQLSourceLayout, bool, error)
}

func resolveSQLSourceLayout(resolver SQLSourceResolver, source sqlSource) *SQLSourceLayout {
	if resolver == nil {
		return nil
	}
	layoutResolver, ok := resolver.(SQLSourceLayoutResolver)
	if !ok {
		return nil
	}
	layout, available, err := layoutResolver.ResolveSQLSourceLayout(source.kind, source.key)
	if err != nil || !available {
		return nil
	}
	normalized, err := normalizeSQLSourceLayout(layout)
	if err != nil || len(normalized.PartitionBy) == 0 && len(normalized.OrderBy) == 0 {
		return nil
	}
	return &normalized
}

func normalizeSQLSourceLayout(layout SQLSourceLayout) (SQLSourceLayout, error) {
	if len(layout.PartitionBy) > maxSQLSourceLayoutFields || len(layout.OrderBy) > maxSQLSourceLayoutFields {
		return SQLSourceLayout{}, ErrSQLSourceLayoutInvalid
	}
	normalized := SQLSourceLayout{
		PartitionBy: make([]string, 0, len(layout.PartitionBy)),
		OrderBy:     make([]SQLSourceOrder, 0, len(layout.OrderBy)),
	}
	partitionFields := make(map[string]struct{}, len(layout.PartitionBy))
	for _, field := range layout.PartitionBy {
		field = strings.TrimSpace(field)
		if field == "" || len(field) > maxSQLSourceLayoutFieldBytes {
			return SQLSourceLayout{}, ErrSQLSourceLayoutInvalid
		}
		key := strings.ToLower(field)
		if _, exists := partitionFields[key]; exists {
			return SQLSourceLayout{}, ErrSQLSourceLayoutInvalid
		}
		partitionFields[key] = struct{}{}
		normalized.PartitionBy = append(normalized.PartitionBy, strings.Clone(field))
	}
	orderFields := make(map[string]struct{}, len(layout.OrderBy))
	for _, order := range layout.OrderBy {
		order.Field = strings.TrimSpace(order.Field)
		if order.Field == "" || len(order.Field) > maxSQLSourceLayoutFieldBytes || order.NullsFirst && order.NullsLast {
			return SQLSourceLayout{}, ErrSQLSourceLayoutInvalid
		}
		key := strings.ToLower(order.Field)
		if _, exists := orderFields[key]; exists {
			return SQLSourceLayout{}, ErrSQLSourceLayoutInvalid
		}
		orderFields[key] = struct{}{}
		order.Field = strings.Clone(order.Field)
		normalized.OrderBy = append(normalized.OrderBy, order)
	}
	return normalized, nil
}

func cloneSQLSourceLayout(layout *SQLSourceLayout) *SQLSourceLayout {
	if layout == nil {
		return nil
	}
	cloned := SQLSourceLayout{
		PartitionBy: append([]string(nil), layout.PartitionBy...),
		OrderBy:     append([]SQLSourceOrder(nil), layout.OrderBy...),
	}
	for index := range cloned.PartitionBy {
		cloned.PartitionBy[index] = strings.Clone(cloned.PartitionBy[index])
	}
	for index := range cloned.OrderBy {
		cloned.OrderBy[index].Field = strings.Clone(cloned.OrderBy[index].Field)
	}
	return &cloned
}
