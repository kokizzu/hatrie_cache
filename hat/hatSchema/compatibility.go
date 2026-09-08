package hatSchema

import (
	"fmt"
	"reflect"
	"sort"
)

// SchemaCompatibilityChange describes one compatibility-relevant difference
// between two schema versions. Changes are returned in deterministic source
// and column order.
type SchemaCompatibilityChange struct {
	Kind   string `json:"kind"`
	Source string `json:"source,omitempty"`
	Column string `json:"column,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// SchemaCompatibilityReport is the result of a rolling schema preflight.
// Compatible is true only when both old and new nodes can safely participate
// in the same rolling deployment under the conservative rules documented by
// CheckRollingCompatibility.
type SchemaCompatibilityReport struct {
	Compatible      bool                        `json:"compatible"`
	PreviousVersion uint64                      `json:"previous_version"`
	NextVersion     uint64                      `json:"next_version"`
	Changes         []SchemaCompatibilityChange `json:"changes,omitempty"`
}

// CheckRollingCompatibility compares a previous schema with its proposed next
// version. It allows only changes that preserve both old and new node access:
// append-only nullable columns and relaxing an existing NOT NULL constraint.
// Source removal/addition, column removal/reordering/type changes, required
// columns, constraint changes, and version regression are incompatible.
// Neither input is modified.
func CheckRollingCompatibility(previous, next Schema) (SchemaCompatibilityReport, error) {
	report := SchemaCompatibilityReport{
		Compatible:      true,
		PreviousVersion: previous.Version,
		NextVersion:     next.Version,
	}
	if err := previous.Validate(); err != nil {
		return report, fmt.Errorf("previous schema: %w", err)
	}
	if err := next.Validate(); err != nil {
		return report, fmt.Errorf("next schema: %w", err)
	}
	addChange := func(change SchemaCompatibilityChange, compatible bool) {
		report.Changes = append(report.Changes, change)
		if !compatible {
			report.Compatible = false
		}
	}
	if next.Version < previous.Version {
		addChange(SchemaCompatibilityChange{Kind: "version_regressed", Detail: fmt.Sprintf("%d -> %d", previous.Version, next.Version)}, false)
	} else if next.Version == previous.Version {
		if previous.Fingerprint() != next.Fingerprint() {
			addChange(SchemaCompatibilityChange{Kind: "version_not_advanced"}, false)
		}
	} else {
		addChange(SchemaCompatibilityChange{Kind: "version_advanced"}, true)
	}

	names := schemaCompatibilitySourceNames(previous, next)
	for _, name := range names {
		previousSource, previousExists := previous.Sources[name]
		nextSource, nextExists := next.Sources[name]
		switch {
		case !previousExists:
			addChange(SchemaCompatibilityChange{Kind: "source_added", Source: name}, false)
		case !nextExists:
			addChange(SchemaCompatibilityChange{Kind: "source_removed", Source: name}, false)
		default:
			compareRollingSource(previousSource, nextSource, addChange)
		}
	}
	return report, nil
}

func schemaCompatibilitySourceNames(previous, next Schema) []string {
	names := make([]string, 0, len(previous.Sources)+len(next.Sources))
	seen := make(map[string]struct{}, len(previous.Sources)+len(next.Sources))
	for name := range previous.Sources {
		seen[name] = struct{}{}
		names = append(names, name)
	}
	for name := range next.Sources {
		if _, exists := seen[name]; exists {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func compareRollingSource(previous, next Source, addChange func(SchemaCompatibilityChange, bool)) {
	if previous.Name != next.Name {
		addChange(SchemaCompatibilityChange{Kind: "source_name_changed", Source: previous.Name, Detail: fmt.Sprintf("%q -> %q", previous.Name, next.Name)}, false)
	}
	previousIndexes := make(map[string]int, len(previous.Columns))
	for index, column := range previous.Columns {
		previousIndexes[column.Name] = index
	}
	nextIndexes := make(map[string]int, len(next.Columns))
	for index, column := range next.Columns {
		nextIndexes[column.Name] = index
	}
	for index, previousColumn := range previous.Columns {
		nextIndex, exists := nextIndexes[previousColumn.Name]
		if !exists {
			addChange(SchemaCompatibilityChange{Kind: "column_removed", Source: previous.Name, Column: previousColumn.Name}, false)
			continue
		}
		if nextIndex != index {
			addChange(SchemaCompatibilityChange{Kind: "column_reordered", Source: previous.Name, Column: previousColumn.Name}, false)
		}
		nextColumn := next.Columns[nextIndex]
		if previousColumn.Type != nextColumn.Type {
			addChange(SchemaCompatibilityChange{Kind: "column_type_changed", Source: previous.Name, Column: previousColumn.Name, Detail: fmt.Sprintf("%s -> %s", previousColumn.Type, nextColumn.Type)}, false)
		}
		if previousColumn.NotNull != nextColumn.NotNull {
			if previousColumn.NotNull && !nextColumn.NotNull {
				addChange(SchemaCompatibilityChange{Kind: "nullability_relaxed", Source: previous.Name, Column: previousColumn.Name}, true)
			} else {
				addChange(SchemaCompatibilityChange{Kind: "column_made_required", Source: previous.Name, Column: previousColumn.Name}, false)
			}
		}
	}
	for index, nextColumn := range next.Columns {
		if _, exists := previousIndexes[nextColumn.Name]; exists {
			continue
		}
		if nextColumn.NotNull {
			addChange(SchemaCompatibilityChange{Kind: "required_column_added", Source: next.Name, Column: nextColumn.Name}, false)
		} else {
			addChange(SchemaCompatibilityChange{Kind: "nullable_column_added", Source: next.Name, Column: nextColumn.Name}, true)
		}
		if index < len(previous.Columns) {
			addChange(SchemaCompatibilityChange{Kind: "column_reordered", Source: next.Name, Column: nextColumn.Name}, false)
		}
	}
	if !schemaCompatibilityConstraintsEqual(previous.Constraints, next.Constraints) {
		addChange(SchemaCompatibilityChange{Kind: "constraints_changed", Source: previous.Name}, false)
	}
}

func schemaCompatibilityConstraintsEqual(previous, next []Constraint) bool {
	if len(previous) != len(next) {
		return false
	}
	for index := range previous {
		if !reflect.DeepEqual(previous[index], next[index]) {
			return false
		}
	}
	return true
}
