package hatSchema

import (
	"hash/fnv"
	"io"
	"sort"
	"strconv"
)

// Fingerprint returns a deterministic content fingerprint for this schema.
// Source-map iteration order does not affect the result; ordered columns and
// constraints remain part of the contract.
func (schema Schema) Fingerprint() string {
	hash := fnv.New64a()
	part := func(value string) {
		_, _ = io.WriteString(hash, value)
		_, _ = hash.Write([]byte{0})
	}
	part(strconv.FormatUint(schema.Version, 10))
	names := make([]string, 0, len(schema.Sources))
	for name := range schema.Sources {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		source := schema.Sources[name]
		part(name)
		part(source.Name)
		for _, column := range source.Columns {
			part(column.Name)
			part(string(column.Type))
			if column.NotNull {
				part("1")
			} else {
				part("0")
			}
		}
		for _, constraint := range source.Constraints {
			part(constraint.Name)
			part(string(constraint.Kind))
			part(constraint.Expression)
			part(constraint.ReferenceSource)
			for _, column := range constraint.Columns {
				part(column)
			}
			for _, column := range constraint.ReferenceColumns {
				part(column)
			}
		}
	}
	return strconv.FormatUint(hash.Sum64(), 16)
}
