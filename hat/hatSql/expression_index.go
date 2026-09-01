package hatSql

import "strings"

const lowerIndexFieldPrefix = "\x00hatrie.lower:"

// LowerIndexField returns the internal resolver field used by an opt-in
// LOWER(field) equality index. It is intended for resolver implementations;
// SQL callers should use LOWER(field) in the query text.
func LowerIndexField(field string) string {
	return lowerIndexFieldPrefix + field
}

// LowerIndexFieldName returns the source field represented by an internal
// LOWER(field) resolver field.
func LowerIndexFieldName(field string) (string, bool) {
	if !strings.HasPrefix(field, lowerIndexFieldPrefix) {
		return "", false
	}
	field = strings.TrimPrefix(field, lowerIndexFieldPrefix)
	return field, field != ""
}
