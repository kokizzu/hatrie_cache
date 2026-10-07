package hatSql

import "strings"

const lowerIndexFieldPrefix = "\x00hatrie.lower:"
const upperIndexFieldPrefix = "\x00hatrie.upper:"

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

// UpperIndexField returns the internal resolver field used by an opt-in
// UPPER(field) equality index. It is intended for resolver implementations;
// SQL callers should use UPPER(field) in the query text.
func UpperIndexField(field string) string {
	return upperIndexFieldPrefix + field
}

// UpperIndexFieldName returns the source field represented by an internal
// UPPER(field) resolver field.
func UpperIndexFieldName(field string) (string, bool) {
	if !strings.HasPrefix(field, upperIndexFieldPrefix) {
		return "", false
	}
	field = strings.TrimPrefix(field, upperIndexFieldPrefix)
	return field, field != ""
}
