package hatDictionary

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"hatrie_cache/hat/hatSql"
)

var (
	ErrSQLDictionaryNil        = errors.New("hatDictionary: SQL dictionary is nil")
	ErrSQLBaseResolverNil      = errors.New("hatDictionary: SQL base resolver is nil")
	ErrSQLLookupOptionsInvalid = errors.New("hatDictionary: SQL lookup options are invalid")
	ErrSQLFullScanUnavailable  = errors.New("hatDictionary: SQL dictionary full scan is unavailable")
)

// SQLDictionaryKeyEncoder converts a SQL join value to a dictionary key.
// The default encoder handles strings, byte slices, booleans, and Go numeric
// values. Return false when a value cannot be represented by the dictionary.
type SQLDictionaryKeyEncoder func(value interface{}) (key string, ok bool)

// SQLDictionaryLookupOptions binds a dictionary to one EXTERNAL source.
// SourceKey is the argument in EXTERNAL(SourceKey); KeyField is the indexed
// join field and ValueField receives the dictionary value. ExpectedVersion is
// optional and pins every lookup to one dictionary snapshot.
type SQLDictionaryLookupOptions struct {
	SourceKey       string
	KeyField        string
	ValueField      string
	ExpectedVersion string
	KeyEncoder      SQLDictionaryKeyEncoder
	FullScan        hatSql.ExternalSourceResolver
}

// SQLDictionaryLookupResolver combines a normal SQL source resolver with a
// bounded dictionary-backed EXTERNAL lookup arrangement. It intentionally
// does not enumerate dictionary entries; joins probe the dictionary by key.
type SQLDictionaryLookupResolver struct {
	base       hatSql.SQLSourceResolver
	dictionary *Dictionary
	options    SQLDictionaryLookupOptions
	encode     SQLDictionaryKeyEncoder
	fullScan   hatSql.ExternalSourceResolver
}

// NewSQLDictionaryLookupResolver creates a resolver that delegates ordinary
// sources to base and serves EXTERNAL(SourceKey) equality joins from the
// dictionary. A full scan delegate is optional because lookup joins do not
// need to materialize the external source.
func NewSQLDictionaryLookupResolver(base hatSql.SQLSourceResolver, dictionary *Dictionary, options SQLDictionaryLookupOptions) (*SQLDictionaryLookupResolver, error) {
	if base == nil {
		return nil, ErrSQLBaseResolverNil
	}
	if dictionary == nil {
		return nil, ErrSQLDictionaryNil
	}
	options.SourceKey = strings.TrimSpace(options.SourceKey)
	options.KeyField = strings.TrimSpace(options.KeyField)
	options.ValueField = strings.TrimSpace(options.ValueField)
	options.ExpectedVersion = strings.TrimSpace(options.ExpectedVersion)
	if options.SourceKey == "" || options.KeyField == "" || options.ValueField == "" || options.KeyField == options.ValueField || strings.IndexByte(options.SourceKey, 0) >= 0 || strings.IndexByte(options.KeyField, 0) >= 0 || strings.IndexByte(options.ValueField, 0) >= 0 {
		return nil, ErrSQLLookupOptionsInvalid
	}
	fullScan := options.FullScan
	if fullScan == nil {
		fullScan, _ = base.(hatSql.ExternalSourceResolver)
	}
	if options.KeyEncoder == nil {
		options.KeyEncoder = defaultSQLDictionaryKeyEncoder
	}
	return &SQLDictionaryLookupResolver{
		base:       base,
		dictionary: dictionary,
		options:    options,
		encode:     options.KeyEncoder,
		fullScan:   fullScan,
	}, nil
}

// ResolveSQLSource delegates non-dictionary source resolution to the base
// resolver, allowing one resolver to serve both fact and dimension inputs.
func (resolver *SQLDictionaryLookupResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if resolver == nil || resolver.base == nil {
		return nil, ErrSQLBaseResolverNil
	}
	return resolver.base.ResolveSQLSource(name, key)
}

// ResolveSQLLookupSource serves an exact equality candidate for the bound
// EXTERNAL source. It returns available=true for unsupported or missing keys
// so SQL can treat them as ordinary lookup misses rather than scanning an
// unenumerable dictionary.
func (resolver *SQLDictionaryLookupResolver) ResolveSQLLookupSource(name, key, field string, value interface{}) ([]hatSql.Row, bool, error) {
	if resolver == nil || resolver.dictionary == nil {
		return nil, false, ErrSQLDictionaryNil
	}
	if !strings.EqualFold(name, "EXTERNAL") || key != resolver.options.SourceKey || field != resolver.options.KeyField {
		return nil, false, nil
	}
	if value == nil {
		return nil, true, nil
	}
	dictionaryKey, ok := resolver.encode(value)
	if !ok {
		return nil, true, nil
	}
	result, err := resolver.dictionary.LookupAtVersion(context.Background(), dictionaryKey, resolver.options.ExpectedVersion)
	if err != nil {
		return nil, true, err
	}
	if !result.Found {
		return nil, true, nil
	}
	return []hatSql.Row{{resolver.options.KeyField: value, resolver.options.ValueField: result.Value}}, true, nil
}

// ResolveSQLExternalSource delegates a full external scan when one was
// supplied. Without one it fails explicitly instead of pretending the
// dictionary can be enumerated.
func (resolver *SQLDictionaryLookupResolver) ResolveSQLExternalSource(name string) ([]hatSql.Row, error) {
	if resolver == nil {
		return nil, ErrSQLDictionaryNil
	}
	if resolver.fullScan == nil {
		return nil, fmt.Errorf("%w for EXTERNAL(%q)", ErrSQLFullScanUnavailable, name)
	}
	return resolver.fullScan.ResolveSQLExternalSource(name)
}

func defaultSQLDictionaryKeyEncoder(value interface{}) (string, bool) {
	switch value := value.(type) {
	case string:
		return value, true
	case []byte:
		return string(value), true
	case bool:
		return strconv.FormatBool(value), true
	case int:
		return strconv.FormatInt(int64(value), 10), true
	case int8:
		return strconv.FormatInt(int64(value), 10), true
	case int16:
		return strconv.FormatInt(int64(value), 10), true
	case int32:
		return strconv.FormatInt(int64(value), 10), true
	case int64:
		return strconv.FormatInt(value, 10), true
	case uint:
		return strconv.FormatUint(uint64(value), 10), true
	case uint8:
		return strconv.FormatUint(uint64(value), 10), true
	case uint16:
		return strconv.FormatUint(uint64(value), 10), true
	case uint32:
		return strconv.FormatUint(uint64(value), 10), true
	case uint64:
		return strconv.FormatUint(value, 10), true
	case float32:
		if math.IsNaN(float64(value)) {
			return "NaN", true
		}
		return strconv.FormatFloat(float64(value), 'g', -1, 32), true
	case float64:
		if math.IsNaN(value) {
			return "NaN", true
		}
		return strconv.FormatFloat(value, 'g', -1, 64), true
	default:
		return "", false
	}
}
