package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// TupleFieldType identifies the compact physical encoding of one positional
// tuple field.
type TupleFieldType uint8

const (
	TupleFieldInvalid TupleFieldType = iota
	TupleFieldString
	TupleFieldBytes
	TupleFieldInt64
	TupleFieldUint64
	TupleFieldFloat64
	TupleFieldBool
	TupleFieldDate
	TupleFieldTimestamp
)

// TupleFieldValue is one typed tuple value. Valid false represents SQL NULL;
// its other fields are ignored in that state.
type TupleFieldValue struct {
	Kind    TupleFieldType
	String  string
	Bytes   []byte
	Int64   int64
	Uint64  uint64
	Float64 float64
	Bool    bool
	Time    time.Time
	Valid   bool
}

func TupleNull() TupleFieldValue { return TupleFieldValue{} }
func TupleString(value string) TupleFieldValue {
	return TupleFieldValue{Kind: TupleFieldString, String: value, Valid: true}
}
func TupleBytes(value []byte) TupleFieldValue {
	return TupleFieldValue{Kind: TupleFieldBytes, Bytes: append([]byte(nil), value...), Valid: true}
}
func TupleInt64(value int64) TupleFieldValue {
	return TupleFieldValue{Kind: TupleFieldInt64, Int64: value, Valid: true}
}
func TupleUint64(value uint64) TupleFieldValue {
	return TupleFieldValue{Kind: TupleFieldUint64, Uint64: value, Valid: true}
}
func TupleFloat64(value float64) TupleFieldValue {
	return TupleFieldValue{Kind: TupleFieldFloat64, Float64: value, Valid: true}
}
func TupleBool(value bool) TupleFieldValue {
	return TupleFieldValue{Kind: TupleFieldBool, Bool: value, Valid: true}
}
func TupleDate(value time.Time) TupleFieldValue {
	return TupleFieldValue{Kind: TupleFieldDate, Time: value.UTC(), Valid: true}
}
func TupleTimestamp(value time.Time) TupleFieldValue {
	return TupleFieldValue{Kind: TupleFieldTimestamp, Time: value.UTC(), Valid: true}
}

// TupleFieldGenerator supplies a missing field after all preceding fields
// have been resolved. The input is an independent copy and may be retained by
// the generator only if it copies it again.
type TupleFieldGenerator func([]TupleFieldValue) (TupleFieldValue, error)

// TupleFieldSpec declares one ordered field in a TupleFormat. Default takes
// precedence over Generated when an input omits the field.
type TupleFieldSpec struct {
	Name      string
	Type      TupleFieldType
	Nullable  bool
	Default   *TupleFieldValue
	Generated TupleFieldGenerator
}

// TupleFormat is an immutable positional tuple schema. Its physical records
// use one contiguous data buffer plus uint32 offsets, with an optional
// validity bitmap only when a record contains NULL values.
type TupleFormat struct {
	version uint64
	fields  []TupleFieldSpec
}

// NewTupleFormat validates and copies a positional format definition.
func NewTupleFormat(version uint64, fields []TupleFieldSpec) (TupleFormat, error) {
	if version == 0 {
		return TupleFormat{}, errors.New("hatDataStructure: tuple format version must be positive")
	}
	if len(fields) > MaxTupleFieldOffsetFields {
		return TupleFormat{}, fmt.Errorf("hatDataStructure: tuple format has %d fields, maximum %d", len(fields), MaxTupleFieldOffsetFields)
	}
	cloned := make([]TupleFieldSpec, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for index, field := range fields {
		field.Name = strings.TrimSpace(field.Name)
		if field.Name == "" {
			return TupleFormat{}, fmt.Errorf("hatDataStructure: tuple field %d name is required", index)
		}
		if _, exists := seen[field.Name]; exists {
			return TupleFormat{}, fmt.Errorf("hatDataStructure: duplicate tuple field %q", field.Name)
		}
		seen[field.Name] = struct{}{}
		if !validTupleFieldType(field.Type) {
			return TupleFormat{}, fmt.Errorf("hatDataStructure: tuple field %q has unsupported type %d", field.Name, field.Type)
		}
		if field.Default != nil {
			value := cloneTupleFieldValue(*field.Default)
			if err := validateTupleFieldValue(field, value); err != nil {
				return TupleFormat{}, fmt.Errorf("hatDataStructure: tuple field %q default: %w", field.Name, err)
			}
			field.Default = &value
		}
		cloned[index] = field
	}
	return TupleFormat{version: version, fields: cloned}, nil
}

// Version returns the format's schema version.
func (format TupleFormat) Version() uint64 { return format.version }

// FieldCount returns the number of positional fields.
func (format TupleFormat) FieldCount() int { return len(format.fields) }

// Fields returns an independent format definition copy.
func (format TupleFormat) Fields() []TupleFieldSpec {
	fields := make([]TupleFieldSpec, len(format.fields))
	for index, field := range format.fields {
		fields[index] = field
		if field.Default != nil {
			value := cloneTupleFieldValue(*field.Default)
			fields[index].Default = &value
		}
	}
	return fields
}

// Pack resolves supplied values, defaults, and generated fields into one
// compact TupleFieldOffsetCache. Missing nullable fields become NULL.
func (format TupleFormat) Pack(values []TupleFieldValue) (TupleFieldOffsetCache, error) {
	if err := format.validateDefinition(); err != nil {
		return TupleFieldOffsetCache{}, err
	}
	if len(values) > len(format.fields) {
		return TupleFieldOffsetCache{}, fmt.Errorf("hatDataStructure: tuple has %d values, format accepts %d", len(values), len(format.fields))
	}
	resolved := make([]TupleFieldValue, len(format.fields))
	lengths := make([]uint32, len(format.fields))
	var total uint64
	hasNull := false
	for index, field := range format.fields {
		var value TupleFieldValue
		switch {
		case index < len(values):
			value = cloneTupleFieldValue(values[index])
		case field.Default != nil:
			value = cloneTupleFieldValue(*field.Default)
		case field.Generated != nil:
			generated, err := field.Generated(cloneTupleFieldValues(resolved[:index]))
			if err != nil {
				return TupleFieldOffsetCache{}, fmt.Errorf("hatDataStructure: generate tuple field %q: %w", field.Name, err)
			}
			value = cloneTupleFieldValue(generated)
		default:
			value = TupleNull()
		}
		if err := validateTupleFieldValue(field, value); err != nil {
			return TupleFieldOffsetCache{}, fmt.Errorf("hatDataStructure: tuple field %q: %w", field.Name, err)
		}
		resolved[index] = value
		if !value.Valid {
			hasNull = true
			continue
		}
		length, err := tupleFieldEncodedLength(value)
		if err != nil {
			return TupleFieldOffsetCache{}, fmt.Errorf("hatDataStructure: tuple field %q: %w", field.Name, err)
		}
		total += uint64(length)
		if total < uint64(length) || total > maxTupleFieldOffsetUint32 || total > uint64(^uint(0)>>1) {
			return TupleFieldOffsetCache{}, ErrTupleFieldOffsetLengths
		}
		lengths[index] = uint32(length)
	}

	data := make([]byte, int(total))
	offsets := make([]uint32, len(resolved)+1)
	var offset uint32
	var valid []bool
	if hasNull {
		valid = make([]bool, len(resolved))
	}
	for index, value := range resolved {
		if hasNull {
			valid[index] = value.Valid
		}
		if value.Valid {
			if err := encodeTupleField(data[offset:offset+lengths[index]], value); err != nil {
				return TupleFieldOffsetCache{}, fmt.Errorf("hatDataStructure: tuple field %q: %w", format.fields[index].Name, err)
			}
			offset += lengths[index]
		}
		offsets[index+1] = offset
	}
	return TupleFieldOffsetCache{data: data, offsets: offsets, valid: valid}, nil
}

// Validate checks a packed tuple against the format without copying its data.
func (format TupleFormat) Validate(tuple TupleFieldOffsetCache) error {
	if err := format.validateDefinition(); err != nil {
		return err
	}
	if tuple.FieldCount() != len(format.fields) {
		return fmt.Errorf("hatDataStructure: tuple has %d fields, format requires %d", tuple.FieldCount(), len(format.fields))
	}
	for index, field := range format.fields {
		valid, err := tuple.FieldValid(index)
		if err != nil {
			return err
		}
		if !valid {
			if !field.Nullable {
				return fmt.Errorf("hatDataStructure: tuple field %q is NULL but not nullable", field.Name)
			}
			continue
		}
		data, err := tuple.Field(index)
		if err != nil {
			return err
		}
		if err := validateTupleFieldBytes(field.Type, data); err != nil {
			return fmt.Errorf("hatDataStructure: tuple field %q: %w", field.Name, err)
		}
	}
	return nil
}

// Unpack validates and decodes a packed tuple into independent typed values.
func (format TupleFormat) Unpack(tuple TupleFieldOffsetCache) ([]TupleFieldValue, error) {
	if err := format.Validate(tuple); err != nil {
		return nil, err
	}
	values := make([]TupleFieldValue, len(format.fields))
	for index, field := range format.fields {
		valid, _ := tuple.FieldValid(index)
		if !valid {
			continue
		}
		data, _ := tuple.Field(index)
		value, err := decodeTupleField(field.Type, data)
		if err != nil {
			return nil, fmt.Errorf("hatDataStructure: tuple field %q: %w", field.Name, err)
		}
		values[index] = value
	}
	return values, nil
}

func (format TupleFormat) validateDefinition() error {
	if format.version == 0 {
		return errors.New("hatDataStructure: tuple format is uninitialized")
	}
	return nil
}

func validTupleFieldType(fieldType TupleFieldType) bool {
	return fieldType >= TupleFieldString && fieldType <= TupleFieldTimestamp
}

func validateTupleFieldValue(field TupleFieldSpec, value TupleFieldValue) error {
	if !value.Valid {
		if !field.Nullable {
			return errors.New("NULL is not allowed")
		}
		return nil
	}
	if value.Kind != field.Type {
		return fmt.Errorf("value type %d does not match format type %d", value.Kind, field.Type)
	}
	return nil
}

func tupleFieldEncodedLength(value TupleFieldValue) (int, error) {
	switch value.Kind {
	case TupleFieldString:
		return len(value.String), nil
	case TupleFieldBytes:
		return len(value.Bytes), nil
	case TupleFieldInt64, TupleFieldUint64, TupleFieldFloat64, TupleFieldDate, TupleFieldTimestamp:
		return 8, nil
	case TupleFieldBool:
		return 1, nil
	default:
		return 0, errors.New("unsupported value type")
	}
}

func encodeTupleField(destination []byte, value TupleFieldValue) error {
	switch value.Kind {
	case TupleFieldString:
		copy(destination, value.String)
	case TupleFieldBytes:
		copy(destination, value.Bytes)
	case TupleFieldInt64:
		binary.BigEndian.PutUint64(destination, uint64(value.Int64))
	case TupleFieldUint64:
		binary.BigEndian.PutUint64(destination, value.Uint64)
	case TupleFieldFloat64:
		binary.BigEndian.PutUint64(destination, math.Float64bits(value.Float64))
	case TupleFieldBool:
		if value.Bool {
			destination[0] = 1
		}
	case TupleFieldDate:
		binary.BigEndian.PutUint64(destination, uint64(tupleDateDays(value.Time)))
	case TupleFieldTimestamp:
		binary.BigEndian.PutUint64(destination, uint64(value.Time.UnixNano()))
	default:
		return errors.New("unsupported value type")
	}
	return nil
}

func validateTupleFieldBytes(fieldType TupleFieldType, data []byte) error {
	switch fieldType {
	case TupleFieldString, TupleFieldBytes:
		return nil
	case TupleFieldInt64, TupleFieldUint64, TupleFieldFloat64, TupleFieldDate, TupleFieldTimestamp:
		if len(data) != 8 {
			return fmt.Errorf("fixed-width field has length %d, want 8", len(data))
		}
	case TupleFieldBool:
		if len(data) != 1 || data[0] > 1 {
			return errors.New("boolean field must be one byte equal to 0 or 1")
		}
	default:
		return errors.New("unsupported field type")
	}
	return nil
}

func decodeTupleField(fieldType TupleFieldType, data []byte) (TupleFieldValue, error) {
	if err := validateTupleFieldBytes(fieldType, data); err != nil {
		return TupleFieldValue{}, err
	}
	value := TupleFieldValue{Kind: fieldType, Valid: true}
	switch fieldType {
	case TupleFieldString:
		value.String = string(data)
	case TupleFieldBytes:
		value.Bytes = append([]byte(nil), data...)
	case TupleFieldInt64:
		value.Int64 = int64(binary.BigEndian.Uint64(data))
	case TupleFieldUint64:
		value.Uint64 = binary.BigEndian.Uint64(data)
	case TupleFieldFloat64:
		value.Float64 = math.Float64frombits(binary.BigEndian.Uint64(data))
	case TupleFieldBool:
		value.Bool = data[0] == 1
	case TupleFieldDate:
		value.Time = time.Unix(int64(binary.BigEndian.Uint64(data))*24*60*60, 0).UTC()
	case TupleFieldTimestamp:
		value.Time = time.Unix(0, int64(binary.BigEndian.Uint64(data))).UTC()
	}
	return value, nil
}

func tupleDateDays(value time.Time) int64 {
	utc := value.UTC()
	midnight := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
	return midnight.Unix() / (24 * 60 * 60)
}

func cloneTupleFieldValue(value TupleFieldValue) TupleFieldValue {
	value.Bytes = append([]byte(nil), value.Bytes...)
	return value
}

func cloneTupleFieldValues(values []TupleFieldValue) []TupleFieldValue {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]TupleFieldValue, len(values))
	for index, value := range values {
		cloned[index] = cloneTupleFieldValue(value)
	}
	return cloned
}
