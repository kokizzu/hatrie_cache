package hatDataStructure

import (
	"errors"
	"fmt"
)

var (
	// ErrTupleFormatIncompatibleReader indicates that a reader cannot safely
	// consume the source format's positional fields.
	ErrTupleFormatIncompatibleReader = errors.New("hatDataStructure: tuple format reader is incompatible")
	// ErrTupleFormatReaderTupleCount indicates that a tuple does not have the
	// exact field count advertised by the source format.
	ErrTupleFormatReaderTupleCount = errors.New("hatDataStructure: tuple format reader source field count mismatch")
)

// TupleFormatReader adapts tuples produced by one immutable format to another
// compatible format. Compatibility is positional: common fields retain their
// name and physical type, while a target reader may be more nullable. Missing
// target suffix fields must have a default, a generator, or be nullable.
//
// The source format is validated in full before the target prefix is decoded,
// so extra fields are ignored only after their source representation has been
// checked. The adapter does not copy tuple bytes; decoded strings and byte
// fields follow TupleFormat.Unpack's ownership behavior.
type TupleFormatReader struct {
	source     TupleFormat
	target     TupleFormat
	exactShape bool
}

// NewTupleFormatReader creates a reader from source (writer) format to target
// (reader) format. Additive trailing fields are compatible in both directions:
// an older reader can ignore newer validated fields, and a newer reader can
// resolve omitted trailing fields from its default/generator/nullability rules.
func NewTupleFormatReader(source, target TupleFormat) (TupleFormatReader, error) {
	if err := source.validateDefinition(); err != nil {
		return TupleFormatReader{}, err
	}
	if err := target.validateDefinition(); err != nil {
		return TupleFormatReader{}, err
	}
	commonFields := len(source.fields)
	if len(target.fields) < commonFields {
		commonFields = len(target.fields)
	}
	for index := 0; index < commonFields; index++ {
		sourceField := source.fields[index]
		targetField := target.fields[index]
		if sourceField.Name != targetField.Name {
			return TupleFormatReader{}, fmt.Errorf("%w: field %d name %q does not match source name %q", ErrTupleFormatIncompatibleReader, index, targetField.Name, sourceField.Name)
		}
		if sourceField.Type != targetField.Type {
			return TupleFormatReader{}, fmt.Errorf("%w: field %q type %d does not match source type %d", ErrTupleFormatIncompatibleReader, sourceField.Name, targetField.Type, sourceField.Type)
		}
		if sourceField.Nullable && !targetField.Nullable {
			return TupleFormatReader{}, fmt.Errorf("%w: field %q may be NULL in source but is required by target", ErrTupleFormatIncompatibleReader, sourceField.Name)
		}
	}
	for index := commonFields; index < len(target.fields); index++ {
		field := target.fields[index]
		if field.Default == nil && field.Generated == nil && !field.Nullable {
			return TupleFormatReader{}, fmt.Errorf("%w: missing target field %q has no default, generator, or nullable rule", ErrTupleFormatIncompatibleReader, field.Name)
		}
	}
	exactShape := len(source.fields) == len(target.fields)
	if exactShape {
		for index := range source.fields {
			sourceField := source.fields[index]
			targetField := target.fields[index]
			if sourceField.Name != targetField.Name || sourceField.Type != targetField.Type || sourceField.Nullable != targetField.Nullable {
				exactShape = false
				break
			}
		}
	}
	return TupleFormatReader{source: source, target: target, exactShape: exactShape}, nil
}

// ReaderFor creates a reader that adapts source tuples to this target format.
func (format TupleFormat) ReaderFor(source TupleFormat) (TupleFormatReader, error) {
	return NewTupleFormatReader(source, format)
}

// SourceVersion returns the version expected on incoming tuples.
func (reader TupleFormatReader) SourceVersion() uint64 { return reader.source.version }

// TargetVersion returns the version exposed by decoded values.
func (reader TupleFormatReader) TargetVersion() uint64 { return reader.target.version }

// Validate checks the source tuple's exact count and physical fields without
// resolving target defaults or invoking target generators.
func (reader TupleFormatReader) Validate(tuple TupleFieldOffsetCache) error {
	if err := reader.validateDefinition(); err != nil {
		return err
	}
	if tuple.FieldCount() != len(reader.source.fields) {
		return fmt.Errorf("%w: got %d fields, want %d", ErrTupleFormatReaderTupleCount, tuple.FieldCount(), len(reader.source.fields))
	}
	return reader.source.validateTuple(tuple)
}

// Unpack validates a source tuple and decodes it into the target format. Any
// target suffix omitted by the source is resolved in field order, matching
// TupleFormat.Pack's default, generated, and nullable semantics.
func (reader TupleFormatReader) Unpack(tuple TupleFieldOffsetCache) ([]TupleFieldValue, error) {
	if reader.exactShape {
		if tuple.FieldCount() != len(reader.source.fields) {
			return nil, fmt.Errorf("%w: got %d fields, want %d", ErrTupleFormatReaderTupleCount, tuple.FieldCount(), len(reader.source.fields))
		}
		return reader.target.Unpack(tuple)
	}
	if err := reader.Validate(tuple); err != nil {
		return nil, err
	}
	values := make([]TupleFieldValue, len(reader.target.fields))
	commonFields := len(reader.source.fields)
	if len(reader.target.fields) < commonFields {
		commonFields = len(reader.target.fields)
	}
	for index := 0; index < commonFields; index++ {
		valid, err := tuple.FieldValid(index)
		if err != nil {
			return nil, err
		}
		if !valid {
			continue
		}
		data, err := tuple.Field(index)
		if err != nil {
			return nil, err
		}
		value, err := decodeTupleField(reader.target.fields[index].Type, data)
		if err != nil {
			return nil, fmt.Errorf("hatDataStructure: tuple field %q: %w", reader.target.fields[index].Name, err)
		}
		values[index] = value
	}
	for index := commonFields; index < len(reader.target.fields); index++ {
		field := reader.target.fields[index]
		value := TupleNull()
		switch {
		case field.Default != nil:
			value = cloneTupleFieldValue(*field.Default)
		case field.Generated != nil:
			generated, err := field.Generated(cloneTupleFieldValues(values[:index]))
			if err != nil {
				return nil, fmt.Errorf("hatDataStructure: generate tuple field %q: %w", field.Name, err)
			}
			value = cloneTupleFieldValue(generated)
		}
		if err := validateTupleFieldValue(field, value); err != nil {
			return nil, fmt.Errorf("hatDataStructure: tuple field %q: %w", field.Name, err)
		}
		values[index] = value
	}
	return values, nil
}

// UnpackVersioned requires the versioned envelope to carry the source version
// used to construct this reader before applying the compatibility rules.
func (reader TupleFormatReader) UnpackVersioned(tuple VersionedTuple) ([]TupleFieldValue, error) {
	if tuple.version == 0 {
		return nil, ErrVersionedTupleInvalid
	}
	if err := reader.validateDefinition(); err != nil {
		return nil, err
	}
	if tuple.version != reader.source.version {
		return nil, fmt.Errorf("%w: expected %d, got %d", ErrVersionedTupleVersionMismatch, reader.source.version, tuple.version)
	}
	return reader.Unpack(tuple.tuple)
}

func (reader TupleFormatReader) validateDefinition() error {
	if err := reader.source.validateDefinition(); err != nil {
		return err
	}
	return reader.target.validateDefinition()
}
