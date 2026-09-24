package hatSql

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

const (
	sqlDataflowPlanBinaryMagic        = "HDP1"
	maxSQLDataflowPlanBinaryBytes     = 16 << 20
	maxSQLDataflowPlanBinaryFragments = 1_000_000
	maxSQLDataflowPlanBinaryString    = 1 << 20
)

var (
	// ErrSQLDataflowPlanBinaryInvalid indicates malformed or unsafe binary
	// dataflow-plan bytes.
	ErrSQLDataflowPlanBinaryInvalid = errors.New("hatSql: invalid binary SQL dataflow plan")
	// ErrSQLDataflowPlanJSONInvalid indicates malformed or invalid JSON plan
	// data. JSON remains available as an explicit compatibility fallback.
	ErrSQLDataflowPlanJSONInvalid = errors.New("hatSql: invalid JSON SQL dataflow plan")
)

// EncodeSQLDataflowPlan encodes a validated logical plan in the compact
// versioned binary format used for plan transfer and storage.
func EncodeSQLDataflowPlan(plan SQLDataflowPlan) ([]byte, error) {
	if err := validateSQLDataflowPlan(plan); err != nil {
		return nil, err
	}
	if len(plan.Source) > maxSQLDataflowPlanBinaryString {
		return nil, fmt.Errorf("source length %d exceeds %d: %w", len(plan.Source), maxSQLDataflowPlanBinaryString, ErrSQLDataflowPlanBinaryInvalid)
	}
	if len(plan.Fragments) > maxSQLDataflowPlanBinaryFragments {
		return nil, fmt.Errorf("fragment count %d exceeds %d: %w", len(plan.Fragments), maxSQLDataflowPlanBinaryFragments, ErrSQLDataflowPlanBinaryInvalid)
	}
	for _, fragment := range plan.Fragments {
		if len(fragment.Kind) > maxSQLDataflowPlanBinaryString || len(fragment.Detail) > maxSQLDataflowPlanBinaryString {
			return nil, fmt.Errorf("fragment %d string exceeds %d: %w", fragment.ID, maxSQLDataflowPlanBinaryString, ErrSQLDataflowPlanBinaryInvalid)
		}
	}
	capacity, ok := sqlDataflowPlanBinaryCapacity(plan)
	if !ok || capacity > maxSQLDataflowPlanBinaryBytes {
		return nil, fmt.Errorf("encoded plan exceeds %d: %w", maxSQLDataflowPlanBinaryBytes, ErrSQLDataflowPlanBinaryInvalid)
	}
	encoded := make([]byte, 0, capacity)
	encoded = append(encoded, sqlDataflowPlanBinaryMagic...)
	encoded = appendSQLDataflowPlanUvarint(encoded, uint64(len(plan.Source)))
	encoded = append(encoded, plan.Source...)
	encoded = appendSQLDataflowPlanUvarint(encoded, uint64(plan.Root+1))
	encoded = appendSQLDataflowPlanUvarint(encoded, uint64(len(plan.Fragments)))
	for _, fragment := range plan.Fragments {
		encoded = appendSQLDataflowPlanUvarint(encoded, uint64(fragment.ID))
		encoded = appendSQLDataflowPlanString(encoded, fragment.Kind)
		encoded = appendSQLDataflowPlanString(encoded, fragment.Detail)
		encoded = appendSQLDataflowPlanUvarint(encoded, uint64(len(fragment.Inputs)))
		for _, input := range fragment.Inputs {
			encoded = appendSQLDataflowPlanUvarint(encoded, uint64(input))
		}
	}
	if len(encoded) > maxSQLDataflowPlanBinaryBytes {
		return nil, fmt.Errorf("encoded plan length %d exceeds %d: %w", len(encoded), maxSQLDataflowPlanBinaryBytes, ErrSQLDataflowPlanBinaryInvalid)
	}
	return encoded, nil
}

// EncodeSQLDataflowPlanJSON encodes a validated plan using the legacy JSON
// representation. It is the compatibility fallback for callers that need a
// text format or an older JSON-only transport.
func EncodeSQLDataflowPlanJSON(plan SQLDataflowPlan) ([]byte, error) {
	if err := validateSQLDataflowPlan(plan); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return nil, fmt.Errorf("encode SQL dataflow plan JSON: %w", err)
	}
	if len(encoded) > maxSQLDataflowPlanBinaryBytes {
		return nil, fmt.Errorf("encoded JSON plan length %d exceeds %d: %w", len(encoded), maxSQLDataflowPlanBinaryBytes, ErrSQLDataflowPlanJSONInvalid)
	}
	return encoded, nil
}

// DecodeSQLDataflowPlan decodes the compact binary format by default and
// accepts the legacy JSON format when the payload is a JSON object.
func DecodeSQLDataflowPlan(encoded []byte) (SQLDataflowPlan, error) {
	if len(encoded) == 0 || len(encoded) > maxSQLDataflowPlanBinaryBytes {
		return SQLDataflowPlan{}, fmt.Errorf("payload length %d: %w", len(encoded), ErrSQLDataflowPlanBinaryInvalid)
	}
	trimmed := bytes.TrimSpace(encoded)
	if bytes.HasPrefix(trimmed, []byte(sqlDataflowPlanBinaryMagic)) {
		return decodeSQLDataflowPlanBinary(trimmed)
	}
	if len(trimmed) > 0 && trimmed[0] == '{' {
		return DecodeSQLDataflowPlanJSON(trimmed)
	}
	return SQLDataflowPlan{}, fmt.Errorf("unknown plan encoding: %w", ErrSQLDataflowPlanBinaryInvalid)
}

// DecodeSQLDataflowPlanJSON decodes and validates the legacy JSON format.
func DecodeSQLDataflowPlanJSON(encoded []byte) (SQLDataflowPlan, error) {
	if len(encoded) == 0 || len(encoded) > maxSQLDataflowPlanBinaryBytes {
		return SQLDataflowPlan{}, fmt.Errorf("payload length %d: %w", len(encoded), ErrSQLDataflowPlanJSONInvalid)
	}
	var plan SQLDataflowPlan
	if err := json.Unmarshal(encoded, &plan); err != nil {
		return SQLDataflowPlan{}, fmt.Errorf("decode SQL dataflow plan JSON: %w: %v", ErrSQLDataflowPlanJSONInvalid, err)
	}
	if err := validateSQLDataflowPlan(plan); err != nil {
		return SQLDataflowPlan{}, fmt.Errorf("decode SQL dataflow plan JSON: %w: %v", ErrSQLDataflowPlanJSONInvalid, err)
	}
	return plan, nil
}

// MarshalBinary implements encoding.BinaryMarshaler for the compact plan
// format.
func (plan SQLDataflowPlan) MarshalBinary() ([]byte, error) {
	return EncodeSQLDataflowPlan(plan)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler. The receiver is
// changed only after the complete payload has been validated.
func (plan *SQLDataflowPlan) UnmarshalBinary(encoded []byte) error {
	if plan == nil {
		return fmt.Errorf("nil plan receiver: %w", ErrSQLDataflowPlanBinaryInvalid)
	}
	decoded, err := decodeSQLDataflowPlanBinary(encoded)
	if err != nil {
		return err
	}
	*plan = decoded
	return nil
}

func decodeSQLDataflowPlanBinary(encoded []byte) (SQLDataflowPlan, error) {
	if len(encoded) < len(sqlDataflowPlanBinaryMagic) || len(encoded) > maxSQLDataflowPlanBinaryBytes || !bytes.HasPrefix(encoded, []byte(sqlDataflowPlanBinaryMagic)) {
		return SQLDataflowPlan{}, fmt.Errorf("header: %w", ErrSQLDataflowPlanBinaryInvalid)
	}
	reader := sqlDataflowPlanBinaryReader{
		data:    encoded[len(sqlDataflowPlanBinaryMagic):],
		strings: make([]string, 0, 8),
	}
	source, err := reader.readString("source")
	if err != nil {
		return SQLDataflowPlan{}, err
	}
	rootValue, err := reader.readUvarint("root")
	if err != nil {
		return SQLDataflowPlan{}, err
	}
	if rootValue > uint64(^uint(0)>>1)+1 {
		return SQLDataflowPlan{}, reader.invalid("root is too large")
	}
	root := -1
	if rootValue > 0 {
		root = int(rootValue - 1)
	}
	fragmentCount, err := reader.readCount("fragment count", maxSQLDataflowPlanBinaryFragments)
	if err != nil {
		return SQLDataflowPlan{}, err
	}
	plan := SQLDataflowPlan{
		Format:    sqlDataflowPlanFormat,
		Source:    source,
		Fragments: make([]SQLDataflowFragment, fragmentCount),
		Root:      root,
	}
	for index := range plan.Fragments {
		id, err := reader.readCount("fragment id", maxSQLDataflowPlanBinaryFragments)
		if err != nil {
			return SQLDataflowPlan{}, err
		}
		kind, err := reader.readString("fragment kind")
		if err != nil {
			return SQLDataflowPlan{}, err
		}
		detail, err := reader.readString("fragment detail")
		if err != nil {
			return SQLDataflowPlan{}, err
		}
		inputCount, err := reader.readCount("fragment input count", maxSQLDataflowPlanBinaryFragments)
		if err != nil {
			return SQLDataflowPlan{}, err
		}
		var inputs []int
		if inputCount > 0 {
			inputs = make([]int, inputCount)
			for inputIndex := range inputs {
				input, err := reader.readCount("fragment input", maxSQLDataflowPlanBinaryFragments)
				if err != nil {
					return SQLDataflowPlan{}, err
				}
				inputs[inputIndex] = input
			}
		}
		plan.Fragments[index] = SQLDataflowFragment{ID: id, Kind: kind, Detail: detail, Inputs: inputs}
	}
	if reader.remaining() != 0 {
		return SQLDataflowPlan{}, reader.invalid("trailing bytes")
	}
	if err := validateSQLDataflowPlan(plan); err != nil {
		return SQLDataflowPlan{}, fmt.Errorf("decoded plan: %w", err)
	}
	return plan, nil
}

func appendSQLDataflowPlanUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func appendSQLDataflowPlanString(dst []byte, value string) []byte {
	dst = appendSQLDataflowPlanUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func sqlDataflowPlanBinaryCapacity(plan SQLDataflowPlan) (int, bool) {
	capacity := len(sqlDataflowPlanBinaryMagic)
	add := func(value int) bool {
		if value < 0 || capacity > maxSQLDataflowPlanBinaryBytes-value {
			return false
		}
		capacity += value
		return true
	}
	if !add(sqlDataflowPlanUvarintLength(uint64(len(plan.Source)))+len(plan.Source)) ||
		!add(sqlDataflowPlanUvarintLength(uint64(plan.Root+1))) ||
		!add(sqlDataflowPlanUvarintLength(uint64(len(plan.Fragments)))) {
		return 0, false
	}
	for _, fragment := range plan.Fragments {
		if !add(sqlDataflowPlanUvarintLength(uint64(fragment.ID))) ||
			!add(sqlDataflowPlanUvarintLength(uint64(len(fragment.Kind)))+len(fragment.Kind)) ||
			!add(sqlDataflowPlanUvarintLength(uint64(len(fragment.Detail)))+len(fragment.Detail)) ||
			!add(sqlDataflowPlanUvarintLength(uint64(len(fragment.Inputs)))) {
			return 0, false
		}
		for _, input := range fragment.Inputs {
			if !add(sqlDataflowPlanUvarintLength(uint64(input))) {
				return 0, false
			}
		}
	}
	return capacity, true
}

func sqlDataflowPlanUvarintLength(value uint64) int {
	length := 1
	for value >= 0x80 {
		value >>= 7
		length++
	}
	return length
}

type sqlDataflowPlanBinaryReader struct {
	data    []byte
	pos     int
	strings []string
}

func (reader *sqlDataflowPlanBinaryReader) readUvarint(field string) (uint64, error) {
	if reader == nil || reader.pos >= len(reader.data) {
		return 0, reader.invalid(field + " is truncated")
	}
	value, size := binary.Uvarint(reader.data[reader.pos:])
	if size == 0 {
		return 0, reader.invalid(field + " is truncated")
	}
	if size < 0 {
		return 0, reader.invalid(field + " overflows uint64")
	}
	reader.pos += size
	return value, nil
}

func (reader *sqlDataflowPlanBinaryReader) readCount(field string, maximum int) (int, error) {
	value, err := reader.readUvarint(field)
	if err != nil {
		return 0, err
	}
	if value > uint64(maximum) || value > uint64(^uint(0)>>1) {
		return 0, reader.invalid(fmt.Sprintf("%s %d exceeds %d", field, value, maximum))
	}
	return int(value), nil
}

func (reader *sqlDataflowPlanBinaryReader) readString(field string) (string, error) {
	length, err := reader.readCount(field+" length", maxSQLDataflowPlanBinaryString)
	if err != nil {
		return "", err
	}
	if length > len(reader.data)-reader.pos {
		return "", reader.invalid(field + " is truncated")
	}
	raw := reader.data[reader.pos : reader.pos+length]
	reader.pos += length
	for _, existing := range reader.strings {
		if sqlDataflowPlanBytesEqualString(raw, existing) {
			return existing, nil
		}
	}
	value := string(raw)
	reader.strings = append(reader.strings, value)
	return value, nil
}

func sqlDataflowPlanBytesEqualString(raw []byte, value string) bool {
	if len(raw) != len(value) {
		return false
	}
	for index, item := range raw {
		if item != value[index] {
			return false
		}
	}
	return true
}

func (reader *sqlDataflowPlanBinaryReader) remaining() int {
	if reader == nil {
		return 0
	}
	return len(reader.data) - reader.pos
}

func (reader *sqlDataflowPlanBinaryReader) invalid(detail string) error {
	return fmt.Errorf("%s: %w", detail, ErrSQLDataflowPlanBinaryInvalid)
}
