package hatSql

import (
	"fmt"
	"math/big"
	"strings"
)

// SQLDecimal128 is a signed two's-complement 128-bit decimal coefficient.
// Its scale is supplied by the owning SQLRowBinaryColumn.
type SQLDecimal128 [16]byte

// SQLDecimal256 is a signed two's-complement 256-bit decimal coefficient.
// Its scale is supplied by the owning SQLRowBinaryColumn.
type SQLDecimal256 [32]byte

const (
	maxSQLDecimal128Precision uint8 = 38
	maxSQLDecimal256Precision uint8 = 76
	maxSQLDecimalInputBytes         = 128
)

var sqlDecimalPower10Table = buildSQLDecimalPower10Table()

func buildSQLDecimalPower10Table() [77][32]byte {
	var table [77][32]byte
	value := big.NewInt(1)
	for precision := range table {
		encoded := value.Bytes()
		for index := range encoded {
			table[precision][index] = encoded[len(encoded)-index-1]
		}
		value.Mul(value, big.NewInt(10))
	}
	return table
}

// ParseSQLDecimal128 parses an exact decimal string into a scaled 128-bit
// coefficient. The input must have no more fractional digits than scale.
func ParseSQLDecimal128(value string, scale uint8) (SQLDecimal128, error) {
	coefficient, err := parseSQLDecimalFixed(value, scale, 128)
	if err != nil {
		return SQLDecimal128{}, err
	}
	var result SQLDecimal128
	putSQLDecimalLittleEndian(result[:], coefficient)
	return result, nil
}

// ParseSQLDecimal256 parses an exact decimal string into a scaled 256-bit
// coefficient. The input must have no more fractional digits than scale.
func ParseSQLDecimal256(value string, scale uint8) (SQLDecimal256, error) {
	coefficient, err := parseSQLDecimalFixed(value, scale, 256)
	if err != nil {
		return SQLDecimal256{}, err
	}
	var result SQLDecimal256
	putSQLDecimalLittleEndian(result[:], coefficient)
	return result, nil
}

// Format renders a Decimal128 coefficient with the supplied decimal scale.
func (value SQLDecimal128) Format(scale uint8) (string, error) {
	return formatSQLDecimalFixed(value[:], scale, 128)
}

// Format renders a Decimal256 coefficient with the supplied decimal scale.
func (value SQLDecimal256) Format(scale uint8) (string, error) {
	return formatSQLDecimalFixed(value[:], scale, 256)
}

func parseSQLDecimalFixed(value string, scale uint8, bits int) (*big.Int, error) {
	maxPrecision := maxSQLDecimalPrecision(bits)
	if scale > maxPrecision {
		return nil, fmt.Errorf("decimal scale %d exceeds maximum precision %d", scale, maxPrecision)
	}
	if len(value) > maxSQLDecimalInputBytes {
		return nil, fmt.Errorf("decimal input exceeds maximum length %d bytes", maxSQLDecimalInputBytes)
	}
	parsed, ok := parseSQLDecimal(value)
	if !ok {
		return nil, fmt.Errorf("invalid decimal %q", value)
	}
	rational, ok := new(big.Rat).SetString(string(parsed))
	if !ok {
		return nil, fmt.Errorf("invalid decimal %q", value)
	}
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	numerator := new(big.Int).Mul(rational.Num(), factor)
	coefficient, remainder := new(big.Int), new(big.Int)
	coefficient.QuoRem(numerator, rational.Denom(), remainder)
	if remainder.Sign() != 0 {
		return nil, fmt.Errorf("decimal %q has more fractional precision than scale %d", value, scale)
	}
	minimum, maximum := sqlDecimalSignedRange(bits)
	if coefficient.Cmp(minimum) < 0 || coefficient.Cmp(maximum) > 0 {
		return nil, fmt.Errorf("decimal %q overflows signed Decimal%d", value, bits)
	}
	return coefficient, nil
}

func formatSQLDecimalFixed(encoded []byte, scale uint8, bits int) (string, error) {
	maxPrecision := maxSQLDecimalPrecision(bits)
	if scale > maxPrecision {
		return "", fmt.Errorf("decimal scale %d exceeds maximum precision %d", scale, maxPrecision)
	}
	coefficient := sqlDecimalBigInt(encoded, bits)
	negative := coefficient.Sign() < 0
	digits := new(big.Int).Abs(coefficient).String()
	if scale == 0 {
		if negative {
			return "-" + digits, nil
		}
		return digits, nil
	}
	if len(digits) <= int(scale) {
		digits = strings.Repeat("0", int(scale)+1-len(digits)) + digits
	}
	point := len(digits) - int(scale)
	formatted := digits[:point] + "." + digits[point:]
	if negative {
		return "-" + formatted, nil
	}
	return formatted, nil
}

func maxSQLDecimalPrecision(bits int) uint8 {
	if bits == 128 {
		return maxSQLDecimal128Precision
	}
	return maxSQLDecimal256Precision
}

func sqlDecimalSignedRange(bits int) (*big.Int, *big.Int) {
	half := new(big.Int).Lsh(big.NewInt(1), uint(bits-1))
	minimum := new(big.Int).Neg(new(big.Int).Set(half))
	maximum := new(big.Int).Sub(half, big.NewInt(1))
	return minimum, maximum
}

func putSQLDecimalLittleEndian(destination []byte, coefficient *big.Int) {
	encoded := new(big.Int).Set(coefficient)
	if coefficient.Sign() < 0 {
		modulus := new(big.Int).Lsh(big.NewInt(1), uint(len(destination)*8))
		encoded.Add(modulus, encoded)
	}
	bytes := encoded.Bytes()
	for index := range bytes {
		destination[index] = bytes[len(bytes)-1-index]
	}
}

func sqlDecimalBigInt(encoded []byte, bits int) *big.Int {
	bigEndian := make([]byte, len(encoded))
	for index := range encoded {
		bigEndian[index] = encoded[len(encoded)-1-index]
	}
	unsigned := new(big.Int).SetBytes(bigEndian)
	if len(encoded) != 0 && encoded[len(encoded)-1]&0x80 != 0 {
		modulus := new(big.Int).Lsh(big.NewInt(1), uint(bits))
		unsigned.Sub(unsigned, modulus)
	}
	return unsigned
}

func validateSQLRowBinaryDecimalColumn(column SQLRowBinaryColumn) error {
	isDecimal := column.Type == SQLRowBinaryDecimal128 || column.Type == SQLRowBinaryDecimal256
	if !isDecimal {
		if column.DecimalScale != 0 || column.DecimalPrecision != 0 {
			return fmt.Errorf("RowBinary column %q has decimal metadata but type %d is not decimal", column.Name, column.Type)
		}
		return nil
	}
	maxPrecision := maxSQLDecimalPrecision(128)
	if column.Type == SQLRowBinaryDecimal256 {
		maxPrecision = maxSQLDecimalPrecision(256)
	}
	if column.DecimalScale > maxPrecision {
		return fmt.Errorf("RowBinary decimal column %q scale %d exceeds maximum precision %d", column.Name, column.DecimalScale, maxPrecision)
	}
	if column.DecimalPrecision != 0 && column.DecimalPrecision > maxPrecision {
		return fmt.Errorf("RowBinary decimal column %q precision %d exceeds maximum precision %d", column.Name, column.DecimalPrecision, maxPrecision)
	}
	if column.DecimalPrecision != 0 && column.DecimalScale > column.DecimalPrecision {
		return fmt.Errorf("RowBinary decimal column %q scale %d exceeds precision %d", column.Name, column.DecimalScale, column.DecimalPrecision)
	}
	return nil
}

func sqlRowBinaryDecimal128Value(column SQLRowBinaryColumn, value interface{}, row int) (SQLDecimal128, error) {
	var converted SQLDecimal128
	switch value := value.(type) {
	case SQLDecimal128:
		converted = value
	case string:
		parsed, err := ParseSQLDecimal128(value, column.DecimalScale)
		if err != nil {
			return SQLDecimal128{}, fmt.Errorf("RowBinary row %d column %q: %w", row, column.Name, err)
		}
		converted = parsed
	case SQLDecimal:
		parsed, err := ParseSQLDecimal128(string(value), column.DecimalScale)
		if err != nil {
			return SQLDecimal128{}, fmt.Errorf("RowBinary row %d column %q: %w", row, column.Name, err)
		}
		converted = parsed
	default:
		return SQLDecimal128{}, fmt.Errorf("RowBinary row %d column %q expects SQLDecimal128 or decimal string, got %T", row, column.Name, value)
	}
	if err := validateSQLDecimal128Value(column, converted, row); err != nil {
		return SQLDecimal128{}, err
	}
	return converted, nil
}

func sqlRowBinaryDecimal256Value(column SQLRowBinaryColumn, value interface{}, row int) (SQLDecimal256, error) {
	var converted SQLDecimal256
	switch value := value.(type) {
	case SQLDecimal256:
		converted = value
	case string:
		parsed, err := ParseSQLDecimal256(value, column.DecimalScale)
		if err != nil {
			return SQLDecimal256{}, fmt.Errorf("RowBinary row %d column %q: %w", row, column.Name, err)
		}
		converted = parsed
	case SQLDecimal:
		parsed, err := ParseSQLDecimal256(string(value), column.DecimalScale)
		if err != nil {
			return SQLDecimal256{}, fmt.Errorf("RowBinary row %d column %q: %w", row, column.Name, err)
		}
		converted = parsed
	default:
		return SQLDecimal256{}, fmt.Errorf("RowBinary row %d column %q expects SQLDecimal256 or decimal string, got %T", row, column.Name, value)
	}
	if err := validateSQLDecimal256Value(column, converted, row); err != nil {
		return SQLDecimal256{}, err
	}
	return converted, nil
}

func normalizeSQLRowBinaryDecimalValue(column SQLRowBinaryColumn, value interface{}, row int) (interface{}, error) {
	switch column.Type {
	case SQLRowBinaryDecimal128:
		return sqlRowBinaryDecimal128Value(column, value, row)
	case SQLRowBinaryDecimal256:
		return sqlRowBinaryDecimal256Value(column, value, row)
	default:
		return nil, fmt.Errorf("RowBinary column %q has unsupported decimal type %d", column.Name, column.Type)
	}
}

func validateSQLRowBinaryDecimalValue(column SQLRowBinaryColumn, value interface{}, row int) error {
	switch column.Type {
	case SQLRowBinaryDecimal128:
		converted, ok := value.(SQLDecimal128)
		if !ok {
			return fmt.Errorf("RowBinary row %d column %q expects SQLDecimal128, got %T", row, column.Name, value)
		}
		return validateSQLDecimal128Value(column, converted, row)
	case SQLRowBinaryDecimal256:
		converted, ok := value.(SQLDecimal256)
		if !ok {
			return fmt.Errorf("RowBinary row %d column %q expects SQLDecimal256, got %T", row, column.Name, value)
		}
		return validateSQLDecimal256Value(column, converted, row)
	}
	return nil
}

func validateSQLDecimal128Value(column SQLRowBinaryColumn, value SQLDecimal128, row int) error {
	if column.DecimalPrecision != 0 && sqlDecimalFixedMagnitudeExceedsPrecision(value[:], column.DecimalPrecision) {
		return fmt.Errorf("RowBinary row %d column %q coefficient exceeds precision %d", row, column.Name, column.DecimalPrecision)
	}
	return nil
}

func validateSQLDecimal256Value(column SQLRowBinaryColumn, value SQLDecimal256, row int) error {
	if column.DecimalPrecision != 0 && sqlDecimalFixedMagnitudeExceedsPrecision(value[:], column.DecimalPrecision) {
		return fmt.Errorf("RowBinary row %d column %q coefficient exceeds precision %d", row, column.Name, column.DecimalPrecision)
	}
	return nil
}

func sqlDecimalFixedMagnitudeExceedsPrecision(encoded []byte, precision uint8) bool {
	if int(precision) >= len(sqlDecimalPower10Table) {
		return true
	}
	var magnitude [32]byte
	negative := encoded[len(encoded)-1]&0x80 != 0
	if negative {
		carry := byte(1)
		for index, value := range encoded {
			value = ^value
			if carry != 0 {
				value++
				if value != 0 {
					carry = 0
				}
			}
			magnitude[index] = value
		}
	} else {
		copy(magnitude[:], encoded)
	}
	limit := sqlDecimalPower10Table[precision]
	for index := len(encoded) - 1; index >= 0; index-- {
		if magnitude[index] != limit[index] {
			return magnitude[index] > limit[index]
		}
	}
	return true
}

func appendSQLDecimal128(destination []byte, value SQLDecimal128) []byte {
	return append(destination, value[:]...)
}

func appendSQLDecimal256(destination []byte, value SQLDecimal256) []byte {
	return append(destination, value[:]...)
}

func compareSQLDecimalFixed(left, right []byte) int {
	leftNegative := len(left) != 0 && left[len(left)-1]&0x80 != 0
	rightNegative := len(right) != 0 && right[len(right)-1]&0x80 != 0
	if leftNegative != rightNegative {
		if leftNegative {
			return -1
		}
		return 1
	}
	for index := len(left) - 1; index >= 0; index-- {
		if left[index] < right[index] {
			return -1
		}
		if left[index] > right[index] {
			return 1
		}
	}
	return 0
}
