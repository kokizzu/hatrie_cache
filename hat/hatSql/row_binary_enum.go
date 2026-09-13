package hatSql

import "fmt"

// SQLEnum8 is the compact code for a RowBinary Enum8 value. The code indexes
// the labels in the owning SQLRowBinaryColumn from zero.
type SQLEnum8 uint8

// SQLEnum16 is the compact code for a RowBinary Enum16 value. The code
// indexes the labels in the owning SQLRowBinaryColumn from zero.
type SQLEnum16 uint16

const (
	maxSQLRowBinaryEnum8Values  = 1 << 8
	maxSQLRowBinaryEnum16Values = 1 << 16
)

// Label resolves an Enum8 code using the labels from its schema column.
func (value SQLEnum8) Label(labels []string) (string, bool) {
	if int(value) >= len(labels) {
		return "", false
	}
	return labels[value], true
}

// Label resolves an Enum16 code using the labels from its schema column.
func (value SQLEnum16) Label(labels []string) (string, bool) {
	if int(value) >= len(labels) {
		return "", false
	}
	return labels[value], true
}

func validateSQLRowBinaryEnumColumn(column SQLRowBinaryColumn) error {
	if column.Type != SQLRowBinaryEnum8 && column.Type != SQLRowBinaryEnum16 {
		if len(column.EnumValues) != 0 {
			return fmt.Errorf("RowBinary column %q has enum labels but type %d is not an enum", column.Name, column.Type)
		}
		return nil
	}
	if len(column.EnumValues) == 0 {
		return fmt.Errorf("RowBinary enum column %q requires at least one label", column.Name)
	}
	maxValues := maxSQLRowBinaryEnum16Values
	if column.Type == SQLRowBinaryEnum8 {
		maxValues = maxSQLRowBinaryEnum8Values
	}
	if len(column.EnumValues) > maxValues {
		return fmt.Errorf("RowBinary enum column %q has %d labels, maximum is %d", column.Name, len(column.EnumValues), maxValues)
	}
	seen := make(map[string]struct{}, len(column.EnumValues))
	for _, label := range column.EnumValues {
		if label == "" {
			return fmt.Errorf("RowBinary enum column %q has an empty label", column.Name)
		}
		if _, exists := seen[label]; exists {
			return fmt.Errorf("RowBinary enum column %q has duplicate label %q", column.Name, label)
		}
		seen[label] = struct{}{}
	}
	return nil
}

func sqlRowBinaryEnumCode(column SQLRowBinaryColumn, value interface{}, row int) (uint64, error) {
	var code uint64
	typedCode := false
	switch converted := value.(type) {
	case SQLEnum8:
		code = uint64(converted)
		typedCode = true
	case SQLEnum16:
		code = uint64(converted)
		typedCode = true
	case string:
		for index, label := range column.EnumValues {
			if converted == label {
				code = uint64(index)
				goto checked
			}
		}
		return 0, fmt.Errorf("RowBinary row %d column %q has unknown enum label %q", row, column.Name, converted)
	case uint:
		code = uint64(converted)
	case uint8:
		code = uint64(converted)
	case uint16:
		code = uint64(converted)
	case uint32:
		code = uint64(converted)
	case uint64:
		code = converted
	case int:
		if converted < 0 {
			return 0, fmt.Errorf("RowBinary row %d column %q has negative enum code %d", row, column.Name, converted)
		}
		code = uint64(converted)
	case int8:
		if converted < 0 {
			return 0, fmt.Errorf("RowBinary row %d column %q has negative enum code %d", row, column.Name, converted)
		}
		code = uint64(converted)
	case int16:
		if converted < 0 {
			return 0, fmt.Errorf("RowBinary row %d column %q has negative enum code %d", row, column.Name, converted)
		}
		code = uint64(converted)
	case int32:
		if converted < 0 {
			return 0, fmt.Errorf("RowBinary row %d column %q has negative enum code %d", row, column.Name, converted)
		}
		code = uint64(converted)
	case int64:
		if converted < 0 {
			return 0, fmt.Errorf("RowBinary row %d column %q has negative enum code %d", row, column.Name, converted)
		}
		code = uint64(converted)
	default:
		return 0, fmt.Errorf("RowBinary row %d column %q expects enum label or code, got %T", row, column.Name, value)
	}

checked:
	if len(column.EnumValues) == 0 {
		if !typedCode {
			return 0, fmt.Errorf("RowBinary row %d column %q requires enum labels for an untyped code", row, column.Name)
		}
	} else if code >= uint64(len(column.EnumValues)) {
		return 0, fmt.Errorf("RowBinary row %d column %q enum code %d exceeds %d labels", row, column.Name, code, len(column.EnumValues))
	}
	if column.Type == SQLRowBinaryEnum8 && code >= maxSQLRowBinaryEnum8Values {
		return 0, fmt.Errorf("RowBinary row %d column %q enum8 code %d is out of range", row, column.Name, code)
	}
	return code, nil
}

func validateSQLRowBinaryEnumDecodedValue(column SQLRowBinaryColumn, value interface{}, row int) error {
	if column.Type != SQLRowBinaryEnum8 && column.Type != SQLRowBinaryEnum16 {
		return nil
	}
	_, err := sqlRowBinaryEnumCode(column, value, row)
	return err
}
