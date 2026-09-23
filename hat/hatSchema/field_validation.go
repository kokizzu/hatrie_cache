package hatSchema

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"time"
)

// ValidateFieldValue validates one row value against a declared column. It is
// intentionally independent of a row or transaction so callers can reject a
// bad field before constructing a larger candidate dataset.
func ValidateFieldValue(column Column, value interface{}) error {
	if value == nil {
		if column.NotNull {
			return fmt.Errorf("hatSchema: column %q is NOT NULL", column.Name)
		}
		return nil
	}

	typeName := column.Type
	if !fieldValueMatchesType(typeName, value) {
		return fmt.Errorf("hatSchema: column %q expects %s, got %T", column.Name, typeName, value)
	}
	if typeName == TypeEnum8 || typeName == TypeEnum16 {
		text := stringValue(value)
		for _, allowed := range column.EnumValues {
			if text == allowed {
				return nil
			}
		}
		return fmt.Errorf("hatSchema: column %q enum value %q is not declared", column.Name, text)
	}
	return nil
}

func fieldValueMatchesType(typeName Type, value interface{}) bool {
	switch typeName {
	case TypeText:
		switch value.(type) {
		case string, []byte:
			return true
		default:
			reflected := reflect.ValueOf(value)
			return reflected.Kind() == reflect.String || isByteSlice(reflected)
		}
	case TypeNumber:
		return isFiniteNumber(value)
	case TypeInteger:
		return isInteger(value)
	case TypeDecimal, TypeDecimal128, TypeDecimal256:
		if isFiniteNumber(value) {
			return true
		}
		return reflect.ValueOf(value).Kind() == reflect.String
	case TypeBoolean:
		_, ok := value.(bool)
		if ok {
			return true
		}
		return reflect.ValueOf(value).Kind() == reflect.Bool
	case TypeDate, TypeTimestamp:
		_, ok := value.(time.Time)
		return ok
	case TypeUUID, TypeIPv4, TypeIPv6:
		if _, ok := value.(string); ok {
			return true
		}
		return reflect.ValueOf(value).Kind() == reflect.String
	case TypeDuration:
		return isInteger(value)
	case TypeBinary:
		if _, ok := value.([]byte); ok {
			return true
		}
		return isByteSlice(reflect.ValueOf(value))
	case TypeJSON:
		return isJSONValue(value)
	case TypeEnum8, TypeEnum16:
		if _, ok := value.(string); ok {
			return true
		}
		return reflect.ValueOf(value).Kind() == reflect.String
	default:
		return false
	}
}

func isInteger(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, uintptr:
		return true
	}
	kind := reflect.ValueOf(value).Kind()
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return true
	default:
		return false
	}
}

func isFiniteNumber(value interface{}) bool {
	if isInteger(value) {
		return true
	}
	switch numeric := value.(type) {
	case float32:
		return !math.IsNaN(float64(numeric)) && !math.IsInf(float64(numeric), 0)
	case float64:
		return !math.IsNaN(numeric) && !math.IsInf(numeric, 0)
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Float32, reflect.Float64:
		floatValue := reflected.Float()
		return !math.IsNaN(floatValue) && !math.IsInf(floatValue, 0)
	default:
		return false
	}
}

func isByteSlice(value reflect.Value) bool {
	return value.Kind() == reflect.Slice && value.Type().Elem().Kind() == reflect.Uint8
}

func stringValue(value interface{}) string {
	if text, ok := value.(string); ok {
		return text
	}
	return reflect.ValueOf(value).String()
}

func isJSONValue(value interface{}) bool {
	if raw, ok := value.(json.RawMessage); ok {
		return json.Valid(raw)
	}
	if bytes, ok := value.([]byte); ok {
		return json.Valid(bytes)
	}
	_, err := json.Marshal(value)
	return err == nil
}
