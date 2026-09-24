package hatSql

import "reflect"

func sqlArrayJoinElements(value interface{}) (reflect.Value, bool) {
	if value == nil {
		return reflect.Value{}, true
	}
	reflected := reflect.ValueOf(value)
	if reflected.Kind() != reflect.Array && reflected.Kind() != reflect.Slice {
		return reflect.Value{}, false
	}
	return reflected, true
}

func sqlArrayJoinNestedField(value interface{}, field string) (interface{}, bool) {
	if value == nil {
		return nil, false
	}
	if row, ok := value.(SQLRow); ok {
		result, found := row[field]
		return result, found
	}
	reflected := reflect.ValueOf(value)
	if reflected.Kind() != reflect.Map || reflected.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	key := reflect.ValueOf(field)
	if key.Type() != reflected.Type().Key() {
		key = key.Convert(reflected.Type().Key())
	}
	result := reflected.MapIndex(key)
	if !result.IsValid() {
		return nil, false
	}
	return result.Interface(), true
}
