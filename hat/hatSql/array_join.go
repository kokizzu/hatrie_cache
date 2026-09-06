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
