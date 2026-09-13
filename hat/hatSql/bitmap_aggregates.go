package hatSql

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	"hatrie_cache/hat/hatDataStructure"
)

// SQLBitmap is the typed, compressed bitmap value produced by BITMAP_AGG.
// JSON encoding exposes sorted uint32 members for clients that do not know the
// in-process representation.
type SQLBitmap struct {
	bitmap hatDataStructure.RoaringBitmap
}

// NewSQLBitmap returns an empty bitmap suitable for programmatic SQL values.
func NewSQLBitmap() SQLBitmap {
	return SQLBitmap{bitmap: hatDataStructure.NewRoaringBitmap()}
}

// Add inserts one or more uint32 members and returns the number of new
// members.
func (bitmap *SQLBitmap) Add(values ...uint32) int {
	if len(values) == 0 {
		return 0
	}
	return bitmap.bitmap.Add(values[0], values[1:]...)
}

// Contains reports whether value is present.
func (bitmap SQLBitmap) Contains(value uint32) bool {
	return bitmap.bitmap.Contains(value)
}

// Count returns the number of distinct members.
func (bitmap SQLBitmap) Count() uint64 {
	return bitmap.bitmap.Count()
}

// Values returns sorted members. The returned slice is independent of the
// bitmap and may be modified by the caller.
func (bitmap SQLBitmap) Values() []uint32 {
	return bitmap.bitmap.Values()
}

// EncodedSize returns the compact Roaring payload size in bytes.
func (bitmap SQLBitmap) EncodedSize() int64 {
	return bitmap.bitmap.EncodedSize()
}

// MarshalJSON makes SQLBitmap usable with the existing JSON result paths.
func (bitmap SQLBitmap) MarshalJSON() ([]byte, error) {
	values := bitmap.Values()
	if values == nil {
		values = []uint32{}
	}
	return json.Marshal(values)
}

func sqlAggregateBitmapValues(expr sqlExpr, group []sqlExecRow) (SQLBitmap, error) {
	if len(expr.args) != 1 {
		return SQLBitmap{}, fmt.Errorf("%s expects exactly one argument", expr.name)
	}
	rows, err := sqlAggregateFilterRows(expr, group)
	if err != nil {
		return SQLBitmap{}, err
	}
	bitmap := NewSQLBitmap()
	for _, row := range rows {
		value := evalSQLExpr(expr.args[0], []sqlExecRow{row}, row)
		if err := sqlExpressionError(value); err != nil {
			return SQLBitmap{}, err
		}
		if value == nil {
			continue
		}
		member, err := sqlBitmapMember(value)
		if err != nil {
			return SQLBitmap{}, err
		}
		bitmap.Add(member)
	}
	return bitmap, nil
}

func evalSQLBitmapFunction(expr sqlExpr, group []sqlExecRow, row sqlExecRow) (interface{}, error) {
	switch expr.name {
	case "BITMAP_COUNT":
		if len(expr.args) != 1 {
			return nil, fmt.Errorf("%s expects exactly one argument", expr.name)
		}
		bitmap, null, err := evalSQLBitmapArgument(expr.args[0], group, row)
		if err != nil {
			return nil, err
		}
		if null {
			return nil, nil
		}
		return int64(bitmap.Count()), nil
	case "BITMAP_CONTAINS":
		if len(expr.args) != 2 {
			return nil, fmt.Errorf("%s expects exactly two arguments", expr.name)
		}
		bitmap, null, err := evalSQLBitmapArgument(expr.args[0], group, row)
		if err != nil {
			return nil, err
		}
		if null {
			return nil, nil
		}
		value := evalSQLExpr(expr.args[1], group, row)
		if err := sqlExpressionError(value); err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		member, err := sqlBitmapMember(value)
		if err != nil {
			return nil, err
		}
		return bitmap.Contains(member), nil
	case "BITMAP_OR", "BITMAP_AND", "BITMAP_XOR":
		if len(expr.args) != 2 {
			return nil, fmt.Errorf("%s expects exactly two arguments", expr.name)
		}
		left, leftNull, err := evalSQLBitmapArgument(expr.args[0], group, row)
		if err != nil {
			return nil, err
		}
		right, rightNull, err := evalSQLBitmapArgument(expr.args[1], group, row)
		if err != nil {
			return nil, err
		}
		if leftNull || rightNull {
			return nil, nil
		}
		return sqlCombineBitmaps(expr.name, left, right), nil
	default:
		return nil, fmt.Errorf("unsupported bitmap function %s", expr.name)
	}
}

func evalSQLBitmapArgument(expr sqlExpr, group []sqlExecRow, row sqlExecRow) (SQLBitmap, bool, error) {
	value := evalSQLExpr(expr, group, row)
	if err := sqlExpressionError(value); err != nil {
		return SQLBitmap{}, false, err
	}
	if value == nil {
		return SQLBitmap{}, true, nil
	}
	switch bitmap := value.(type) {
	case SQLBitmap:
		return bitmap, false, nil
	case *SQLBitmap:
		if bitmap == nil {
			return SQLBitmap{}, true, nil
		}
		return *bitmap, false, nil
	case hatDataStructure.RoaringBitmap:
		return SQLBitmap{bitmap: bitmap}, false, nil
	case *hatDataStructure.RoaringBitmap:
		if bitmap == nil {
			return SQLBitmap{}, true, nil
		}
		return SQLBitmap{bitmap: *bitmap}, false, nil
	default:
		return SQLBitmap{}, false, fmt.Errorf("expected bitmap value, got %T", value)
	}
}

func sqlCombineBitmaps(operation string, left, right SQLBitmap) SQLBitmap {
	leftValues := left.Values()
	rightValues := right.Values()
	result := NewSQLBitmap()
	switch operation {
	case "BITMAP_OR":
		result.Add(leftValues...)
		result.Add(rightValues...)
	case "BITMAP_AND":
		membership := right
		if len(leftValues) > len(rightValues) {
			leftValues, rightValues = rightValues, leftValues
			membership = left
		}
		for _, value := range leftValues {
			if membership.Contains(value) {
				result.Add(value)
			}
		}
	case "BITMAP_XOR":
		for _, value := range leftValues {
			if !right.Contains(value) {
				result.Add(value)
			}
		}
		for _, value := range rightValues {
			if !left.Contains(value) {
				result.Add(value)
			}
		}
	}
	return result
}

func sqlBitmapMember(value interface{}) (uint32, error) {
	const maxMember = uint64(^uint32(0))
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		member := reflected.Int()
		if member < 0 || uint64(member) > maxMember {
			return 0, fmt.Errorf("bitmap member %v is outside uint32 range", value)
		}
		return uint32(member), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		member := reflected.Uint()
		if member > maxMember {
			return 0, fmt.Errorf("bitmap member %v is outside uint32 range", value)
		}
		return uint32(member), nil
	case reflect.Float32, reflect.Float64:
		member := reflected.Float()
		if math.IsNaN(member) || math.IsInf(member, 0) || member < 0 || member != math.Trunc(member) || member > float64(maxMember) {
			return 0, fmt.Errorf("bitmap member %v is not an integer in uint32 range", value)
		}
		return uint32(member), nil
	default:
		return 0, fmt.Errorf("bitmap member must be numeric, got %T", value)
	}
}
