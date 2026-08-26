package hatCache

import (
	"io"
	"math"

	json "github.com/goccy/go-json"
)

func flatJSONScalarMap(values Map) bool {
	for _, value := range values {
		if !flatJSONScalar(value) {
			return false
		}
	}
	return true
}

func flatJSONScalarSlice(values Slice) bool {
	for _, value := range values {
		if !flatJSONScalar(value) {
			return false
		}
	}
	return true
}

func flatJSONScalar(value interface{}) bool {
	switch typed := value.(type) {
	case nil, bool, string, []byte,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, uintptr:
		return true
	case float32:
		return !math.IsNaN(float64(typed)) && !math.IsInf(float64(typed), 0)
	case float64:
		return !math.IsNaN(typed) && !math.IsInf(typed, 0)
	default:
		return false
	}
}

func validateJSONToDiscard(value interface{}) error {
	return json.NewEncoder(io.Discard).Encode(value)
}
