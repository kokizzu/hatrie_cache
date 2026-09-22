package hatDataStructure

import (
	"encoding/json"
	"testing"
)

type c224BaselineArgMaxState struct {
	Argument int64 `json:"argument"`
	Value    int64 `json:"value"`
	HasValue bool  `json:"has_value"`
}

var c224BaselineArgMaxStateValue = c224BaselineArgMaxState{
	Argument: 42,
	Value:    1700000000,
	HasValue: true,
}

var c224BaselineArgMaxStateWire, _ = json.Marshal(c224BaselineArgMaxStateValue)

func BenchmarkC224ArgMaxJSONMarshal(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(c224BaselineArgMaxStateWire)))
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(c224BaselineArgMaxStateValue); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkC224ArgMaxJSONUnmarshal(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(c224BaselineArgMaxStateWire)))
	for i := 0; i < b.N; i++ {
		var state c224BaselineArgMaxState
		if err := json.Unmarshal(c224BaselineArgMaxStateWire, &state); err != nil {
			b.Fatal(err)
		}
	}
}
