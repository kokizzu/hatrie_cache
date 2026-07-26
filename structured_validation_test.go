package hatriecache

import (
	"errors"
	"math"
	"testing"

	json "github.com/goccy/go-json"
)

var structuredValidationTestSink error

type structuredValidationFailingMarshaler struct{}

func (structuredValidationFailingMarshaler) MarshalJSON() ([]byte, error) {
	return nil, errors.New("validation failed")
}

type structuredValidationTrackingMarshaler struct {
	calls *int
}

func (value structuredValidationTrackingMarshaler) MarshalJSON() ([]byte, error) {
	*value.calls++
	return []byte(`"tracked"`), nil
}

func TestFlatScalarStructuredValidationDoesNotAllocate(t *testing.T) {
	values := Map{
		"nil": nil, "bool": true, "string": "value", "bytes": []byte("bytes"),
		"int": int(-1), "int8": int8(-2), "int16": int16(-3), "int32": int32(-4), "int64": int64(-5),
		"uint": uint(1), "uint8": uint8(2), "uint16": uint16(3), "uint32": uint32(4), "uint64": uint64(5), "uintptr": uintptr(6),
		"float32": float32(1.25), "float64": float64(2.5),
	}
	for _, test := range []struct {
		name     string
		validate func() error
	}{
		{name: "map", validate: func() error { return validateMapValue(values) }},
		{name: "radix entries", validate: func() error { return validateRadixTreeEntries(values) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(1000, func() {
				structuredValidationTestSink = test.validate()
			})
			if structuredValidationTestSink != nil {
				t.Fatalf("validation error = %v", structuredValidationTestSink)
			}
			if allocs != 0 {
				t.Fatalf("flat scalar validation allocations = %.0f, want 0", allocs)
			}
		})
	}
}

func TestStructuredValidationMatchesJSONMarshalAcceptance(t *testing.T) {
	cycle := Map{}
	cycle["cycle"] = cycle
	for _, test := range []struct {
		name  string
		value interface{}
	}{
		{name: "nested valid", value: Map{"nested": Slice{"value", 1.5, true}}},
		{name: "not a number", value: math.NaN()},
		{name: "positive infinity", value: math.Inf(1)},
		{name: "negative infinity", value: math.Inf(-1)},
		{name: "invalid number", value: json.Number("not-a-number")},
		{name: "function", value: func() {}},
		{name: "channel", value: make(chan int)},
		{name: "failing marshaler", value: structuredValidationFailingMarshaler{}},
		{name: "cycle", value: cycle},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, marshalErr := json.Marshal(Map{"value": test.value})
			mapErr := validateMapValue(Map{"value": test.value})
			radixErr := validateRadixTreeEntries(Map{"value": test.value})
			if (mapErr == nil) != (marshalErr == nil) {
				t.Fatalf("map validation error = %v, marshal error = %v", mapErr, marshalErr)
			}
			if (radixErr == nil) != (marshalErr == nil) {
				t.Fatalf("radix validation error = %v, marshal error = %v", radixErr, marshalErr)
			}
		})
	}
}

func TestStructuredValidationInvokesCustomMarshalerFallback(t *testing.T) {
	calls := 0
	value := structuredValidationTrackingMarshaler{calls: &calls}
	if err := validateMapValue(Map{"value": value}); err != nil {
		t.Fatalf("map validation error = %v", err)
	}
	if err := validateRadixTreeEntries(Map{"value": value}); err != nil {
		t.Fatalf("radix validation error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("custom marshaler calls = %d, want 2", calls)
	}
}
