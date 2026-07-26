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

func TestFlatScalarSequenceValidationDoesNotAllocate(t *testing.T) {
	values := Slice{"value", true, nil, []byte("bytes"), int64(-1), uint64(2), float64(3.5)}
	queue := make(PriorityQueue, len(values))
	for index, value := range values {
		queue[index] = PriorityItem{Priority: int64(index), Value: value}
	}
	for _, test := range []struct {
		name     string
		validate func() error
	}{
		{name: "slice", validate: func() error { return validateSliceValue(values) }},
		{name: "slice payload", validate: func() error { return validateSliceValues(values[0], values[1:]...) }},
		{name: "priority queue", validate: func() error { return validatePriorityQueueValue(queue) }},
		{name: "priority payload", validate: func() error { return validatePriorityQueuePayload(values[0], values[1:]...) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(1000, func() {
				structuredValidationTestSink = test.validate()
			})
			if structuredValidationTestSink != nil {
				t.Fatalf("validation error = %v", structuredValidationTestSink)
			}
			if allocs != 0 {
				t.Fatalf("flat scalar sequence validation allocations = %.0f, want 0", allocs)
			}
		})
	}
}

func TestSparseNestedSlicePayloadValidationDoesNotMaterializeSequence(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts include race detector instrumentation")
	}
	values := make(Slice, 4096)
	for index := range values {
		values[index] = "value"
	}
	values[len(values)-1] = Map{"nested": "value"}
	allocs := testing.AllocsPerRun(1000, func() {
		structuredValidationTestSink = validateSliceValues(values[0], values[1:]...)
	})
	if structuredValidationTestSink != nil {
		t.Fatalf("validation error = %v", structuredValidationTestSink)
	}
	if allocs != 1 {
		t.Fatalf("sparse nested slice validation allocations = %.0f, want 1", allocs)
	}
}

func TestMultiFallbackSlicePayloadInvokesMarshalersOnce(t *testing.T) {
	calls := 0
	value := structuredValidationTrackingMarshaler{calls: &calls}
	if err := validateSliceValues("before", value, "middle", value, "after"); err != nil {
		t.Fatalf("validation error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("custom marshaler calls = %d, want 2", calls)
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

func TestSequenceValidationMatchesJSONMarshalAcceptance(t *testing.T) {
	cycle := Map{}
	cycle["cycle"] = cycle
	for _, test := range []struct {
		name  string
		value interface{}
	}{
		{name: "nested valid", value: Map{"nested": Slice{"value", 1.5, true}}},
		{name: "not a number", value: math.NaN()},
		{name: "invalid number", value: json.Number("not-a-number")},
		{name: "function", value: func() {}},
		{name: "failing marshaler", value: structuredValidationFailingMarshaler{}},
		{name: "cycle", value: cycle},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := Slice{"before", test.value, "after"}
			_, marshalErr := json.Marshal(payload)
			sliceErr := validateSliceValue(payload)
			slicePayloadErr := validateSliceValues(payload[0], payload[1:]...)
			queueErr := validatePriorityQueueValue(PriorityQueue{{Priority: 1, Value: test.value}})
			priorityPayloadErr := validatePriorityQueuePayload(test.value)
			for name, validationErr := range map[string]error{
				"slice": sliceErr, "slice payload": slicePayloadErr,
				"priority queue": queueErr, "priority payload": priorityPayloadErr,
			} {
				if (validationErr == nil) != (marshalErr == nil) {
					t.Fatalf("%s validation error = %v, marshal error = %v", name, validationErr, marshalErr)
				}
			}
		})
	}
}
