package hatSql

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestSQLPartialAggregateStateRoundTripAndMerge(t *testing.T) {
	left := SQLPartialAggregateState{Groups: []SQLPartialAggregateGroup{
		{
			Key:     "north",
			Value:   "North",
			Ordinal: 3,
			Count:   2,
			Sum:     7.5,
			HasSum:  true,
			Min:     int64(2),
			HasMin:  true,
			Max:     int64(5),
			HasMax:  true,
		},
		{
			Key:     "when",
			Value:   time.Unix(123, 456).UTC(),
			Ordinal: 4,
			Count:   1,
		},
	}}
	right := SQLPartialAggregateState{Groups: []SQLPartialAggregateGroup{
		{
			Key:     "north",
			Value:   "North",
			Ordinal: 8,
			Count:   3,
			Sum:     9.5,
			HasSum:  true,
			Min:     int64(1),
			HasMin:  true,
			Max:     int64(8),
			HasMax:  true,
		},
		{
			Key:     "south",
			Value:   "South",
			Ordinal: 2,
			Count:   4,
			Sum:     10,
			HasSum:  true,
			Min:     float64(1.25),
			HasMin:  true,
			Max:     float64(7.75),
			HasMax:  true,
		},
	}}

	encoded, err := EncodeSQLPartialAggregateState(left)
	if err != nil {
		t.Fatalf("encode left: %v", err)
	}
	encodedAgain, err := EncodeSQLPartialAggregateState(left)
	if err != nil {
		t.Fatalf("encode left again: %v", err)
	}
	if !bytes.Equal(encoded, encodedAgain) {
		t.Fatal("encoding is not deterministic")
	}
	decoded, err := DecodeSQLPartialAggregateState(encoded)
	if err != nil {
		t.Fatalf("decode left: %v", err)
	}
	if !reflect.DeepEqual(decoded, left) {
		t.Fatalf("decoded left = %#v, want %#v", decoded, left)
	}

	merged := left
	if err := MergeSQLPartialAggregateState(&merged, right); err != nil {
		t.Fatalf("merge states: %v", err)
	}
	want := SQLPartialAggregateState{Groups: []SQLPartialAggregateGroup{
		{
			Key:     "north",
			Value:   "North",
			Ordinal: 3,
			Count:   5,
			Sum:     17,
			HasSum:  true,
			Min:     int64(1),
			HasMin:  true,
			Max:     int64(8),
			HasMax:  true,
		},
		{
			Key:     "when",
			Value:   time.Unix(123, 456).UTC(),
			Ordinal: 4,
			Count:   1,
		},
		{
			Key:     "south",
			Value:   "South",
			Ordinal: 2,
			Count:   4,
			Sum:     10,
			HasSum:  true,
			Min:     float64(1.25),
			HasMin:  true,
			Max:     float64(7.75),
			HasMax:  true,
		},
	}}
	if !reflect.DeepEqual(merged, want) {
		t.Fatalf("merged = %#v, want %#v", merged, want)
	}
}

func TestSQLPartialAggregateStateRejectsMalformedInput(t *testing.T) {
	state := SQLPartialAggregateState{Groups: []SQLPartialAggregateGroup{{
		Key:    "x",
		Value:  "x",
		Count:  1,
		Sum:    math.NaN(),
		HasSum: true,
	}}}
	encoded, err := EncodeSQLPartialAggregateState(state)
	if err != nil {
		t.Fatalf("encode malformed-input fixture: %v", err)
	}
	version := append([]byte(nil), encoded...)
	version[len(sqlPartialAggregateStateMagic)] = 0xff
	marker := append([]byte(nil), encoded...)
	marker[0] = 'X'
	for name, input := range map[string][]byte{
		"empty":     nil,
		"truncated": encoded[:len(encoded)-1],
		"trailing":  append(append([]byte(nil), encoded...), 0),
		"marker":    marker,
		"version":   version,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeSQLPartialAggregateState(input); err == nil {
				t.Fatalf("DecodeSQLPartialAggregateState(%q) unexpectedly succeeded", name)
			}
		})
	}
}

func TestSQLPartialAggregateStateRejectsUnsupportedAndOverflow(t *testing.T) {
	if _, err := EncodeSQLPartialAggregateState(SQLPartialAggregateState{Groups: []SQLPartialAggregateGroup{{
		Key:   "unsupported",
		Value: map[string]interface{}{"x": 1},
	}}}); err == nil {
		t.Fatal("unsupported group value unexpectedly encoded")
	}

	destination := SQLPartialAggregateState{Groups: []SQLPartialAggregateGroup{{
		Key:   "overflow",
		Count: math.MaxInt64,
	}}}
	snapshot := destination
	err := MergeSQLPartialAggregateState(&destination, SQLPartialAggregateState{Groups: []SQLPartialAggregateGroup{{
		Key:   "overflow",
		Count: 1,
	}}})
	if err == nil {
		t.Fatal("count overflow unexpectedly succeeded")
	}
	if !reflect.DeepEqual(destination, snapshot) {
		t.Fatalf("destination changed after overflow: %#v, want %#v", destination, snapshot)
	}
}

func TestSQLPartialAggregateStateBinaryMarshaler(t *testing.T) {
	want := SQLPartialAggregateState{Groups: []SQLPartialAggregateGroup{{
		Key:    "north",
		Value:  "North",
		Count:  2,
		Sum:    7,
		HasSum: true,
	}}}
	encoded, err := want.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal binary state: %v", err)
	}
	var got SQLPartialAggregateState
	if err := got.UnmarshalBinary(encoded); err != nil {
		t.Fatalf("unmarshal binary state: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unmarshaled state = %#v, want %#v", got, want)
	}
}

func BenchmarkSQLPartialAggregateStateCodec(b *testing.B) {
	state := SQLPartialAggregateState{Groups: make([]SQLPartialAggregateGroup, 1024)}
	for index := range state.Groups {
		state.Groups[index] = SQLPartialAggregateGroup{
			Key:     "region-" + strconv.Itoa(index),
			Value:   "region-" + strconv.Itoa(index),
			Ordinal: uint64(index),
			Count:   int64(index + 1),
			Sum:     float64(index) * 1.5,
			HasSum:  true,
			Min:     float64(index),
			HasMin:  true,
			Max:     float64(index + 10),
			HasMax:  true,
		}
	}
	binaryPayload, err := EncodeSQLPartialAggregateState(state)
	if err != nil {
		b.Fatal(err)
	}
	jsonPayload, err := json.Marshal(state)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(binaryPayload)), "binary-payload-bytes")
	b.ReportMetric(float64(len(jsonPayload)), "json-payload-bytes")

	b.Run("binary-encode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(binaryPayload)))
		b.ReportMetric(float64(len(binaryPayload)), "payload-bytes")
		for range b.N {
			if _, err := EncodeSQLPartialAggregateState(state); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("json-encode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(jsonPayload)))
		b.ReportMetric(float64(len(jsonPayload)), "payload-bytes")
		for range b.N {
			if _, err := json.Marshal(state); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("binary-decode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(binaryPayload)))
		b.ReportMetric(float64(len(binaryPayload)), "payload-bytes")
		for range b.N {
			if _, err := DecodeSQLPartialAggregateState(binaryPayload); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("json-decode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(jsonPayload)))
		b.ReportMetric(float64(len(jsonPayload)), "payload-bytes")
		for range b.N {
			var decoded SQLPartialAggregateState
			if err := json.Unmarshal(jsonPayload, &decoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}
