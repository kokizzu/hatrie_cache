package hatSql

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestCH037SQLArgExtremeStateUsesQueryCollation(t *testing.T) {
	query, err := CompileSQLQuery("FROM VALUES ('first', 'A'), ('second', 'a') AS events(payload, score) SELECT ARGMAX_STATE(events.payload, events.score)")
	if err != nil {
		t.Fatal(err)
	}
	result, err := query.Execute(context.Background(), nil, nil, SQLQueryOptions{Collation: SQLCollationUnicodeCI})
	if err != nil {
		t.Fatal(err)
	}
	state, ok := result.Rows[0]["argmax_state"].([]byte)
	if !ok {
		t.Fatalf("state = %#v, want []byte", result.Rows[0]["argmax_state"])
	}
	decoded, err := sqlDecodeArgExtremeState(state)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.collation != SQLCollationUnicodeCI || decoded.selected != "first" {
		t.Fatalf("decoded state = %#v, want UnicodeCI first winner", decoded)
	}
}

func TestCH037ArgExtremeScalarRoundTrip(t *testing.T) {
	values := []interface{}{
		nil,
		"text",
		[]byte("bytes"),
		false,
		true,
		time.Date(2026, time.September, 16, 12, 34, 56, 789, time.UTC),
		int(-7), int8(-8), int16(-9), int32(-10), int64(-11),
		uint(7), uint8(8), uint16(9), uint32(10), uint64(11),
		float32(1.5), float64(-2.5),
		sqlDate("2026-09-16"),
		sqlDecimal("12345678901234567890.125"),
		sqlUUID("00112233-4455-6677-8899-aabbccddeeff"),
		sqlDuration("2h3m4s"),
		SQLIPv4(0x01020304),
		SQLIPv6{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	}
	for _, want := range values {
		encoded, err := sqlAppendArgExtremeScalar(nil, want)
		if err != nil {
			t.Fatalf("encode %T: %v", want, err)
		}
		got, offset, err := sqlReadArgExtremeScalar(encoded, 0)
		if err != nil {
			t.Fatalf("decode %T: %v", want, err)
		}
		if offset != len(encoded) {
			t.Fatalf("decode %T offset = %d, want %d", want, offset, len(encoded))
		}
		if wantTime, ok := want.(time.Time); ok {
			gotTime, gotOK := got.(time.Time)
			if !gotOK || !gotTime.Equal(wantTime) {
				t.Fatalf("round-trip %T = %#v, want %#v", want, got, want)
			}
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round-trip %T = %#v, want %#v", want, got, want)
		}
	}
}

func TestCH037ArgExtremeStateCollationAndTieMerge(t *testing.T) {
	left := sqlArgExtremeStateAccumulator{
		kind:         sqlArgExtremeStateMax,
		collation:    SQLCollationUnicodeCI,
		collationSet: true,
	}
	if err := left.add("first", "A"); err != nil {
		t.Fatal(err)
	}
	right := sqlArgExtremeStateAccumulator{
		kind:         sqlArgExtremeStateMax,
		collation:    SQLCollationUnicodeCI,
		collationSet: true,
	}
	if err := right.add("second", "a"); err != nil {
		t.Fatal(err)
	}
	encoded, err := right.output(true)
	if err != nil {
		t.Fatal(err)
	}
	merged := sqlArgExtremeStateAccumulator{kind: sqlArgExtremeStateMax}
	if err := merged.mergeValue(encoded); err != nil {
		t.Fatal(err)
	}
	leftEncoded, err := left.output(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := merged.mergeValue(leftEncoded); err != nil {
		t.Fatal(err)
	}
	if got := merged.result(false); got != "second" {
		t.Fatalf("tie merge winner = %#v, want second", got)
	}
}

func TestCH037ArgExtremeStateRejectsTrailingAndInvalidFlags(t *testing.T) {
	encoded, err := sqlEncodeArgExtremeState(sqlArgExtremeStateMax, SQLCollationBinary, "winner", int64(1), true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDecodeArgExtremeState(append(append([]byte(nil), encoded...), 0)); err == nil {
		t.Fatal("trailing state bytes unexpectedly accepted")
	}
	encoded[7] = 0x80
	if _, err := sqlDecodeArgExtremeState(encoded); err == nil {
		t.Fatal("invalid state flags unexpectedly accepted")
	}
}
