package hatSql_test

import (
	"encoding/binary"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLRowBinaryAdaptiveDecoderReusesRowsAndRecovers(t *testing.T) {
	columns, rows := rowBinaryAdaptiveEncoderFixture()
	encoded, err := hatSql.EncodeSQLRowBinaryAdaptive(columns, rows)
	if err != nil {
		t.Fatal(err)
	}

	var decoder hatSql.SQLRowBinaryAdaptiveDecoder
	decoded, err := decoder.DecodeInto(nil, columns, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, rows) {
		t.Fatalf("decoded rows differ from source:\n got %#v\nwant %#v", decoded, rows)
	}
	if len(decoded) == 0 {
		t.Fatal("decoder returned no rows")
	}
	decoded[0]["stale"] = "must be cleared"
	rowAddress := &decoded[0]

	next, err := decoder.DecodeInto(decoded[:0], columns, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if &next[0] != rowAddress {
		t.Fatal("decoder did not reuse the caller row slice")
	}
	if _, ok := next[0]["stale"]; ok {
		t.Fatal("decoder retained a stale map field")
	}
	if !reflect.DeepEqual(next, rows) {
		t.Fatalf("reused decode differs from source:\n got %#v\nwant %#v", next, rows)
	}

	if _, err := decoder.DecodeInto(next[:0], columns, encoded[:len(encoded)-1]); err == nil {
		t.Fatal("truncated adaptive input unexpectedly decoded")
	}
	recovered, err := decoder.DecodeInto(next[:0], columns, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recovered, rows) {
		t.Fatalf("decoder did not recover after malformed input: %#v", recovered)
	}

	empty, err := decoder.DecodeInto(recovered, columns, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("empty decode returned %d rows", len(empty))
	}
}

func TestSQLRowBinaryAdaptiveDecoderCoversAllCodecsAndTypes(t *testing.T) {
	columns := []hatSql.SQLRowBinaryColumn{
		{Name: "int", Type: hatSql.SQLRowBinaryInt64},
		{Name: "uint", Type: hatSql.SQLRowBinaryUint64},
		{Name: "float", Type: hatSql.SQLRowBinaryFloat64},
		{Name: "bool", Type: hatSql.SQLRowBinaryBool},
		{Name: "string", Type: hatSql.SQLRowBinaryString},
		{Name: "bytes", Type: hatSql.SQLRowBinaryBytes},
		{Name: "json", Type: hatSql.SQLRowBinaryJSON},
		{Name: "date", Type: hatSql.SQLRowBinaryDate},
		{Name: "datetime", Type: hatSql.SQLRowBinaryDateTime},
		{Name: "duration", Type: hatSql.SQLRowBinaryDuration},
		{Name: "uuid", Type: hatSql.SQLRowBinaryUUID},
		{Name: "optional", Type: hatSql.SQLRowBinaryString, Nullable: true},
	}
	rows := []hatSql.SQLRow{
		{
			"int":      int64(-10),
			"uint":     uint64(20),
			"float":    float64(1.25),
			"bool":     true,
			"string":   "first",
			"bytes":    []byte{1, 2, 3},
			"json":     json.RawMessage(`{"ok":true}`),
			"date":     time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			"datetime": time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
			"duration": time.Second,
			"uuid":     [16]byte{1, 2, 3},
			"optional": nil,
		},
		{
			"int":      int64(11),
			"uint":     uint64(21),
			"float":    float64(2.5),
			"bool":     false,
			"string":   "second",
			"bytes":    []byte{4, 5},
			"json":     json.RawMessage(`{"ok":false}`),
			"date":     time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			"datetime": time.Date(2024, 1, 2, 3, 4, 6, 0, time.UTC),
			"duration": 2 * time.Second,
			"uuid":     [16]byte{4, 5, 6},
			"optional": "present",
		},
	}

	legacy, err := hatSql.EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := hatSql.EncodeSQLRowBinaryDelta(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	doubleDelta, err := hatSql.EncodeSQLRowBinaryDoubleDelta(columns, rows)
	if err != nil {
		t.Fatal(err)
	}

	for _, testCase := range []struct {
		name    string
		codec   hatSql.SQLRowBinaryAdaptiveCodec
		payload []byte
	}{
		{name: "legacy", codec: hatSql.SQLRowBinaryAdaptiveCodecLegacy, payload: legacy},
		{name: "delta", codec: hatSql.SQLRowBinaryAdaptiveCodecDelta, payload: delta},
		{name: "double_delta", codec: hatSql.SQLRowBinaryAdaptiveCodecDoubleDelta, payload: doubleDelta},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			enveloped := wrapSQLRowBinaryAdaptivePayload(testCase.codec, testCase.payload)
			decoded, decodeErr := hatSql.DecodeSQLRowBinaryAdaptiveInto(nil, columns, enveloped)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if !reflect.DeepEqual(decoded, rows) {
				t.Fatalf("decoded rows differ from source:\n got %#v\nwant %#v", decoded, rows)
			}
		})
	}
}

func wrapSQLRowBinaryAdaptivePayload(codec hatSql.SQLRowBinaryAdaptiveCodec, payload []byte) []byte {
	encoded := make([]byte, 0, 5+binary.MaxVarintLen64+len(payload))
	encoded = append(encoded, []byte("HSA1")...)
	encoded = append(encoded, byte(codec))
	var length [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(length[:], uint64(len(payload)))
	encoded = append(encoded, length[:n]...)
	return append(encoded, payload...)
}
