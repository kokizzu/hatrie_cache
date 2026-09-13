package hatSql

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"strings"
	"testing"
)

func TestSQLDecimal128RoundTripUsesFixedWidthCoefficient(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "amount", Type: SQLRowBinaryDecimal128, DecimalScale: 2, DecimalPrecision: 38}}
	positive, err := ParseSQLDecimal128("12345678901234567890.12", 2)
	if err != nil {
		t.Fatalf("ParseSQLDecimal128(positive) error = %v", err)
	}
	negative, err := ParseSQLDecimal128("-0.34", 2)
	if err != nil {
		t.Fatalf("ParseSQLDecimal128(negative) error = %v", err)
	}
	rows := []SQLRow{{"amount": SQLDecimal("12345678901234567890.12")}, {"amount": negative}, {"amount": positive}}

	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinary() error = %v", err)
	}
	if len(encoded) != 48 {
		t.Fatalf("encoded length = %d, want 48 fixed-width bytes", len(encoded))
	}
	decoded, err := DecodeSQLRowBinary(columns, encoded)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinary() error = %v", err)
	}
	if _, ok := decoded[0]["amount"].(SQLDecimal128); !ok {
		t.Fatalf("decoded type = %T, want SQLDecimal128", decoded[0]["amount"])
	}
	for index, want := range []string{"12345678901234567890.12", "-0.34", "12345678901234567890.12"} {
		value := decoded[index]["amount"].(SQLDecimal128)
		formatted, formatErr := value.Format(2)
		if formatErr != nil {
			t.Fatalf("decoded[%d].Format() error = %v", index, formatErr)
		}
		if formatted != want {
			t.Fatalf("decoded[%d] = %q, want %q", index, formatted, want)
		}
	}
}

func TestSQLDecimal256RoundTripAndWireWidth(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "amount", Type: SQLRowBinaryDecimal256, DecimalScale: 4, DecimalPrecision: 76}}
	value, err := ParseSQLDecimal256("12345678901234567890123456789012345678901234567890.1234", 4)
	if err != nil {
		t.Fatalf("ParseSQLDecimal256() error = %v", err)
	}
	encoded, err := EncodeSQLRowBinary(columns, []SQLRow{{"amount": value}})
	if err != nil {
		t.Fatalf("EncodeSQLRowBinary() error = %v", err)
	}
	if len(encoded) != 32 {
		t.Fatalf("encoded length = %d, want 32 fixed-width bytes", len(encoded))
	}
	decoded, err := DecodeSQLRowBinary(columns, encoded)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinary() error = %v", err)
	}
	formatted, err := decoded[0]["amount"].(SQLDecimal256).Format(4)
	if err != nil {
		t.Fatalf("decoded.Format() error = %v", err)
	}
	if formatted != "12345678901234567890123456789012345678901234567890.1234" {
		t.Fatalf("formatted = %q, want original decimal", formatted)
	}
}

func TestSQLDecimalValidationRejectsBadScalePrecisionAndValues(t *testing.T) {
	cases := []struct {
		name    string
		columns []SQLRowBinaryColumn
		rows    []SQLRow
	}{
		{name: "decimal128 scale too large", columns: []SQLRowBinaryColumn{{Name: "amount", Type: SQLRowBinaryDecimal128, DecimalScale: 39, DecimalPrecision: 39}}},
		{name: "decimal128 precision too large", columns: []SQLRowBinaryColumn{{Name: "amount", Type: SQLRowBinaryDecimal128, DecimalScale: 2, DecimalPrecision: 39}}},
		{name: "scale exceeds precision", columns: []SQLRowBinaryColumn{{Name: "amount", Type: SQLRowBinaryDecimal128, DecimalScale: 3, DecimalPrecision: 2}}},
		{name: "metadata on string", columns: []SQLRowBinaryColumn{{Name: "amount", Type: SQLRowBinaryString, DecimalScale: 2}}},
		{name: "fraction does not fit scale", columns: []SQLRowBinaryColumn{{Name: "amount", Type: SQLRowBinaryDecimal128, DecimalScale: 2}}, rows: []SQLRow{{"amount": "1.234"}}},
		{name: "invalid decimal", columns: []SQLRowBinaryColumn{{Name: "amount", Type: SQLRowBinaryDecimal128, DecimalScale: 2}}, rows: []SQLRow{{"amount": "not-a-number"}}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := EncodeSQLRowBinary(testCase.columns, testCase.rows); err == nil {
				t.Fatal("EncodeSQLRowBinary() error = nil, want error")
			}
		})
	}
}

func TestSQLDecimalOverflowAndFormatValidation(t *testing.T) {
	if _, err := ParseSQLDecimal128("170141183460469231731687303715884105728", 0); err == nil {
		t.Fatal("ParseSQLDecimal128(max+1) error = nil, want overflow")
	}
	if _, err := ParseSQLDecimal256("1", 77); err == nil {
		t.Fatal("ParseSQLDecimal256(scale 77) error = nil, want scale overflow")
	}
	value, err := ParseSQLDecimal128("1.23", 2)
	if err != nil {
		t.Fatalf("ParseSQLDecimal128() error = %v", err)
	}
	if _, err := value.Format(39); err == nil {
		t.Fatal("SQLDecimal128.Format(scale 39) error = nil, want error")
	}
	if _, err := ParseSQLDecimal256(strings.Repeat("9", 129), 0); err == nil || !strings.Contains(err.Error(), "input") {
		t.Fatalf("ParseSQLDecimal256(oversized input) error = %v, want bounded-input error", err)
	}
}

func TestSQLDecimalRoundTripsAcrossBitmapAdaptiveStatsAndPruning(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "amount", Type: SQLRowBinaryDecimal128, Nullable: true, DecimalScale: 2, DecimalPrecision: 38}}
	rows := []SQLRow{{"amount": SQLDecimal("1.20")}, {"amount": nil}, {"amount": SQLDecimal("3.40")}}
	wantFirst, _ := ParseSQLDecimal128("1.20", 2)
	wantLast, _ := ParseSQLDecimal128("3.40", 2)
	want := []SQLRow{{"amount": wantFirst}, {"amount": nil}, {"amount": wantLast}}

	bitmap, err := EncodeSQLRowBinaryBitmap(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryBitmap() error = %v", err)
	}
	decodedBitmap, err := DecodeSQLRowBinaryBitmap(columns, bitmap)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryBitmap() error = %v", err)
	}
	if !reflect.DeepEqual(decodedBitmap, want) {
		t.Fatalf("bitmap decoded = %#v, want %#v", decodedBitmap, want)
	}

	adaptive, err := EncodeSQLRowBinaryAdaptive(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryAdaptive() error = %v", err)
	}
	decodedAdaptive, err := DecodeSQLRowBinaryAdaptive(columns, adaptive)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryAdaptive() error = %v", err)
	}
	if !reflect.DeepEqual(decodedAdaptive, want) {
		t.Fatalf("adaptive decoded = %#v, want %#v", decodedAdaptive, want)
	}

	withStats, err := EncodeSQLRowBinaryWithStats(columns, rows)
	if err != nil {
		t.Fatalf("EncodeSQLRowBinaryWithStats() error = %v", err)
	}
	decodedStatsRows, stats, err := DecodeSQLRowBinaryWithStats(columns, withStats)
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryWithStats() error = %v", err)
	}
	if !reflect.DeepEqual(decodedStatsRows, want) || len(stats) != 1 || stats[0].Min != wantFirst || stats[0].Max != wantLast {
		t.Fatalf("stats result = rows %#v stats %#v, want typed range", decodedStatsRows, stats)
	}
	skip, err := CanSkipSQLRowBinaryStats(columns, stats, SQLRowBinaryStatsPredicate{Column: "amount", Operator: SQLRowBinaryStatsEqual, Value: "4.00"})
	if err != nil {
		t.Fatalf("CanSkipSQLRowBinaryStats() error = %v", err)
	}
	if !skip {
		t.Fatal("CanSkipSQLRowBinaryStats() = false, want 4.00 outside [1.20, 3.40] to skip")
	}
}

func TestSQLDecimalStreamRoundTrip(t *testing.T) {
	value, err := ParseSQLDecimal128("12.34", 2)
	if err != nil {
		t.Fatalf("ParseSQLDecimal128() error = %v", err)
	}
	var encoded bytes.Buffer
	writer, err := NewSQLRowBinaryStreamWriterWithColumns(&encoded, []SQLRowBinaryColumn{{Name: "amount", Type: SQLRowBinaryDecimal128, Nullable: true, DecimalScale: 2, DecimalPrecision: 38}})
	if err != nil {
		t.Fatalf("NewSQLRowBinaryStreamWriterWithColumns() error = %v", err)
	}
	if err := writer.WriteRow(Row{"amount": value}); err != nil {
		t.Fatalf("WriteRow() error = %v", err)
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
	columns, rows, err := DecodeSQLRowBinaryStream(encoded.Bytes())
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryStream() error = %v", err)
	}
	if len(columns) != 1 || columns[0].Type != SQLRowBinaryDecimal128 || columns[0].DecimalScale != 2 || len(rows) != 1 {
		t.Fatalf("columns/rows = %#v/%#v, want Decimal128 row", columns, rows)
	}
	if !reflect.DeepEqual(rows[0]["amount"], value) {
		t.Fatalf("stream value = %#v, want %#v", rows[0]["amount"], value)
	}
}

func TestSQLDecimalStreamInferenceRejectsTypedDecimalWithoutScale(t *testing.T) {
	value, err := ParseSQLDecimal128("12.34", 2)
	if err != nil {
		t.Fatalf("ParseSQLDecimal128() error = %v", err)
	}
	var encoded bytes.Buffer
	writer := NewSQLRowBinaryStreamWriter(&encoded, []string{"amount"})
	if err := writer.WriteRow(Row{"amount": value}); err == nil {
		t.Fatal("WriteRow() error = nil, want explicit decimal schema error")
	}
}

func TestSQLDecimalGobRoundTripPreservesTypedCoefficient(t *testing.T) {
	value, err := ParseSQLDecimal256("123456789012345678901234567890.1234", 4)
	if err != nil {
		t.Fatalf("ParseSQLDecimal256() error = %v", err)
	}
	want := SQLRow{"amount": value}
	var encoded bytes.Buffer
	if err := gob.NewEncoder(&encoded).Encode(want); err != nil {
		t.Fatalf("gob Encode() error = %v", err)
	}
	var got SQLRow
	if err := gob.NewDecoder(&encoded).Decode(&got); err != nil {
		t.Fatalf("gob Decode() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("gob value = %#v, want %#v", got, want)
	}
}

func TestSQLDecimalDictionaryFallbackPreservesColumnMetadata(t *testing.T) {
	columns := []SQLRowBinaryColumn{{Name: "amount", Type: SQLRowBinaryDecimal128, DecimalScale: 2, DecimalPrecision: 18}}
	encoder, err := NewSQLRowBinaryDictionaryEncoder(columns, nil)
	if err != nil {
		t.Fatalf("NewSQLRowBinaryDictionaryEncoder() error = %v", err)
	}
	encoded, err := encoder.Encode([]SQLRow{{"amount": SQLDecimal("12.34")}})
	if err != nil {
		t.Fatalf("dictionary Encode() error = %v", err)
	}
	decoder, err := NewSQLRowBinaryDictionaryDecoder(columns, nil)
	if err != nil {
		t.Fatalf("NewSQLRowBinaryDictionaryDecoder() error = %v", err)
	}
	rows, err := decoder.Decode(encoded)
	if err != nil {
		t.Fatalf("dictionary Decode() error = %v", err)
	}
	want, err := ParseSQLDecimal128("12.34", 2)
	if err != nil {
		t.Fatalf("ParseSQLDecimal128() error = %v", err)
	}
	if len(rows) != 1 || !reflect.DeepEqual(rows[0]["amount"], want) {
		t.Fatalf("dictionary rows = %#v, want typed decimal %#v", rows, want)
	}
}
