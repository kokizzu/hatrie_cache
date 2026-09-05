package hatSql_test

import (
	"encoding/binary"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLStringDictionaryRoundTrip(t *testing.T) {
	values := []string{"us-east", "us-east", "ap-south", "", "us-east", "ap-south"}
	wire, err := hatSql.EncodeSQLStringDictionary(values)
	if err != nil {
		t.Fatalf("EncodeSQLStringDictionary() error = %v", err)
	}
	got, err := hatSql.DecodeSQLStringDictionary(wire)
	if err != nil {
		t.Fatalf("DecodeSQLStringDictionary() error = %v", err)
	}
	if !reflect.DeepEqual(got, values) {
		t.Fatalf("round trip = %#v, want %#v", got, values)
	}
	if empty, err := hatSql.EncodeSQLStringDictionary(nil); err != nil || empty != nil {
		t.Fatalf("empty encode = %x, %v", empty, err)
	}
	if empty, err := hatSql.DecodeSQLStringDictionary(nil); err != nil || empty != nil {
		t.Fatalf("empty decode = %#v, %v", empty, err)
	}
}

func TestSQLStringDictionaryRejectsMalformedInput(t *testing.T) {
	wire, err := hatSql.EncodeSQLStringDictionary([]string{"one", "two"})
	if err != nil {
		t.Fatal(err)
	}
	tests := [][]byte{
		[]byte("BAD!"),
		wire[:3],
		wire[:len(wire)-1],
		append(append([]byte(nil), wire...), 1),
	}
	for index, encoded := range tests {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("DecodeSQLStringDictionary() panicked: %v", recovered)
				}
			}()
			if _, err := hatSql.DecodeSQLStringDictionary(encoded); err == nil {
				t.Fatal("DecodeSQLStringDictionary() error = nil")
			}
		})
	}
	invalidID := append([]byte(nil), wire...)
	invalidID[len(invalidID)-1] = 0xff
	if _, err := hatSql.DecodeSQLStringDictionary(invalidID); err == nil {
		t.Fatal("DecodeSQLStringDictionary() error = nil for invalid dictionary id")
	}
}

func TestSQLStringDictionaryReducesRepeatedValues(t *testing.T) {
	values := make([]string, 1024)
	for index := range values {
		values[index] = []string{"us-east", "us-west", "ap-south", "eu-west"}[index%4]
	}
	raw := encodeRawSQLStrings(values)
	dictionary, err := hatSql.EncodeSQLStringDictionary(values)
	if err != nil {
		t.Fatal(err)
	}
	if len(dictionary) >= len(raw) {
		t.Fatalf("dictionary wire size = %d, raw = %d", len(dictionary), len(raw))
	}
	if len(dictionary)*4 > len(raw) {
		t.Fatalf("dictionary wire size = %d, raw = %d, want at least 4x reduction", len(dictionary), len(raw))
	}
}

func BenchmarkSQLStringDictionary(b *testing.B) {
	repeated := make([]string, 1024)
	unique := make([]string, 1024)
	for index := range repeated {
		repeated[index] = []string{"us-east", "us-west", "ap-south", "eu-west"}[index%4]
		unique[index] = "value-" + string(rune(index>>8)) + string(rune(index))
	}
	rawRepeated := encodeRawSQLStrings(repeated)
	rawUnique := encodeRawSQLStrings(unique)
	b.Run("raw_repeated_encode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = encodeRawSQLStrings(repeated)
		}
		b.ReportMetric(float64(len(rawRepeated)), "wire_bytes")
	})
	b.Run("repeated_encode", func(b *testing.B) {
		wire, err := hatSql.EncodeSQLStringDictionary(repeated)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.EncodeSQLStringDictionary(repeated); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(wire)), "wire_bytes")
	})
	b.Run("raw_unique_encode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = encodeRawSQLStrings(unique)
		}
		b.ReportMetric(float64(len(rawUnique)), "wire_bytes")
	})
	b.Run("unique_encode", func(b *testing.B) {
		wire, err := hatSql.EncodeSQLStringDictionary(unique)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.EncodeSQLStringDictionary(unique); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(len(wire)), "wire_bytes")
	})
	repeatedWire, err := hatSql.EncodeSQLStringDictionary(repeated)
	if err != nil {
		b.Fatal(err)
	}
	uniqueWire, err := hatSql.EncodeSQLStringDictionary(unique)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("raw_repeated_decode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if values := decodeRawSQLStrings(rawRepeated); len(values) != len(repeated) {
				b.Fatalf("decoded raw repeated values = %d", len(values))
			}
		}
	})
	b.Run("repeated_decode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if values, err := hatSql.DecodeSQLStringDictionary(repeatedWire); err != nil || len(values) != len(repeated) {
				b.Fatalf("decoded repeated values = %d, %v", len(values), err)
			}
		}
	})
	b.Run("raw_unique_decode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if values := decodeRawSQLStrings(rawUnique); len(values) != len(unique) {
				b.Fatalf("decoded raw unique values = %d", len(values))
			}
		}
	})
	b.Run("unique_decode", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if values, err := hatSql.DecodeSQLStringDictionary(uniqueWire); err != nil || len(values) != len(unique) {
				b.Fatalf("decoded unique values = %d, %v", len(values), err)
			}
		}
	})
}

func encodeRawSQLStrings(values []string) []byte {
	encoded := make([]byte, 0)
	var length [binary.MaxVarintLen64]byte
	for _, value := range values {
		size := binary.PutUvarint(length[:], uint64(len(value)))
		encoded = append(encoded, length[:size]...)
		encoded = append(encoded, value...)
	}
	return encoded
}

func decodeRawSQLStrings(encoded []byte) []string {
	values := make([]string, 0)
	for offset := 0; offset < len(encoded); {
		length, size := binary.Uvarint(encoded[offset:])
		offset += size
		values = append(values, string(encoded[offset:offset+int(length)]))
		offset += int(length)
	}
	return values
}
