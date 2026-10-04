package hatDataStructure

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

type tupleFieldUpdateJournalJSONBaseline struct {
	Sequence uint64             `json:"sequence"`
	Key      string             `json:"key"`
	Updates  []TupleFieldUpdate `json:"updates"`
}

var (
	tupleFieldUpdateJournalBytesSink  []byte
	tupleFieldUpdateJournalRecordSink TupleFieldUpdateJournalRecord
	tupleFieldUpdateJournalJSONSink   tupleFieldUpdateJournalJSONBaseline
)

func benchmarkTupleFieldUpdateJournalRecord() TupleFieldUpdateJournalRecord {
	return TupleFieldUpdateJournalRecord{
		Sequence: 42,
		Key:      "orders/42",
		Updates: []TupleFieldUpdate{
			{Index: 0, Kind: TupleFieldSet, Value: []byte("east")},
			{Index: 1, Kind: TupleFieldAddInt64, Delta: 8},
			{Index: 2, Kind: TupleFieldSplice, Start: 2, Remove: 2, Insert: []byte("XYZ")},
		},
	}
}

func BenchmarkTupleFieldUpdateJournalMarshal(b *testing.B) {
	record := benchmarkTupleFieldUpdateJournalRecord()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := MarshalTupleFieldUpdateJournal(record)
		if err != nil {
			b.Fatal(err)
		}
		tupleFieldUpdateJournalBytesSink = encoded
	}
	b.ReportMetric(float64(len(tupleFieldUpdateJournalBytesSink)), "wire-bytes")
}

func BenchmarkTupleFieldUpdateJournalUnmarshal(b *testing.B) {
	encoded, err := MarshalTupleFieldUpdateJournal(benchmarkTupleFieldUpdateJournalRecord())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		record, err := UnmarshalTupleFieldUpdateJournal(encoded)
		if err != nil {
			b.Fatal(err)
		}
		tupleFieldUpdateJournalRecordSink = record
	}
}

func BenchmarkTupleFieldUpdateJournalJSONBaselineMarshal(b *testing.B) {
	record := benchmarkTupleFieldUpdateJournalRecord()
	baseline := tupleFieldUpdateJournalJSONBaseline{Sequence: record.Sequence, Key: record.Key, Updates: record.Updates}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := json.Marshal(baseline)
		if err != nil {
			b.Fatal(err)
		}
		tupleFieldUpdateJournalBytesSink = encoded
	}
	b.ReportMetric(float64(len(tupleFieldUpdateJournalBytesSink)), "wire-bytes")
}

func BenchmarkTupleFieldUpdateJournalJSONBaselineUnmarshal(b *testing.B) {
	record := benchmarkTupleFieldUpdateJournalRecord()
	encoded, err := json.Marshal(tupleFieldUpdateJournalJSONBaseline{Sequence: record.Sequence, Key: record.Key, Updates: record.Updates})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var decoded tupleFieldUpdateJournalJSONBaseline
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
		tupleFieldUpdateJournalJSONSink = decoded
	}
}

func TestTupleFieldUpdateJournalRoundTripsAndOwnsPayload(t *testing.T) {
	record := TupleFieldUpdateJournalRecord{
		Sequence: 42,
		Key:      "orders/42",
		Updates: []TupleFieldUpdate{
			{Index: 0, Kind: TupleFieldSet, Value: []byte("east")},
			{Index: 1, Kind: TupleFieldAddInt64, Delta: 8},
			{Index: 2, Kind: TupleFieldSplice, Start: 2, Remove: 2, Insert: []byte("XYZ")},
		},
	}

	encoded, err := MarshalTupleFieldUpdateJournal(record)
	if err != nil {
		t.Fatalf("MarshalTupleFieldUpdateJournal() error = %v", err)
	}
	decoded, err := UnmarshalTupleFieldUpdateJournal(encoded)
	if err != nil {
		t.Fatalf("UnmarshalTupleFieldUpdateJournal() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, record) {
		t.Fatalf("decoded record = %#v, want %#v", decoded, record)
	}

	reencoded, err := MarshalTupleFieldUpdateJournal(record)
	if err != nil {
		t.Fatalf("second MarshalTupleFieldUpdateJournal() error = %v", err)
	}
	if !bytes.Equal(reencoded, encoded) {
		t.Fatalf("encoding is not deterministic: first %x, second %x", encoded, reencoded)
	}

	record.Updates[0].Value[0] = 'X'
	if got := string(decoded.Updates[0].Value); got != "east" {
		t.Fatalf("decoded value changed with source mutation: %q", got)
	}
	decoded.Updates[2].Insert[0] = 'Q'
	if got := string(record.Updates[2].Insert); got != "XYZ" {
		t.Fatalf("source value was unexpectedly copied back: %q", got)
	}
}

func TestTupleFieldUpdateJournalAppliesDecodedUpdates(t *testing.T) {
	record := TupleFieldUpdateJournalRecord{
		Sequence: 7,
		Key:      "orders/7",
		Updates: []TupleFieldUpdate{
			{Index: 0, Kind: TupleFieldSet, Value: []byte("east")},
			{Index: 1, Kind: TupleFieldAddInt64, Delta: 8},
			{Index: 2, Kind: TupleFieldSplice, Start: 2, Remove: 2, Insert: []byte("XYZ")},
		},
	}
	encoded, err := MarshalTupleFieldUpdateJournal(record)
	if err != nil {
		t.Fatalf("MarshalTupleFieldUpdateJournal() error = %v", err)
	}
	decoded, err := UnmarshalTupleFieldUpdateJournal(encoded)
	if err != nil {
		t.Fatalf("UnmarshalTupleFieldUpdateJournal() error = %v", err)
	}

	count := make([]byte, 8)
	binary.BigEndian.PutUint64(count, 42)
	cache, err := NewPackedTuple([][]byte{[]byte("west"), count, []byte("abcdef")})
	if err != nil {
		t.Fatalf("NewPackedTuple() error = %v", err)
	}
	updated, err := cache.ApplyUpdates(decoded.Updates)
	if err != nil {
		t.Fatalf("ApplyUpdates() error = %v", err)
	}
	if field, err := updated.Field(0); err != nil || !bytes.Equal(field, []byte("east")) {
		t.Fatalf("field 0 = %q, error = %v", field, err)
	}
	if field, err := updated.Field(1); err != nil || int64(binary.BigEndian.Uint64(field)) != 50 {
		t.Fatalf("field 1 = %x, error = %v", field, err)
	}
	if field, err := updated.Field(2); err != nil || !bytes.Equal(field, []byte("abXYZef")) {
		t.Fatalf("field 2 = %q, error = %v", field, err)
	}
}

func TestTupleFieldUpdateJournalRejectsInvalidRecords(t *testing.T) {
	valid, err := MarshalTupleFieldUpdateJournal(TupleFieldUpdateJournalRecord{
		Sequence: 1,
		Key:      "k",
		Updates:  []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("v")}},
	})
	if err != nil {
		t.Fatalf("MarshalTupleFieldUpdateJournal() error = %v", err)
	}

	badMagic := append([]byte(nil), valid...)
	badMagic[0] = 'X'
	badVersion := append([]byte(nil), valid...)
	badVersion[4]++
	badChecksum := append([]byte(nil), valid...)
	badChecksum[len(badChecksum)-1]++
	trailing := append(append([]byte(nil), valid...), 0)

	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "empty", data: nil, want: ErrTupleFieldUpdateJournalInvalid},
		{name: "truncated", data: valid[:len(valid)-1], want: ErrTupleFieldUpdateJournalInvalid},
		{name: "magic", data: badMagic, want: ErrTupleFieldUpdateJournalInvalid},
		{name: "version", data: badVersion, want: ErrTupleFieldUpdateJournalInvalid},
		{name: "checksum", data: badChecksum, want: ErrTupleFieldUpdateJournalCorrupt},
		{name: "trailing", data: trailing, want: ErrTupleFieldUpdateJournalInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := UnmarshalTupleFieldUpdateJournal(test.data); !errors.Is(err, test.want) {
				t.Fatalf("UnmarshalTupleFieldUpdateJournal() error = %v, want %v", err, test.want)
			}
		})
	}

	if _, err := MarshalTupleFieldUpdateJournal(TupleFieldUpdateJournalRecord{}); !errors.Is(err, ErrTupleFieldUpdateJournalInvalid) {
		t.Fatalf("invalid record marshal error = %v, want %v", err, ErrTupleFieldUpdateJournalInvalid)
	}
	if _, err := MarshalTupleFieldUpdateJournal(TupleFieldUpdateJournalRecord{
		Sequence: 1,
		Key:      "k",
		Updates:  []TupleFieldUpdate{{Index: 0, Kind: TupleFieldUpdateKind(99)}},
	}); !errors.Is(err, ErrTupleFieldUpdateJournalInvalid) {
		t.Fatalf("invalid update marshal error = %v, want %v", err, ErrTupleFieldUpdateJournalInvalid)
	}
}

func FuzzTupleFieldUpdateJournalUnmarshalNeverPanics(f *testing.F) {
	valid, err := MarshalTupleFieldUpdateJournal(benchmarkTupleFieldUpdateJournalRecord())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte("HTU1"))
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Fuzz(func(t *testing.T, encoded []byte) {
		_, _ = UnmarshalTupleFieldUpdateJournal(encoded)
	})
}
