package hatDataStructure_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

type tupleUpdateJournalSyncBuffer struct {
	bytes.Buffer
	syncs int
}

func (writer *tupleUpdateJournalSyncBuffer) Sync() error {
	writer.syncs++
	return nil
}

func testTU19Format(t *testing.T) hatDataStructure.TupleFormat {
	t.Helper()
	format, err := hatDataStructure.NewTupleFormat(7, []hatDataStructure.TupleFieldSpec{
		{Name: "region", Type: hatDataStructure.TupleFieldString},
		{Name: "count", Type: hatDataStructure.TupleFieldInt64},
	})
	if err != nil {
		t.Fatalf("NewTupleFormat() error = %v", err)
	}
	return format
}

func testTU19Tuple(t *testing.T, format hatDataStructure.TupleFormat) hatDataStructure.VersionedTuple {
	t.Helper()
	tuple, err := format.PackVersioned([]hatDataStructure.TupleFieldValue{
		hatDataStructure.TupleString("west"),
		hatDataStructure.TupleInt64(41),
	})
	if err != nil {
		t.Fatalf("PackVersioned() error = %v", err)
	}
	return tuple
}

func TestTU19TupleFieldUpdateJournalRoundTripAndAtomicApply(t *testing.T) {
	format := testTU19Format(t)
	tuple := testTU19Tuple(t, format)
	record := hatDataStructure.TupleFieldUpdateJournalRecord{
		Sequence:      1,
		TupleID:       99,
		SchemaVersion: format.Version(),
		Updates: []hatDataStructure.TupleFieldUpdate{
			{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("east")},
			{Index: 1, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 1},
		},
	}

	encoded, err := hatDataStructure.MarshalTupleFieldUpdateJournalRecord(record)
	if err != nil {
		t.Fatalf("MarshalTupleFieldUpdateJournalRecord() error = %v", err)
	}
	decoded, err := hatDataStructure.UnmarshalTupleFieldUpdateJournalRecord(encoded)
	if err != nil {
		t.Fatalf("UnmarshalTupleFieldUpdateJournalRecord() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, record) {
		t.Fatalf("decoded record = %#v, want %#v", decoded, record)
	}

	updated, err := hatDataStructure.ApplyTupleFieldUpdateJournalRecord(tuple, format, decoded)
	if err != nil {
		t.Fatalf("ApplyTupleFieldUpdateJournalRecord() error = %v", err)
	}
	region, _ := updated.Tuple().Field(0)
	count, _ := updated.Tuple().Field(1)
	if string(region) != "east" || int64(binary.BigEndian.Uint64(count)) != 42 {
		t.Fatalf("updated tuple = %q/%d, want east/42", region, int64(binary.BigEndian.Uint64(count)))
	}

	badSchema := decoded
	badSchema.SchemaVersion++
	if _, err := hatDataStructure.ApplyTupleFieldUpdateJournalRecord(tuple, format, badSchema); !errors.Is(err, hatDataStructure.ErrTupleFieldUpdateJournalSchemaMismatch) {
		t.Fatalf("schema mismatch error = %v, want ErrTupleFieldUpdateJournalSchemaMismatch", err)
	}
	originalRegion, _ := tuple.Tuple().Field(0)
	if string(originalRegion) != "west" {
		t.Fatalf("source tuple changed after rejected replay: %q", originalRegion)
	}
}

func TestTU19TupleFieldUpdateJournalAppendsSyncsAndReplays(t *testing.T) {
	writer := &tupleUpdateJournalSyncBuffer{}
	journal, err := hatDataStructure.NewTupleFieldUpdateJournal(writer, hatDataStructure.TupleFieldUpdateJournalOptions{})
	if err != nil {
		t.Fatalf("NewTupleFieldUpdateJournal() error = %v", err)
	}
	first, err := journal.Append(11, 7, []hatDataStructure.TupleFieldUpdate{{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("east")}})
	if err != nil {
		t.Fatalf("first Append() error = %v", err)
	}
	second, err := journal.Append(12, 7, []hatDataStructure.TupleFieldUpdate{{Index: 1, Kind: hatDataStructure.TupleFieldAddInt64, Delta: 3}})
	if err != nil {
		t.Fatalf("second Append() error = %v", err)
	}
	if first.Sequence != 1 || second.Sequence != 2 || writer.syncs != 2 {
		t.Fatalf("append metadata = %#v/%#v syncs=%d, want sequences 1/2 and two syncs", first, second, writer.syncs)
	}
	noAutoSyncWriter := &tupleUpdateJournalSyncBuffer{}
	noAutoSyncJournal, err := hatDataStructure.NewTupleFieldUpdateJournal(noAutoSyncWriter, hatDataStructure.TupleFieldUpdateJournalOptions{NoSync: true})
	if err != nil {
		t.Fatalf("NewTupleFieldUpdateJournal(NoSync) error = %v", err)
	}
	if _, err := noAutoSyncJournal.Append(13, 7, []hatDataStructure.TupleFieldUpdate{{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("north")}}); err != nil {
		t.Fatalf("NoSync Append() error = %v", err)
	}
	if noAutoSyncWriter.syncs != 0 {
		t.Fatalf("NoSync append syncs = %d, want zero", noAutoSyncWriter.syncs)
	}
	if err := noAutoSyncJournal.Sync(); err != nil || noAutoSyncWriter.syncs != 1 {
		t.Fatalf("explicit Sync() error=%v syncs=%d, want one sync", err, noAutoSyncWriter.syncs)
	}

	var got []hatDataStructure.TupleFieldUpdateJournalRecord
	last, err := hatDataStructure.ReplayTupleFieldUpdateJournal(bytes.NewReader(writer.Bytes()), 0, hatDataStructure.TupleFieldUpdateJournalReplayOptions{}, func(record hatDataStructure.TupleFieldUpdateJournalRecord) error {
		got = append(got, record)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayTupleFieldUpdateJournal() error = %v", err)
	}
	if last != 2 || len(got) != 2 || got[0].TupleID != 11 || got[1].TupleID != 12 {
		t.Fatalf("replay = last=%d records=%#v, want last=2 and tuple IDs 11/12", last, got)
	}

	var resumed []hatDataStructure.TupleFieldUpdateJournalRecord
	last, err = hatDataStructure.ReplayTupleFieldUpdateJournal(bytes.NewReader(writer.Bytes()), 1, hatDataStructure.TupleFieldUpdateJournalReplayOptions{}, func(record hatDataStructure.TupleFieldUpdateJournalRecord) error {
		resumed = append(resumed, record)
		return nil
	})
	if err != nil || last != 2 || len(resumed) != 1 || resumed[0].Sequence != 2 {
		t.Fatalf("resumed replay = last=%d records=%#v err=%v, want sequence 2", last, resumed, err)
	}
}

func TestTU19TupleFieldUpdateJournalRejectsCorruptionTruncationAndOversize(t *testing.T) {
	record := hatDataStructure.TupleFieldUpdateJournalRecord{
		Sequence:      1,
		TupleID:       1,
		SchemaVersion: 1,
		Updates:       []hatDataStructure.TupleFieldUpdate{{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("ok")}},
	}
	encoded, err := hatDataStructure.MarshalTupleFieldUpdateJournalRecord(record)
	if err != nil {
		t.Fatalf("MarshalTupleFieldUpdateJournalRecord() error = %v", err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1]++
	if _, err := hatDataStructure.UnmarshalTupleFieldUpdateJournalRecord(corrupt); !errors.Is(err, hatDataStructure.ErrTupleFieldUpdateJournalWire) {
		t.Fatalf("corrupt record error = %v, want ErrTupleFieldUpdateJournalWire", err)
	}
	if _, err := hatDataStructure.UnmarshalTupleFieldUpdateJournalRecord(encoded[:5]); !errors.Is(err, hatDataStructure.ErrTupleFieldUpdateJournalTruncated) {
		t.Fatalf("truncated record error = %v, want ErrTupleFieldUpdateJournalTruncated", err)
	}
	tooLarge := record
	tooLarge.Updates = []hatDataStructure.TupleFieldUpdate{{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: make([]byte, hatDataStructure.DefaultTupleFieldUpdateJournalRecordBytes)}}
	if _, err := hatDataStructure.MarshalTupleFieldUpdateJournalRecord(tooLarge); !errors.Is(err, hatDataStructure.ErrTupleFieldUpdateJournalLimit) {
		t.Fatalf("oversized record error = %v, want ErrTupleFieldUpdateJournalLimit", err)
	}
}

func TestTU19TupleFieldUpdateJournalRejectsSequenceRegressionAndInvalidOptions(t *testing.T) {
	if _, err := hatDataStructure.NewTupleFieldUpdateJournal(nil, hatDataStructure.TupleFieldUpdateJournalOptions{}); !errors.Is(err, hatDataStructure.ErrTupleFieldUpdateJournalNil) {
		t.Fatalf("nil writer error = %v, want ErrTupleFieldUpdateJournalNil", err)
	}
	if _, err := hatDataStructure.NewTupleFieldUpdateJournal(bytes.NewBuffer(nil), hatDataStructure.TupleFieldUpdateJournalOptions{MaxRecordBytes: hatDataStructure.MaxTupleFieldUpdateJournalRecordBytes + 1}); !errors.Is(err, hatDataStructure.ErrTupleFieldUpdateJournalLimit) {
		t.Fatalf("oversized option error = %v, want ErrTupleFieldUpdateJournalLimit", err)
	}

	first := hatDataStructure.TupleFieldUpdateJournalRecord{Sequence: 2, TupleID: 1, SchemaVersion: 1, Updates: []hatDataStructure.TupleFieldUpdate{{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: []byte("a")}}}
	second := first
	second.Sequence = 1
	firstBytes, _ := hatDataStructure.MarshalTupleFieldUpdateJournalRecord(first)
	secondBytes, _ := hatDataStructure.MarshalTupleFieldUpdateJournalRecord(second)
	var framed bytes.Buffer
	var length [10]byte
	for _, payload := range [][]byte{firstBytes, secondBytes} {
		n := binary.PutUvarint(length[:], uint64(len(payload)))
		framed.Write(length[:n])
		framed.Write(payload)
	}
	if _, err := hatDataStructure.ReplayTupleFieldUpdateJournal(&framed, 0, hatDataStructure.TupleFieldUpdateJournalReplayOptions{}, func(hatDataStructure.TupleFieldUpdateJournalRecord) error { return nil }); !errors.Is(err, hatDataStructure.ErrTupleFieldUpdateJournalSequence) {
		t.Fatalf("sequence regression error = %v, want ErrTupleFieldUpdateJournalSequence", err)
	}
}

func TestTU19TupleFieldUpdateJournalSupportsCustomRecordLimit(t *testing.T) {
	writer := bytes.NewBuffer(nil)
	journal, err := hatDataStructure.NewTupleFieldUpdateJournal(writer, hatDataStructure.TupleFieldUpdateJournalOptions{
		MaxRecordBytes: 128 << 10,
		NoSync:         true,
	})
	if err != nil {
		t.Fatalf("NewTupleFieldUpdateJournal() error = %v", err)
	}
	if _, err := journal.Append(1, 1, []hatDataStructure.TupleFieldUpdate{{Index: 0, Kind: hatDataStructure.TupleFieldSet, Value: make([]byte, 70<<10)}}); err != nil {
		t.Fatalf("large Append() error = %v", err)
	}
	count := 0
	if _, err := hatDataStructure.ReplayTupleFieldUpdateJournal(bytes.NewReader(writer.Bytes()), 0, hatDataStructure.TupleFieldUpdateJournalReplayOptions{MaxRecordBytes: 128 << 10}, func(record hatDataStructure.TupleFieldUpdateJournalRecord) error {
		count++
		if len(record.Updates) != 1 || len(record.Updates[0].Value) != 70<<10 {
			t.Fatalf("large replay record = %#v", record)
		}
		return nil
	}); err != nil {
		t.Fatalf("large Replay() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("large replay count = %d, want one", count)
	}
}
