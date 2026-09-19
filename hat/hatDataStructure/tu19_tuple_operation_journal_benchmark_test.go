package hatDataStructure

import (
	"encoding/binary"
	"path/filepath"
	"strconv"
	"testing"
)

var (
	tu19BenchmarkTupleSink  TupleFieldOffsetCache
	tu19BenchmarkRecordSink TupleFieldOperationRecord
	tu19BenchmarkBytesSink  []byte
)

func BenchmarkTupleFieldOperationJournalBaselineApply(b *testing.B) {
	tuple := tu19BenchmarkTuple()
	updates := tu19BenchmarkUpdates()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		updated, err := tuple.ApplyUpdates(updates)
		if err != nil {
			b.Fatal(err)
		}
		tu19BenchmarkTupleSink = updated
	}
}

func BenchmarkTupleFieldOperationJournalAppendMemory(b *testing.B) {
	journal, err := NewTupleFieldOperationJournal(TupleFieldOperationJournalOptions{MaxRecords: 256, MaxBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	operation := TupleFieldOperation{OperationID: "bench", Updates: tu19BenchmarkUpdates()}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		operation.OperationID = "bench-" + strconv.FormatUint(uint64(index), 10)
		record, appendErr := journal.Append(operation)
		if appendErr != nil {
			b.Fatal(appendErr)
		}
		tu19BenchmarkRecordSink = record
	}
}

func BenchmarkTupleFieldOperationJournalMarshalBinary(b *testing.B) {
	journal, err := NewTupleFieldOperationJournal(TupleFieldOperationJournalOptions{MaxRecords: 256, MaxBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := journal.Append(TupleFieldOperation{OperationID: "bench", Updates: tu19BenchmarkUpdates()}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, marshalErr := journal.MarshalBinary()
		if marshalErr != nil {
			b.Fatal(marshalErr)
		}
		tu19BenchmarkBytesSink = encoded
	}
}

func BenchmarkTupleFieldOperationJournalReplayAndApply(b *testing.B) {
	journal, err := NewTupleFieldOperationJournal(TupleFieldOperationJournalOptions{MaxRecords: 256, MaxBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := journal.Append(TupleFieldOperation{OperationID: "bench", Updates: tu19BenchmarkUpdates()}); err != nil {
		b.Fatal(err)
	}
	records, err := journal.Replay(0, 0)
	if err != nil {
		b.Fatal(err)
	}
	tuple := tu19BenchmarkTuple()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		updated, replayErr := ReplayTupleFieldOperationRecords(tuple, records)
		if replayErr != nil {
			b.Fatal(replayErr)
		}
		tu19BenchmarkTupleSink = updated
	}
}

func BenchmarkTupleFieldOperationJournalAppendDurable(b *testing.B) {
	path := filepath.Join(b.TempDir(), "tuple-operations.journal")
	journal, err := OpenTupleFieldOperationJournal(path, TupleFieldOperationJournalOptions{MaxRecords: 256, MaxBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	operation := TupleFieldOperation{Updates: tu19BenchmarkUpdates()}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		operation.OperationID = "durable-" + strconv.FormatUint(uint64(index), 10)
		if _, appendErr := journal.Append(operation); appendErr != nil {
			b.Fatal(appendErr)
		}
	}
}

func tu19BenchmarkTuple() TupleFieldOffsetCache {
	count := make([]byte, 8)
	binary.BigEndian.PutUint64(count, 41)
	tuple, err := NewPackedTuple([][]byte{[]byte("west"), count, []byte("abcdef")})
	if err != nil {
		panic(err)
	}
	return tuple
}

func tu19BenchmarkUpdates() []TupleFieldUpdate {
	return []TupleFieldUpdate{
		{Index: 0, Kind: TupleFieldSet, Value: []byte("east")},
		{Index: 1, Kind: TupleFieldAddInt64, Delta: 1},
		{Index: 2, Kind: TupleFieldSplice, Start: 2, Remove: 2, Insert: []byte("XYZ")},
	}
}
