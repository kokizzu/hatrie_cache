package hatDataStructure_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestSealedUpsertRunSortsConsolidatesAndOwnsValues(t *testing.T) {
	records := []hatDataStructure.UpsertRecord[[]byte]{
		{Key: "customer:002", Value: []byte("two")},
		{Key: "customer:001", Value: []byte("old")},
		{Key: "deleted", Value: []byte("discarded"), Deleted: true},
		{Key: "customer:001", Value: []byte("latest")},
		{Key: "customer:010", Value: []byte("ten")},
	}
	run, err := hatDataStructure.NewSealedUpsertRun(records, hatDataStructure.SealedUpsertRunOptions{IndexStride: 2})
	if err != nil {
		t.Fatalf("NewSealedUpsertRun() error = %v", err)
	}
	if got := run.Len(); got != 4 {
		t.Fatalf("Len() = %d, want 4 distinct records", got)
	}

	records[3].Value[0] = 'X'
	got, found := run.Lookup("customer:001")
	if !found || got.Deleted || !reflect.DeepEqual(got.Value, []byte("latest")) {
		t.Fatalf("Lookup(customer:001) = %#v, %t, want latest value", got, found)
	}
	got.Value[0] = 'X'
	again, found := run.Lookup("customer:001")
	if !found || !reflect.DeepEqual(again.Value, []byte("latest")) {
		t.Fatalf("Lookup(customer:001) after caller mutation = %#v, %t, want independent value", again, found)
	}

	deleted, found := run.Lookup("deleted")
	if !found || !deleted.Deleted || len(deleted.Value) != 0 {
		t.Fatalf("Lookup(deleted) = %#v, %t, want tombstone without value", deleted, found)
	}
	if _, found := run.Lookup("missing"); found {
		t.Fatal("Lookup(missing) found an entry")
	}

	var visited []hatDataStructure.SealedUpsertRecord
	run.ForEach(func(record hatDataStructure.SealedUpsertRecord) {
		visited = append(visited, record)
	})
	want := []hatDataStructure.SealedUpsertRecord{
		{Key: "customer:001", Value: []byte("latest")},
		{Key: "customer:002", Value: []byte("two")},
		{Key: "customer:010", Value: []byte("ten")},
		{Key: "deleted", Deleted: true},
	}
	if !reflect.DeepEqual(visited, want) {
		t.Fatalf("ForEach() = %#v, want %#v", visited, want)
	}
}

func TestSealedUpsertRunBinaryRoundTripAndValidation(t *testing.T) {
	records := make([]hatDataStructure.UpsertRecord[[]byte], 0, 130)
	for index := 0; index < 130; index++ {
		records = append(records, hatDataStructure.UpsertRecord[[]byte]{
			Key:   "tenant:region:customer:" + zeroPaddedSealedRunNumber(index),
			Value: []byte("value-" + zeroPaddedSealedRunNumber(index)),
		})
	}
	records = append(records, hatDataStructure.UpsertRecord[[]byte]{Key: "tombstone", Deleted: true})
	options := hatDataStructure.SealedUpsertRunOptions{IndexStride: 8}
	original, err := hatDataStructure.NewSealedUpsertRun(records, options)
	if err != nil {
		t.Fatalf("NewSealedUpsertRun() error = %v", err)
	}
	wire, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	if len(wire) != original.WireBytes() || len(wire) < 32 {
		t.Fatalf("WireBytes() = %d, encoded length = %d", original.WireBytes(), len(wire))
	}
	restored, err := hatDataStructure.UnmarshalSealedUpsertRun(wire, options)
	if err != nil {
		t.Fatalf("UnmarshalSealedUpsertRun() error = %v", err)
	}
	wire[0] = 'X'
	value, found := restored.Lookup("tenant:region:customer:000129")
	if !found || !reflect.DeepEqual(value.Value, []byte("value-000129")) {
		t.Fatalf("restored Lookup() = %#v, %t, want final record", value, found)
	}

	for name, mutate := range map[string]func([]byte){
		"bad magic":    func(data []byte) { data[0] = 'X' },
		"bad checksum": func(data []byte) { data[len(data)-1] ^= 0xff },
		"truncated":    func(data []byte) {},
	} {
		data := make([]byte, len(wire))
		copy(data, wire)
		if name == "truncated" {
			data = data[:len(data)-1]
		}
		mutate(data)
		if _, err := hatDataStructure.UnmarshalSealedUpsertRun(data, options); !errors.Is(err, hatDataStructure.ErrSealedUpsertRunCorrupt) {
			t.Errorf("%s error = %v, want corrupt error", name, err)
		}
	}

	for name, options := range map[string]hatDataStructure.SealedUpsertRunOptions{
		"negative records":     {MaxRecords: -1},
		"negative key bytes":   {MaxKeyBytes: -1},
		"negative value bytes": {MaxValueBytes: -1},
		"negative wire bytes":  {MaxWireBytes: -1},
		"negative stride":      {IndexStride: -1},
	} {
		if _, err := hatDataStructure.NewSealedUpsertRun(nil, options); !errors.Is(err, hatDataStructure.ErrSealedUpsertRunOptions) {
			t.Errorf("%s error = %v, want options error", name, err)
		}
	}
	if _, err := hatDataStructure.NewSealedUpsertRun([]hatDataStructure.UpsertRecord[[]byte]{{Key: ""}}, hatDataStructure.SealedUpsertRunOptions{}); !errors.Is(err, hatDataStructure.ErrSealedUpsertRunKeyRequired) {
		t.Errorf("empty key error = %v, want key error", err)
	}
	if _, err := hatDataStructure.NewSealedUpsertRun([]hatDataStructure.UpsertRecord[[]byte]{{Key: "key", Value: []byte("value")}}, hatDataStructure.SealedUpsertRunOptions{MaxValueBytes: 2}); !errors.Is(err, hatDataStructure.ErrSealedUpsertRunValueTooLarge) {
		t.Errorf("large value error = %v, want value limit error", err)
	}
	if _, err := hatDataStructure.UnmarshalSealedUpsertRun(wire, hatDataStructure.SealedUpsertRunOptions{MaxWireBytes: len(wire) - 1}); !errors.Is(err, hatDataStructure.ErrSealedUpsertRunWireLimit) {
		t.Errorf("wire limit error = %v, want wire limit error", err)
	}
}

func TestSealedUpsertRunSupportsZeroValueAndNilReceiver(t *testing.T) {
	var run hatDataStructure.SealedUpsertRun
	if run.Len() != 0 || run.WireBytes() != 0 {
		t.Fatalf("zero-value metrics = len %d, bytes %d, want zero", run.Len(), run.WireBytes())
	}
	if _, found := run.Lookup("key"); found {
		t.Fatal("zero-value Lookup found an entry")
	}
	called := false
	run.ForEach(func(hatDataStructure.SealedUpsertRecord) { called = true })
	if called {
		t.Fatal("zero-value ForEach called visitor")
	}
	if data, err := run.MarshalBinary(); err != nil || len(data) != 0 {
		t.Fatalf("zero-value MarshalBinary() = %d bytes, %v, want empty/nil", len(data), err)
	}
	var nilRun *hatDataStructure.SealedUpsertRun
	if _, err := nilRun.MarshalBinary(); !errors.Is(err, hatDataStructure.ErrSealedUpsertRunNil) {
		t.Fatalf("nil MarshalBinary() error = %v, want nil receiver error", err)
	}
}

func TestSealedUpsertRunLookupHandlesLongFrontCodedKeys(t *testing.T) {
	prefix := strings.Repeat("x", 512)
	run, err := hatDataStructure.NewSealedUpsertRun([]hatDataStructure.UpsertRecord[[]byte]{
		{Key: prefix + ":001", Value: []byte("one")},
		{Key: prefix + ":002", Value: []byte("two")},
	}, hatDataStructure.SealedUpsertRunOptions{IndexStride: 2})
	if err != nil {
		t.Fatalf("NewSealedUpsertRun() error = %v", err)
	}
	record, found := run.Lookup(prefix + ":002")
	if !found || !reflect.DeepEqual(record.Value, []byte("two")) {
		t.Fatalf("Lookup(long key) = %#v, %t, want two", record, found)
	}
}

func TestSealedUpsertRunSingleRecordHonorsRecordLimit(t *testing.T) {
	options := hatDataStructure.SealedUpsertRunOptions{MaxRecords: 1}
	run, err := hatDataStructure.NewSealedUpsertRun([]hatDataStructure.UpsertRecord[[]byte]{{Key: "key", Value: []byte("value")}}, options)
	if err != nil {
		t.Fatalf("NewSealedUpsertRun() error = %v", err)
	}
	wire, err := run.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	if _, err := hatDataStructure.UnmarshalSealedUpsertRun(wire, options); err != nil {
		t.Fatalf("UnmarshalSealedUpsertRun() error = %v, want valid one-record run", err)
	}
}

func zeroPaddedSealedRunNumber(value int) string {
	if value < 10 {
		return "00000" + string(rune('0'+value))
	}
	if value < 100 {
		return "0000" + string(rune('0'+value/10)) + string(rune('0'+value%10))
	}
	return "000" + string(rune('0'+value/100)) + string(rune('0'+(value/10)%10)) + string(rune('0'+value%10))
}
