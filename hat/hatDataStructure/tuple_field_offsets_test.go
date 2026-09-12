package hatDataStructure

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestTupleFieldOffsetCacheBuildAndAccess(t *testing.T) {
	data := []byte("abc123xyz")
	cache, err := NewTupleFieldOffsetCache(data, []uint32{3, 3, 3})
	if err != nil {
		t.Fatalf("NewTupleFieldOffsetCache() error = %v", err)
	}
	if got, want := cache.FieldCount(), 3; got != want {
		t.Fatalf("FieldCount() = %d, want %d", got, want)
	}
	if got, want := cache.Bytes(), data; !reflect.DeepEqual(got, want) {
		t.Fatalf("Bytes() = %q, want %q", got, want)
	}
	for index, want := range []string{"abc", "123", "xyz"} {
		field, err := cache.Field(index)
		if err != nil {
			t.Fatalf("Field(%d) error = %v", index, err)
		}
		if got := string(field); got != want {
			t.Fatalf("Field(%d) = %q, want %q", index, got, want)
		}
		start, end, err := cache.Offset(index)
		if err != nil {
			t.Fatalf("Offset(%d) error = %v", index, err)
		}
		if got := data[start:end]; string(got) != want {
			t.Fatalf("Offset(%d) = [%d:%d] -> %q, want %q", index, start, end, got, want)
		}
	}
	destination := make([]byte, 0, 3)
	got, err := cache.FieldInto(1, destination)
	if err != nil {
		t.Fatalf("FieldInto() error = %v", err)
	}
	if string(got) != "123" || &got[:1][0] != &destination[:1][0] {
		t.Fatalf("FieldInto() = %q, did not reuse destination", got)
	}
}

func TestPackedTupleOwnsDataAndClone(t *testing.T) {
	fields := [][]byte{[]byte("west"), []byte("42"), nil}
	packed, err := NewPackedTuple(fields)
	if err != nil {
		t.Fatalf("NewPackedTuple() error = %v", err)
	}
	fields[0][0] = 'X'
	field, err := packed.Field(0)
	if err != nil {
		t.Fatalf("packed Field() error = %v", err)
	}
	if got, want := string(field), "west"; got != want {
		t.Fatalf("packed Field(0) = %q, want %q", got, want)
	}
	if got := packed.FieldCount(); got != 3 {
		t.Fatalf("packed FieldCount() = %d, want 3", got)
	}
	clone := packed.Clone()
	if got, err := clone.Field(1); err != nil || string(got) != "42" {
		t.Fatalf("clone Field(1) = (%q, %v), want (42, nil)", got, err)
	}
	packed.Bytes()[0] = 'Y'
	cloneField, err := clone.Field(0)
	if err != nil {
		t.Fatalf("clone Field(0) error = %v", err)
	}
	if got, want := string(cloneField), "west"; got != want {
		t.Fatalf("clone changed with source mutation: got %q, want %q", got, want)
	}
}

func TestTupleFieldOffsetCacheRejectsInvalidInput(t *testing.T) {
	for _, test := range []struct {
		name   string
		data   []byte
		length []uint32
		want   error
	}{
		{name: "short data", data: []byte("abc"), length: []uint32{2, 2}, want: ErrTupleFieldOffsetLengths},
		{name: "long data", data: []byte("abc"), length: []uint32{1}, want: ErrTupleFieldOffsetLengths},
		{name: "uint32 overflow", data: make([]byte, 0), length: []uint32{^uint32(0), 1}, want: ErrTupleFieldOffsetLengths},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewTupleFieldOffsetCache(test.data, test.length); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
	cache, err := NewTupleFieldOffsetCache([]byte("abc"), []uint32{3})
	if err != nil {
		t.Fatalf("valid cache error = %v", err)
	}
	for _, index := range []int{-1, 1, int(^uint(0) >> 1)} {
		if _, err := cache.Field(index); !errors.Is(err, ErrTupleFieldOffsetIndex) {
			t.Fatalf("Field(%d) error = %v, want %v", index, err, ErrTupleFieldOffsetIndex)
		}
		if _, _, err := cache.Offset(index); !errors.Is(err, ErrTupleFieldOffsetIndex) {
			t.Fatalf("Offset(%d) error = %v, want %v", index, err, ErrTupleFieldOffsetIndex)
		}
	}
	if _, err := NewTupleFieldOffsetCache(nil, make([]uint32, MaxTupleFieldOffsetFields+1)); !errors.Is(err, ErrTupleFieldOffsetCount) {
		t.Fatalf("too many fields error = %v, want %v", err, ErrTupleFieldOffsetCount)
	}
	if _, err := NewPackedTuple(make([][]byte, MaxTupleFieldOffsetFields+1)); !errors.Is(err, ErrTupleFieldOffsetCount) {
		t.Fatalf("packed too many fields error = %v, want %v", err, ErrTupleFieldOffsetCount)
	}
}

func TestTupleFieldOffsetCacheConcurrentReads(t *testing.T) {
	cache, err := NewTupleFieldOffsetCache([]byte("aabbcc"), []uint32{2, 2, 2})
	if err != nil {
		t.Fatalf("NewTupleFieldOffsetCache() error = %v", err)
	}
	var waitGroup sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			for iteration := 0; iteration < 1000; iteration++ {
				field, err := cache.Field(iteration % 3)
				if err != nil || len(field) != 2 {
					t.Errorf("Field() = (%q, %v), want length 2 and nil", field, err)
					return
				}
			}
		}()
	}
	waitGroup.Wait()
}

func BenchmarkTupleFieldOffsetCacheAccess(b *testing.B) {
	lengths := make([]uint32, 1024)
	for index := range lengths {
		lengths[index] = 1
	}
	cache, err := NewTupleFieldOffsetCache(make([]byte, 1024), lengths)
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < cache.FieldCount(); index++ {
		cache.data[index] = byte(index)
	}
	b.ResetTimer()
	var sink byte
	for iteration := 0; iteration < b.N; iteration++ {
		field, err := cache.Field(768)
		if err != nil {
			b.Fatal(err)
		}
		sink ^= field[0]
	}
	benchmarkTupleFieldOffsetSink = sink
}

func BenchmarkTupleFieldScan(b *testing.B) {
	lengths := make([]uint32, 1024)
	for index := range lengths {
		lengths[index] = 1
	}
	b.ResetTimer()
	var sink uint32
	for iteration := 0; iteration < b.N; iteration++ {
		var offset uint32
		for index := 0; index <= 768; index++ {
			offset += lengths[index]
		}
		sink ^= offset
	}
	benchmarkTupleFieldScanSink = sink
}

func BenchmarkNewPackedTuple(b *testing.B) {
	fields := tupleFieldOffsetBenchmarkFields(512)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		packed, err := NewPackedTuple(fields)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkPackedTupleSink = packed
	}
}

func BenchmarkNaiveTupleCopies(b *testing.B) {
	fields := tupleFieldOffsetBenchmarkFields(512)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		copies := make([][]byte, len(fields))
		for index, field := range fields {
			copies[index] = append([]byte(nil), field...)
		}
		benchmarkNaiveTupleSink = copies
	}
}

var (
	benchmarkTupleFieldOffsetSink byte
	benchmarkTupleFieldScanSink   uint32
	benchmarkPackedTupleSink      TupleFieldOffsetCache
	benchmarkNaiveTupleSink       [][]byte
)

func tupleFieldOffsetBenchmarkFields(count int) [][]byte {
	fields := make([][]byte, count)
	for index := range fields {
		fields[index] = []byte("field-value-0123456789")
	}
	return fields
}
