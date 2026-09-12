package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

func TestTupleFieldUpdatesApplyAtomically(t *testing.T) {
	count := make([]byte, 8)
	binary.BigEndian.PutUint64(count, 42)
	cache, err := NewPackedTuple([][]byte{[]byte("west"), count, []byte("abcdef")})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := cache.ApplyUpdates([]TupleFieldUpdate{
		{Index: 0, Kind: TupleFieldSet, Value: []byte("east")},
		{Index: 1, Kind: TupleFieldAddInt64, Delta: 8},
		{Index: 2, Kind: TupleFieldSplice, Start: 2, Remove: 2, Insert: []byte("XYZ")},
	})
	if err != nil {
		t.Fatalf("ApplyUpdates() error = %v", err)
	}
	if got, _ := updated.Field(0); string(got) != "east" {
		t.Fatalf("updated field 0 = %q", got)
	}
	field, err := updated.Field(1)
	if err != nil || int64(binary.BigEndian.Uint64(field)) != 50 {
		t.Fatalf("updated field 1 = %x, %v", field, err)
	}
	if got, _ := updated.Field(2); string(got) != "abXYZef" {
		t.Fatalf("updated field 2 = %q", got)
	}
	if got, _ := cache.Field(0); string(got) != "west" {
		t.Fatalf("source field 0 changed = %q", got)
	}
	if got, _ := cache.Field(2); string(got) != "abcdef" {
		t.Fatalf("source field 2 changed = %q", got)
	}
}

func TestTupleFieldUpdatesRejectInvalidInputAtomically(t *testing.T) {
	cache, err := NewPackedTuple([][]byte{[]byte("west"), []byte("42")})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		updates []TupleFieldUpdate
		want    error
	}{
		{name: "index", updates: []TupleFieldUpdate{{Index: 2, Kind: TupleFieldSet, Value: []byte("x")}}, want: ErrTupleFieldUpdateIndex},
		{name: "duplicate", updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte("a")}, {Index: 0, Kind: TupleFieldSet, Value: []byte("b")}}, want: ErrTupleFieldUpdateDuplicate},
		{name: "kind", updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldUpdateKind(99)}}, want: ErrTupleFieldUpdateKind},
		{name: "add type", updates: []TupleFieldUpdate{{Index: 1, Kind: TupleFieldAddInt64, Delta: 1}}, want: ErrTupleFieldUpdateType},
		{name: "splice start", updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSplice, Start: 5, Remove: 0}}, want: ErrTupleFieldUpdateRange},
		{name: "splice remove", updates: []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSplice, Start: 1, Remove: 9}}, want: ErrTupleFieldUpdateRange},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := cache.ApplyUpdates(test.updates); !errors.Is(err, test.want) {
				t.Fatalf("ApplyUpdates() error = %v, want %v", err, test.want)
			}
			if got, _ := cache.Field(0); string(got) != "west" {
				t.Fatalf("source changed after rejected update = %q", got)
			}
		})
	}
	count := make([]byte, 8)
	binary.BigEndian.PutUint64(count, uint64(^uint64(0)>>1))
	overflowCache, err := NewPackedTuple([][]byte{count})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := overflowCache.ApplyUpdates([]TupleFieldUpdate{{Index: 0, Kind: TupleFieldAddInt64, Delta: 1}}); !errors.Is(err, ErrTupleFieldUpdateOverflow) {
		t.Fatalf("positive overflow error = %v", err)
	}
	minimum := make([]byte, 8)
	binary.BigEndian.PutUint64(minimum, uint64(1)<<63)
	underflowCache, err := NewPackedTuple([][]byte{minimum})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := underflowCache.ApplyUpdates([]TupleFieldUpdate{{Index: 0, Kind: TupleFieldAddInt64, Delta: -1}}); !errors.Is(err, ErrTupleFieldUpdateOverflow) {
		t.Fatalf("negative underflow error = %v", err)
	}
}

func TestTupleFieldUpdatesReuseOffsetsForFixedWidth(t *testing.T) {
	count := make([]byte, 8)
	cache, err := NewPackedTuple([][]byte{[]byte("west"), count})
	if err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(100, func() {
		if _, err := cache.ApplyUpdates([]TupleFieldUpdate{{Index: 1, Kind: TupleFieldAddInt64, Delta: 1}}); err != nil {
			t.Fatal(err)
		}
	}); got != 1 {
		t.Fatalf("fixed-width update allocations = %f, want 1", got)
	}
	updated, err := cache.ApplyUpdates(nil)
	if err != nil || !reflect.DeepEqual(updated.Bytes(), cache.Bytes()) {
		t.Fatalf("empty update = (%q, %v), want unchanged", updated.Bytes(), err)
	}
}
