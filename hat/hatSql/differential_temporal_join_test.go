package hatSql

import (
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

func TestDifferentialTemporalJoinMatchesKeysWithinTimeDistance(t *testing.T) {
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		MaxTimeDistance: 3,
		LeftKey: func(row SQLRow) string {
			return row["join"].(string)
		},
		RightKey: func(row SQLRow) string {
			return row["join"].(string)
		},
	})
	if err != nil {
		t.Fatalf("NewDifferentialTemporalJoin() error = %v", err)
	}
	left := DifferentialRow{Key: "left-1", Time: 10, Diff: 1, Row: Row{"join": "a", "left": 1}}
	if got, err := join.ApplyLeft([]DifferentialRow{left}); err != nil || got != nil {
		t.Fatalf("ApplyLeft() = %#v, error = %v; want nil, nil", got, err)
	}
	right := DifferentialRow{Key: "right-1", Time: 12, Diff: 1, Row: Row{"join": "a", "right": 2}}
	got, err := join.ApplyRight([]DifferentialRow{right})
	if err != nil {
		t.Fatalf("ApplyRight() error = %v", err)
	}
	want := []DifferentialRow{{
		Key:  differentialTemporalJoinPairKey("left-1", "right-1"),
		Time: 12,
		Diff: 1,
		Row:  Row{"left.join": "a", "left.left": 1, "right.join": "a", "right.right": 2},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
	if _, err := join.ApplyLeft([]DifferentialRow{{Key: "left-1", Time: 10, Diff: -1}}); err != nil {
		t.Fatalf("remove left error = %v", err)
	}
	got, err = join.ApplyRight(nil)
	if err != nil {
		t.Fatalf("empty ApplyRight() error = %v", err)
	}
	if got != nil {
		t.Fatalf("empty ApplyRight() = %#v, want nil", got)
	}
}

func TestDifferentialTemporalJoinRejectsOutOfWindowAndDifferentKeys(t *testing.T) {
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		MaxTimeDistance: 1,
		LeftKey:         func(row SQLRow) string { return row["join"].(string) },
		RightKey:        func(row SQLRow) string { return row["join"].(string) },
	})
	if err != nil {
		t.Fatalf("NewDifferentialTemporalJoin() error = %v", err)
	}
	if _, err := join.ApplyLeft([]DifferentialRow{{Key: "left", Time: 1, Diff: 1, Row: Row{"join": "a"}}}); err != nil {
		t.Fatalf("ApplyLeft() error = %v", err)
	}
	for _, row := range []DifferentialRow{
		{Key: "far", Time: 3, Diff: 1, Row: Row{"join": "a"}},
		{Key: "other", Time: 1, Diff: 1, Row: Row{"join": "b"}},
	} {
		got, err := join.ApplyRight([]DifferentialRow{row})
		if err != nil {
			t.Fatalf("ApplyRight(%q) error = %v", row.Key, err)
		}
		if got != nil {
			t.Fatalf("ApplyRight(%q) = %#v, want nil", row.Key, got)
		}
	}
}

func TestDifferentialTemporalJoinPreservesWeightedMultiplicityAndClonesRows(t *testing.T) {
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		MaxTimeDistance: 0,
		LeftKey:         func(row SQLRow) string { return row["join"].(string) },
		RightKey:        func(row SQLRow) string { return row["join"].(string) },
	})
	if err != nil {
		t.Fatalf("NewDifferentialTemporalJoin() error = %v", err)
	}
	left := DifferentialRow{Key: "left", Time: 5, Diff: 2, Row: Row{"join": "a", "value": "left"}}
	if _, err := join.ApplyLeft([]DifferentialRow{left}); err != nil {
		t.Fatalf("ApplyLeft() error = %v", err)
	}
	left.Row["value"] = "changed"
	right := DifferentialRow{Key: "right", Time: 5, Diff: 3, Row: Row{"join": "a", "value": "right"}}
	got, err := join.ApplyRight([]DifferentialRow{right})
	if err != nil {
		t.Fatalf("ApplyRight() error = %v", err)
	}
	if len(got) != 1 || got[0].Diff != 6 || got[0].Row["left.value"] != "left" {
		t.Fatalf("got weighted join = %#v, want diff 6 with cloned left value", got)
	}
}

func TestDifferentialTemporalJoinRejectsInvalidStateAndCallbacks(t *testing.T) {
	if _, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{}); !errors.Is(err, ErrDifferentialTemporalJoinLeftKeyRequired) {
		t.Fatalf("missing left key error = %v, want ErrDifferentialTemporalJoinLeftKeyRequired", err)
	}
	if _, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{LeftKey: func(SQLRow) string { return "a" }}); !errors.Is(err, ErrDifferentialTemporalJoinRightKeyRequired) {
		t.Fatalf("missing right key error = %v, want ErrDifferentialTemporalJoinRightKeyRequired", err)
	}
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		LeftKey:  func(SQLRow) string { return "a" },
		RightKey: func(SQLRow) string { return "a" },
	})
	if err != nil {
		t.Fatalf("NewDifferentialTemporalJoin() error = %v", err)
	}
	if _, err := join.ApplyLeft([]DifferentialRow{{Key: "left", Diff: -1}}); !errors.Is(err, ErrDifferentialTemporalJoinNegativeMultiplicity) {
		t.Fatalf("negative left error = %v, want ErrDifferentialTemporalJoinNegativeMultiplicity", err)
	}
	var nilJoin *DifferentialTemporalJoin
	if _, err := nilJoin.ApplyRight(nil); !errors.Is(err, ErrDifferentialTemporalJoinNil) {
		t.Fatalf("nil join error = %v, want ErrDifferentialTemporalJoinNil", err)
	}
}

func TestDifferentialTemporalJoinRejectsPairDiffOverflow(t *testing.T) {
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		LeftKey:  func(row SQLRow) string { return row["join"].(string) },
		RightKey: func(row SQLRow) string { return row["join"].(string) },
	})
	if err != nil {
		t.Fatalf("NewDifferentialTemporalJoin() error = %v", err)
	}
	if _, err := join.ApplyRight([]DifferentialRow{{Key: "right", Time: 1, Diff: 1<<63 - 1, Row: Row{"join": "a"}}}); err != nil {
		t.Fatalf("seed ApplyRight() error = %v", err)
	}
	if _, err := join.ApplyLeft([]DifferentialRow{{Key: "left", Time: 1, Diff: 2, Row: Row{"join": "a"}}}); !errors.Is(err, ErrDifferentialTemporalJoinPairDiffOverflow) {
		t.Fatalf("pair overflow error = %v, want ErrDifferentialTemporalJoinPairDiffOverflow", err)
	}
}

func TestDifferentialTemporalJoinSerializesConcurrentUpdates(t *testing.T) {
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		LeftKey:  func(SQLRow) string { return "a" },
		RightKey: func(SQLRow) string { return "a" },
	})
	if err != nil {
		t.Fatalf("NewDifferentialTemporalJoin() error = %v", err)
	}
	var group sync.WaitGroup
	for index := 0; index < 32; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			if _, err := join.ApplyLeft([]DifferentialRow{{Key: strconv.Itoa(index), Time: uint64(index), Diff: 1, Row: Row{"value": index}}}); err != nil {
				t.Errorf("ApplyLeft() error = %v", err)
			}
		}(index)
	}
	group.Wait()
}

func BenchmarkDifferentialTemporalJoin(b *testing.B) {
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		MaxTimeDistance: 0,
		LeftKey:         func(row SQLRow) string { return row["join"].(string) },
		RightKey:        func(row SQLRow) string { return row["join"].(string) },
	})
	if err != nil {
		b.Fatal(err)
	}
	left := make([]DifferentialRow, 256)
	for index := range left {
		left[index] = DifferentialRow{
			Key:  "left-" + strconv.Itoa(index),
			Time: uint64(index % 64),
			Diff: 1,
			Row:  Row{"join": strconv.Itoa(index % 64), "value": index},
		}
	}
	right := make([]DifferentialRow, 1024)
	for index := range right {
		right[index] = DifferentialRow{
			Key:  "right-" + strconv.Itoa(index),
			Time: uint64(index % 64),
			Diff: 1,
			Row:  Row{"join": strconv.Itoa(index % 64), "value": index},
		}
	}
	if _, err := join.ApplyLeft(left); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := join.ApplyRight(right); err != nil {
			b.Fatal(err)
		}
	}
}
