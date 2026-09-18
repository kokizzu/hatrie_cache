package hatSql_test

import (
	"errors"
	"strconv"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestMZ028TemporalIntervalArrangementQueriesHalfOpenIntervals(t *testing.T) {
	arrangement, err := hatSql.NewSQLTemporalIntervalArrangement(hatSql.SQLTemporalIntervalArrangementOptions{MaxIntervals: 8})
	if err != nil {
		t.Fatalf("NewSQLTemporalIntervalArrangement() error = %v", err)
	}
	for _, interval := range []hatSql.SQLTemporalInterval{
		{ID: "a", Key: "account", Start: 0, End: 10, Row: hatSql.Row{"id": int64(1)}},
		{ID: "b", Key: "account", Start: 5, End: 15, Row: hatSql.Row{"id": int64(2)}},
		{ID: "c", Key: "other", Start: 0, End: 20, Row: hatSql.Row{"id": int64(3)}},
	} {
		if err := arrangement.Upsert(interval); err != nil {
			t.Fatalf("Upsert(%q) error = %v", interval.ID, err)
		}
	}

	got, err := arrangement.At("account", 10)
	if err != nil {
		t.Fatalf("At() error = %v", err)
	}
	if len(got) != 1 || got[0].ID != "b" {
		t.Fatalf("At(account,10) = %#v, want [b]", got)
	}
	overlap, err := arrangement.Overlap("account", 9, 11)
	if err != nil {
		t.Fatalf("Overlap() error = %v", err)
	}
	if len(overlap) != 2 || overlap[0].ID != "a" || overlap[1].ID != "b" {
		t.Fatalf("Overlap(account,9,11) = %#v, want [a b]", overlap)
	}

	got[0].Row["id"] = int64(99)
	again, err := arrangement.At("account", 10)
	if err != nil {
		t.Fatalf("second At() error = %v", err)
	}
	if again[0].Row["id"] != int64(2) {
		t.Fatalf("query row mutation changed stored row = %#v", again[0].Row)
	}
}

func TestMZ028TemporalIntervalArrangementReplacesDeletesAndSnapshots(t *testing.T) {
	arrangement, err := hatSql.NewSQLTemporalIntervalArrangement(hatSql.SQLTemporalIntervalArrangementOptions{MaxIntervals: 2})
	if err != nil {
		t.Fatalf("NewSQLTemporalIntervalArrangement() error = %v", err)
	}
	if err := arrangement.Upsert(hatSql.SQLTemporalInterval{ID: "row", Key: "account", Start: 0, End: 10, Row: hatSql.Row{"value": "old"}}); err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Upsert(hatSql.SQLTemporalInterval{ID: "row", Key: "account", Start: 20, End: 30, Row: hatSql.Row{"value": "new"}}); err != nil {
		t.Fatal(err)
	}
	if arrangement.Len() != 1 {
		t.Fatalf("Len() = %d, want 1 after replacement", arrangement.Len())
	}
	old, err := arrangement.At("account", 5)
	if err != nil || len(old) != 0 {
		t.Fatalf("old interval lookup = %#v/%v, want empty", old, err)
	}

	if err := arrangement.Upsert(hatSql.SQLTemporalInterval{ID: "second", Key: "account", Start: 30, End: 40, Row: hatSql.Row{"value": "second"}}); err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Upsert(hatSql.SQLTemporalInterval{ID: "full", Key: "account", Start: 40, End: 50, Row: hatSql.Row{"value": "full"}}); !errors.Is(err, hatSql.ErrSQLTemporalIntervalArrangementFull) {
		t.Fatalf("full Upsert() error = %v, want full", err)
	}
	if deleted, err := arrangement.Delete("second"); err != nil || !deleted {
		t.Fatalf("Delete(second) = %t/%v, want true/nil", deleted, err)
	}
	if deleted, err := arrangement.Delete("second"); err != nil || deleted {
		t.Fatalf("second Delete(second) = %t/%v, want false/nil", deleted, err)
	}

	snapshot := arrangement.Snapshot()
	if len(snapshot) != 1 || snapshot[0].ID != "row" || snapshot[0].Row["value"] != "new" {
		t.Fatalf("Snapshot() = %#v, want replacement only", snapshot)
	}
}

func TestMZ028TemporalIntervalArrangementValidatesAtomically(t *testing.T) {
	arrangement, err := hatSql.NewSQLTemporalIntervalArrangement(hatSql.SQLTemporalIntervalArrangementOptions{MaxIntervals: 1})
	if err != nil {
		t.Fatal(err)
	}
	invalid := []hatSql.SQLTemporalInterval{
		{Key: "account", Start: 0, End: 1, Row: hatSql.Row{}},
		{ID: "id", Key: "account", Start: 2, End: 2, Row: hatSql.Row{}},
		{ID: "id", Key: "account", Start: 3, End: 2, Row: hatSql.Row{}},
	}
	for _, interval := range invalid {
		if err := arrangement.Upsert(interval); !errors.Is(err, hatSql.ErrSQLTemporalIntervalArrangementInvalid) {
			t.Errorf("invalid Upsert(%#v) error = %v, want invalid", interval, err)
		}
	}
	if err := arrangement.Upsert(hatSql.SQLTemporalInterval{ID: "valid", Key: "account", Start: 0, End: 1, Row: hatSql.Row{}}); err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Upsert(hatSql.SQLTemporalInterval{ID: "another", Key: "account", Start: 1, End: 2, Row: hatSql.Row{}}); !errors.Is(err, hatSql.ErrSQLTemporalIntervalArrangementFull) {
		t.Fatalf("capacity Upsert() error = %v, want full", err)
	}
	if err := arrangement.Upsert(hatSql.SQLTemporalInterval{ID: "valid", Key: "account", Start: 4, End: 5, Row: hatSql.Row{}}); err != nil {
		t.Fatalf("replacement at capacity error = %v", err)
	}
	if _, err := arrangement.At("", 0); !errors.Is(err, hatSql.ErrSQLTemporalIntervalArrangementInvalid) {
		t.Fatalf("empty At() error = %v, want invalid", err)
	}
	if _, err := arrangement.Overlap("account", 2, 2); !errors.Is(err, hatSql.ErrSQLTemporalIntervalArrangementInvalid) {
		t.Fatalf("empty Overlap() error = %v, want invalid", err)
	}
}

func TestMZ028TemporalIntervalArrangementScalesPointQueries(t *testing.T) {
	arrangement, err := hatSql.NewSQLTemporalIntervalArrangement(hatSql.SQLTemporalIntervalArrangementOptions{MaxIntervals: 256})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 256; index++ {
		if err := arrangement.Upsert(hatSql.SQLTemporalInterval{
			ID:    "id-" + strconv.Itoa(index),
			Key:   "account",
			Start: int64(index * 2),
			End:   int64(index*2 + 2),
			Row:   hatSql.Row{"id": int64(index)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	for index := 0; index < 256; index++ {
		rows, err := arrangement.At("account", int64(index*2))
		if err != nil || len(rows) != 1 || rows[0].Row["id"] != int64(index) {
			t.Fatalf("At(%d) = %#v/%v, want row %d", index*2, rows, err, index)
		}
	}
}
