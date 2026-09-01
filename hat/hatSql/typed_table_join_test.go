package hatSql_test

import (
	"math"
	"sync"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableJoinAppliesExactInsertUpdateAndDeleteChanges(t *testing.T) {
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "scores", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}, {Name: "score", Kind: hatSql.TypedTableInt64}}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "people", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}, {Name: "name", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key, team string
		score     int64
	}{{"a", "red", 3}, {"b", "blue", 7}} {
		if _, err := left.Upsert(row.key, []hatSql.TypedTableValue{hatSql.TypedString(row.team), hatSql.TypedInt64(row.score)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range []struct{ key, team, name string }{{"u", "red", "Ada"}, {"v", "red", "Lin"}, {"w", "blue", "Grace"}} {
		if _, err := right.Upsert(row.key, []hatSql.TypedTableValue{hatSql.TypedString(row.team), hatSql.TypedString(row.name)}); err != nil {
			t.Fatal(err)
		}
	}

	join, err := hatSql.NewTypedTableJoin(left, right, hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		t.Fatal(err)
	}
	assertTypedTableJoinPairs(t, join.Rows(), "a/u", "a/v", "b/w")
	if join.LeftCheckpoint() != 2 || join.RightCheckpoint() != 3 {
		t.Fatalf("checkpoints = %d/%d", join.LeftCheckpoint(), join.RightCheckpoint())
	}

	change, err := left.Upsert("b", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(8)})
	if err != nil {
		t.Fatal(err)
	}
	if err := join.ApplyLeft([]hatSql.TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	assertTypedTableJoinPairs(t, join.Rows(), "a/u", "a/v", "b/u", "b/v")

	change, err = right.Delete("v")
	if err != nil {
		t.Fatal(err)
	}
	if err := join.ApplyRight([]hatSql.TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	assertTypedTableJoinPairs(t, join.Rows(), "a/u", "b/u")
}

func TestTypedTableJoinRowsAreIndependentlyOwned(t *testing.T) {
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "scores_owned", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "people_owned", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := left.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("u", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}
	join, err := hatSql.NewTypedTableJoin(left, right, hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		t.Fatal(err)
	}

	rows := join.Rows()
	rows[0].Left[0] = hatSql.TypedString("mutated")
	rows[0].Right[0] = hatSql.TypedString("mutated")
	fresh := join.Rows()
	if got, want := fresh[0].Left[0].String, "red"; got != want {
		t.Fatalf("fresh left value = %q, want %q", got, want)
	}
	if got, want := fresh[0].Right[0].String, "red"; got != want {
		t.Fatalf("fresh right value = %q, want %q", got, want)
	}
}

func TestTypedTableJoinKeepsDelimiterLikeKeysDistinct(t *testing.T) {
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "left_delimited", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "right_delimited", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a:bc", "ab:c"} {
		if _, err := left.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
			t.Fatal(err)
		}
		if _, err := right.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
			t.Fatal(err)
		}
	}
	join, err := hatSql.NewTypedTableJoin(left, right, hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		t.Fatal(err)
	}
	assertTypedTableJoinPairs(t, join.Rows(), "a:bc/a:bc", "a:bc/ab:c", "ab:c/a:bc", "ab:c/ab:c")
}

func TestTypedTableJoinCoalescesConsecutiveChangesForOneKey(t *testing.T) {
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "scores_coalesced", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "people_coalesced", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := left.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("blue")}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("u", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}
	join, err := hatSql.NewTypedTableJoin(left, right, hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := left.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("red")})
	if err != nil {
		t.Fatal(err)
	}
	last, err := left.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("blue")})
	if err != nil {
		t.Fatal(err)
	}
	if err := join.ApplyLeft([]hatSql.TypedTableChange{first, last}); err != nil {
		t.Fatal(err)
	}
	if join.LeftCheckpoint() != last.Sequence {
		t.Fatalf("checkpoint = %d, want %d", join.LeftCheckpoint(), last.Sequence)
	}
	assertTypedTableJoinPairs(t, join.Rows())
}

func TestTypedTableJoinCoalescesConsecutiveRightChangesForOneKey(t *testing.T) {
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "scores_coalesced_right", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "people_coalesced_right", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := left.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("u", []hatSql.TypedTableValue{hatSql.TypedString("blue")}); err != nil {
		t.Fatal(err)
	}
	join, err := hatSql.NewTypedTableJoin(left, right, hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := right.Upsert("u", []hatSql.TypedTableValue{hatSql.TypedString("red")})
	if err != nil {
		t.Fatal(err)
	}
	last, err := right.Upsert("u", []hatSql.TypedTableValue{hatSql.TypedString("blue")})
	if err != nil {
		t.Fatal(err)
	}
	if err := join.ApplyRight([]hatSql.TypedTableChange{first, last}); err != nil {
		t.Fatal(err)
	}
	if join.RightCheckpoint() != last.Sequence {
		t.Fatalf("checkpoint = %d, want %d", join.RightCheckpoint(), last.Sequence)
	}
	assertTypedTableJoinPairs(t, join.Rows())
}

func TestTypedTableJoinCoalescedRunRetainsValidPrefixBeforeSequenceGap(t *testing.T) {
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "scores_coalesced_gap", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "people_coalesced_gap", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := left.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("blue")}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("u", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}
	join, err := hatSql.NewTypedTableJoin(left, right, hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := left.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("red")})
	if err != nil {
		t.Fatal(err)
	}
	invalid := first
	invalid.Sequence += 2
	if err := join.ApplyLeft([]hatSql.TypedTableChange{first, invalid}); err == nil {
		t.Fatal("ApplyLeft() error = nil, want sequence gap")
	}
	if join.LeftCheckpoint() != first.Sequence {
		t.Fatalf("checkpoint = %d, want %d", join.LeftCheckpoint(), first.Sequence)
	}
	assertTypedTableJoinPairs(t, join.Rows(), "a/u")
}

func TestTypedTableJoinDoesNotMatchNullKeys(t *testing.T) {
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "left_null_keys", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "right_null_keys", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := left.Upsert("left-null", []hatSql.TypedTableValue{hatSql.TypedNull()}); err != nil {
		t.Fatal(err)
	}
	if _, err := left.Upsert("left-red", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("right-null", []hatSql.TypedTableValue{hatSql.TypedNull()}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("right-red", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}

	join, err := hatSql.NewTypedTableJoin(left, right, hatSql.TypedTableJoinDefinition{
		LeftField:  "team",
		RightField: "team",
	})
	if err != nil {
		t.Fatalf("NewTypedTableJoin() error = %v", err)
	}

	rows := join.Rows()
	if len(rows) != 1 || rows[0].LeftKey != "left-red" || rows[0].RightKey != "right-red" {
		t.Fatalf("Rows() = %#v, want only the non-null pair", rows)
	}
}

func TestTypedTableJoinFloatKeysMatchSQLScalarEquality(t *testing.T) {
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "left_float_keys", Columns: []hatSql.TypedTableColumn{{Name: "value", Kind: hatSql.TypedTableFloat64}}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "right_float_keys", Columns: []hatSql.TypedTableColumn{{Name: "value", Kind: hatSql.TypedTableFloat64}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := left.Upsert("left-zero", []hatSql.TypedTableValue{hatSql.TypedFloat64(math.Copysign(0, -1))}); err != nil {
		t.Fatal(err)
	}
	if _, err := left.Upsert("left-nan", []hatSql.TypedTableValue{hatSql.TypedFloat64(math.NaN())}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("right-zero", []hatSql.TypedTableValue{hatSql.TypedFloat64(0)}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("right-nan", []hatSql.TypedTableValue{hatSql.TypedFloat64(math.NaN())}); err != nil {
		t.Fatal(err)
	}

	join, err := hatSql.NewTypedTableJoin(left, right, hatSql.TypedTableJoinDefinition{LeftField: "value", RightField: "value"})
	if err != nil {
		t.Fatal(err)
	}
	assertTypedTableJoinPairs(t, join.Rows(), "left-zero/right-zero")
}

func TestTypedTableJoinArrangementsShareOneExactJoin(t *testing.T) {
	left, _ := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "scores", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	right, _ := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "people", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if _, err := left.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil { t.Fatal(err) }
	if _, err := right.Upsert("u", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil { t.Fatal(err) }
	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil { t.Fatal(err) }
	definition := hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"}
	first, err := arrangements.Acquire(definition)
	if err != nil { t.Fatal(err) }
	second, err := arrangements.Acquire(definition)
	if err != nil { t.Fatal(err) }
	if arrangements.Active() != 1 { t.Fatalf("active arrangements = %d", arrangements.Active()) }
	change, err := left.Upsert("b", []hatSql.TypedTableValue{hatSql.TypedString("red")})
	if err != nil { t.Fatal(err) }
	if err := first.ApplyLeft([]hatSql.TypedTableChange{change}); err != nil { t.Fatal(err) }
	assertTypedTableJoinPairs(t, second.Rows(), "a/u", "b/u")
	firstReleased := first.Release()
	secondReleased := second.Release()
	if !firstReleased || !secondReleased || arrangements.Active() != 0 { t.Fatalf("release/active = %t/%t/%d", firstReleased, secondReleased, arrangements.Active()) }
}

func TestTypedTableJoinArrangementReleaseIsConcurrentSafe(t *testing.T) {
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "scores", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "people", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := arrangements.Acquire(hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan bool, 2)
	var group sync.WaitGroup
	group.Add(2)
	for range 2 {
		go func() {
			defer group.Done()
			results <- lease.Release()
		}()
	}
	group.Wait()
	close(results)
	released := 0
	for result := range results {
		if result {
			released++
		}
	}
	if released != 1 || arrangements.Active() != 0 {
		t.Fatalf("released/active = %d/%d, want 1/0", released, arrangements.Active())
	}
}

func assertTypedTableJoinPairs(t *testing.T, rows []hatSql.TypedTableJoinRow, want ...string) {
	t.Helper()
	if len(rows) != len(want) {
		t.Fatalf("join rows = %#v, want %v", rows, want)
	}
	got := make(map[string]bool, len(rows))
	for _, row := range rows {
		got[row.LeftKey+"/"+row.RightKey] = true
	}
	for _, pair := range want {
		if !got[pair] {
			t.Fatalf("join pairs = %#v, want %q", got, pair)
		}
	}
}
