package hatSql_test

import (
	"errors"
	"math"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestMergeSortedTypedTableJoinEmitsDuplicateRunsAndOwnsOutput(t *testing.T) {
	left := []hatSql.TypedTableMergeJoinInput{
		{Key: "l1", Values: []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(1)}},
		{Key: "l2", Values: []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(2)}},
		{Key: "l3", Values: []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(3)}},
	}
	right := []hatSql.TypedTableMergeJoinInput{
		{Key: "r1", Values: []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedString("Ada")}},
		{Key: "r2", Values: []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedString("Lin")}},
		{Key: "r3", Values: []hatSql.TypedTableValue{hatSql.TypedString("yellow"), hatSql.TypedString("Mia")}},
	}

	var got []string
	err := hatSql.MergeSortedTypedTableJoin(left, right, 0, 0, func(row hatSql.TypedTableJoinRow) error {
		got = append(got, row.LeftKey+"/"+row.RightKey)
		row.Left[0] = hatSql.TypedString("mutated")
		row.Right[0] = hatSql.TypedString("mutated")
		return nil
	})
	if err != nil {
		t.Fatalf("MergeSortedTypedTableJoin() error = %v", err)
	}
	if want := []string{"l2/r1", "l2/r2", "l3/r1", "l3/r2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("join pairs = %v, want %v", got, want)
	}
	if left[1].Values[0].String != "red" || right[0].Values[0].String != "red" {
		t.Fatal("merge join exposed input row storage")
	}
}

func TestMergeSortedTypedTableJoinSkipsNullAndNaNKeys(t *testing.T) {
	left := []hatSql.TypedTableMergeJoinInput{
		{Key: "null", Values: []hatSql.TypedTableValue{hatSql.TypedNull()}},
		{Key: "red", Values: []hatSql.TypedTableValue{hatSql.TypedString("red")}},
	}
	right := []hatSql.TypedTableMergeJoinInput{
		{Key: "null", Values: []hatSql.TypedTableValue{hatSql.TypedNull()}},
		{Key: "red", Values: []hatSql.TypedTableValue{hatSql.TypedString("red")}},
	}
	var matches int
	if err := hatSql.MergeSortedTypedTableJoin(left, right, 0, 0, func(hatSql.TypedTableJoinRow) error {
		matches++
		return nil
	}); err != nil {
		t.Fatalf("string merge join error = %v", err)
	}
	if matches != 1 {
		t.Fatalf("string matches = %d, want 1", matches)
	}

	left = []hatSql.TypedTableMergeJoinInput{
		{Key: "nan", Values: []hatSql.TypedTableValue{hatSql.TypedFloat64(math.NaN())}},
		{Key: "one", Values: []hatSql.TypedTableValue{hatSql.TypedFloat64(1)}},
	}
	right = []hatSql.TypedTableMergeJoinInput{
		{Key: "nan", Values: []hatSql.TypedTableValue{hatSql.TypedFloat64(math.NaN())}},
		{Key: "one", Values: []hatSql.TypedTableValue{hatSql.TypedFloat64(1)}},
	}
	matches = 0
	if err := hatSql.MergeSortedTypedTableJoin(left, right, 0, 0, func(hatSql.TypedTableJoinRow) error {
		matches++
		return nil
	}); err != nil {
		t.Fatalf("float merge join error = %v", err)
	}
	if matches != 1 {
		t.Fatalf("float matches = %d, want 1", matches)
	}
}

func TestMergeSortedTypedTableJoinRejectsInvalidInputs(t *testing.T) {
	valid := []hatSql.TypedTableMergeJoinInput{{Key: "a", Values: []hatSql.TypedTableValue{hatSql.TypedString("a")}}}
	unsorted := []hatSql.TypedTableMergeJoinInput{
		{Key: "z", Values: []hatSql.TypedTableValue{hatSql.TypedString("z")}},
		{Key: "a", Values: []hatSql.TypedTableValue{hatSql.TypedString("a")}},
	}
	if err := hatSql.MergeSortedTypedTableJoin(unsorted, valid, 0, 0, func(hatSql.TypedTableJoinRow) error { return nil }); !errors.Is(err, hatSql.ErrTypedTableMergeJoinUnsorted) {
		t.Fatalf("unsorted error = %v, want ErrTypedTableMergeJoinUnsorted", err)
	}
	if err := hatSql.MergeSortedTypedTableJoin(valid, valid, -1, 0, func(hatSql.TypedTableJoinRow) error { return nil }); !errors.Is(err, hatSql.ErrTypedTableMergeJoinField) {
		t.Fatalf("field error = %v, want ErrTypedTableMergeJoinField", err)
	}
	mismatched := []hatSql.TypedTableMergeJoinInput{{Key: "a", Values: []hatSql.TypedTableValue{hatSql.TypedInt64(1)}}}
	if err := hatSql.MergeSortedTypedTableJoin(valid, mismatched, 0, 0, func(hatSql.TypedTableJoinRow) error { return nil }); !errors.Is(err, hatSql.ErrTypedTableMergeJoinTypeMismatch) {
		t.Fatalf("type error = %v, want ErrTypedTableMergeJoinTypeMismatch", err)
	}
	if err := hatSql.MergeSortedTypedTableJoin(nil, nil, 0, 0, nil); !errors.Is(err, hatSql.ErrTypedTableMergeJoinVisitor) {
		t.Fatalf("visitor error = %v, want ErrTypedTableMergeJoinVisitor", err)
	}
}

func TestMergeSortedTypedTableJoinStopsOnVisitorError(t *testing.T) {
	left := []hatSql.TypedTableMergeJoinInput{{Key: "l", Values: []hatSql.TypedTableValue{hatSql.TypedString("red")}}}
	right := []hatSql.TypedTableMergeJoinInput{{Key: "r", Values: []hatSql.TypedTableValue{hatSql.TypedString("red")}}}
	want := errors.New("stop")
	calls := 0
	err := hatSql.MergeSortedTypedTableJoin(left, right, 0, 0, func(hatSql.TypedTableJoinRow) error {
		calls++
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("visitor error = %v, want %v", err, want)
	}
	if calls != 1 {
		t.Fatalf("visitor calls = %d, want 1", calls)
	}
}

func TestMergeSortedTypedTableJoinOrdersNumericAndBooleanKeys(t *testing.T) {
	left := []hatSql.TypedTableMergeJoinInput{
		{Key: "negative", Values: []hatSql.TypedTableValue{hatSql.TypedInt64(-1)}},
		{Key: "zero", Values: []hatSql.TypedTableValue{hatSql.TypedInt64(0)}},
		{Key: "positive", Values: []hatSql.TypedTableValue{hatSql.TypedInt64(1)}},
	}
	right := []hatSql.TypedTableMergeJoinInput{
		{Key: "right-negative", Values: []hatSql.TypedTableValue{hatSql.TypedInt64(-1)}},
		{Key: "right-positive", Values: []hatSql.TypedTableValue{hatSql.TypedInt64(1)}},
	}
	var got []string
	if err := hatSql.MergeSortedTypedTableJoin(left, right, 0, 0, func(row hatSql.TypedTableJoinRow) error {
		got = append(got, row.LeftKey+"/"+row.RightKey)
		return nil
	}); err != nil {
		t.Fatalf("integer merge join error = %v", err)
	}
	if want := []string{"negative/right-negative", "positive/right-positive"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("integer join pairs = %v, want %v", got, want)
	}

	left = []hatSql.TypedTableMergeJoinInput{
		{Key: "negative-zero", Values: []hatSql.TypedTableValue{hatSql.TypedFloat64(math.Copysign(0, -1))}},
		{Key: "one", Values: []hatSql.TypedTableValue{hatSql.TypedFloat64(1)}},
	}
	right = []hatSql.TypedTableMergeJoinInput{
		{Key: "positive-zero", Values: []hatSql.TypedTableValue{hatSql.TypedFloat64(0)}},
		{Key: "one-right", Values: []hatSql.TypedTableValue{hatSql.TypedFloat64(1)}},
	}
	got = nil
	if err := hatSql.MergeSortedTypedTableJoin(left, right, 0, 0, func(row hatSql.TypedTableJoinRow) error {
		got = append(got, row.LeftKey+"/"+row.RightKey)
		return nil
	}); err != nil {
		t.Fatalf("float merge join error = %v", err)
	}
	if want := []string{"negative-zero/positive-zero", "one/one-right"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("float join pairs = %v, want %v", got, want)
	}

	left = []hatSql.TypedTableMergeJoinInput{
		{Key: "false", Values: []hatSql.TypedTableValue{hatSql.TypedBool(false)}},
		{Key: "true", Values: []hatSql.TypedTableValue{hatSql.TypedBool(true)}},
	}
	right = []hatSql.TypedTableMergeJoinInput{
		{Key: "true-right", Values: []hatSql.TypedTableValue{hatSql.TypedBool(true)}},
	}
	got = nil
	if err := hatSql.MergeSortedTypedTableJoin(left, right, 0, 0, func(row hatSql.TypedTableJoinRow) error {
		got = append(got, row.LeftKey+"/"+row.RightKey)
		return nil
	}); err != nil {
		t.Fatalf("boolean merge join error = %v", err)
	}
	if want := []string{"true/true-right"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("boolean join pairs = %v, want %v", got, want)
	}
}
