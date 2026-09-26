package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestGroupMinMaxStringDifferentialRowsEmitsLexicalEndpointChanges(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "pear-a", Time: 1, Diff: 2, Row: Row{"group": "fruit", "value": "pear"}},
		{Key: "apple", Time: 2, Diff: 1, Row: Row{"group": "fruit", "value": "apple"}},
		{Key: "apple", Time: 3, Diff: -1, Row: Row{"group": "fruit", "value": "apple"}},
		{Key: "pear-b", Time: 4, Diff: -1, Row: Row{"group": "fruit", "value": "pear"}},
		{Key: "pear-a", Time: 5, Diff: -1, Row: Row{"group": "fruit", "value": "pear"}},
	}
	got, err := GroupMinMaxStringDifferentialRows(rows,
		func(row SQLRow) string { return row["group"].(string) },
		func(row SQLRow) (string, error) { return row["value"].(string), nil },
	)
	if err != nil {
		t.Fatalf("GroupMinMaxStringDifferentialRows() error = %v", err)
	}
	want := []DifferentialRow{
		{Key: "fruit", Time: 1, Diff: 1, Row: Row{"min": "pear", "max": "pear"}},
		{Key: "fruit", Time: 2, Diff: -1, Row: Row{"min": "pear", "max": "pear"}},
		{Key: "fruit", Time: 2, Diff: 1, Row: Row{"min": "apple", "max": "pear"}},
		{Key: "fruit", Time: 3, Diff: -1, Row: Row{"min": "apple", "max": "pear"}},
		{Key: "fruit", Time: 3, Diff: 1, Row: Row{"min": "pear", "max": "pear"}},
		{Key: "fruit", Time: 5, Diff: -1, Row: Row{"min": "pear", "max": "pear"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
}

func TestGroupMinMaxStringDifferentialRowsSupportsEmptyStringAndAtomicErrors(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "empty", Time: 1, Diff: 1, Row: Row{"group": "g", "value": ""}},
		{Key: "z", Time: 2, Diff: 1, Row: Row{"group": "g", "value": "z"}},
	}
	got, err := GroupMinMaxStringDifferentialRows(rows,
		func(row SQLRow) string { return row["group"].(string) },
		func(row SQLRow) (string, error) { return row["value"].(string), nil },
	)
	if err != nil {
		t.Fatalf("GroupMinMaxStringDifferentialRows() error = %v", err)
	}
	want := Row{"min": "", "max": "z"}
	if !reflect.DeepEqual(got[len(got)-1].Row, want) {
		t.Fatalf("last aggregate = %#v, want %#v", got[len(got)-1].Row, want)
	}

	callbackErr := errors.New("string value callback failed")
	invalid, err := GroupMinMaxStringDifferentialRows(
		[]DifferentialRow{{Key: "bad", Diff: 1, Row: Row{"group": "g"}}},
		func(row SQLRow) string { return row["group"].(string) },
		func(SQLRow) (string, error) { return "", callbackErr },
	)
	if !errors.Is(err, callbackErr) || invalid != nil {
		t.Fatalf("callback failure = %#v, %v; want nil and callback error", invalid, err)
	}

	invalid, err = GroupMinMaxStringDifferentialRows(
		[]DifferentialRow{{Key: "missing", Diff: -1, Row: Row{"group": "g", "value": "x"}}},
		func(row SQLRow) string { return row["group"].(string) },
		func(row SQLRow) (string, error) { return row["value"].(string), nil },
	)
	if !errors.Is(err, ErrDifferentialGroupByNegativeCount) || invalid != nil {
		t.Fatalf("negative count = %#v, %v; want nil and negative-count error", invalid, err)
	}

	invalid, err = GroupMinMaxStringDifferentialRows(
		[]DifferentialRow{
			{Key: "present", Diff: 1, Row: Row{"group": "g", "value": "x"}},
			{Key: "other", Diff: 1, Row: Row{"group": "g", "value": "y"}},
			{Key: "underflow", Diff: -2, Row: Row{"group": "g", "value": "x"}},
		},
		func(row SQLRow) string { return row["group"].(string) },
		func(row SQLRow) (string, error) { return row["value"].(string), nil },
	)
	if !errors.Is(err, ErrDifferentialGroupByValueMultiplicity) || invalid != nil {
		t.Fatalf("value underflow = %#v, %v; want nil and value-multiplicity error", invalid, err)
	}
}

func TestGroupMinMaxStringDifferentialRowsRequiresCallbacks(t *testing.T) {
	if _, err := GroupMinMaxStringDifferentialRows(nil, nil, nil); !errors.Is(err, ErrDifferentialGroupByKeyRequired) {
		t.Fatalf("nil key error = %v, want ErrDifferentialGroupByKeyRequired", err)
	}
	if _, err := GroupMinMaxStringDifferentialRows(nil, func(SQLRow) string { return "g" }, nil); !errors.Is(err, ErrDifferentialGroupByValueRequired) {
		t.Fatalf("nil value error = %v, want ErrDifferentialGroupByValueRequired", err)
	}
}
