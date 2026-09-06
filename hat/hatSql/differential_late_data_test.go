package hatSql

import (
	"errors"
	"reflect"
	"strconv"
	"testing"
)

func TestApplyDifferentialLateDataPolicyAcceptsAndClonesAllRows(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "early", Time: 1, Diff: 1, Row: Row{"value": "early"}},
		{Key: "boundary", Time: 5, Diff: -1, Row: Row{"value": "boundary"}},
		{Key: "late", Time: 4, Diff: 1, Row: Row{"value": "late"}},
	}
	got, err := ApplyDifferentialLateDataPolicy(rows, 5, DifferentialLateDataAccept)
	if err != nil {
		t.Fatalf("ApplyDifferentialLateDataPolicy() error = %v", err)
	}
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("got = %#v, want %#v", got, rows)
	}
	got[0].Row["value"] = "changed"
	if rows[0].Row["value"] != "early" {
		t.Fatal("accepted output aliases input row")
	}
}

func TestApplyDifferentialLateDataPolicyDropsOnlyRowsBeforeFrontier(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "late", Time: 4, Diff: 1, Row: Row{"value": "late"}},
		{Key: "boundary", Time: 5, Diff: 1, Row: Row{"value": "boundary"}},
		{Key: "on-time", Time: 6, Diff: -1, Row: Row{"value": "on-time"}},
	}
	got, err := ApplyDifferentialLateDataPolicy(rows, 5, DifferentialLateDataDrop)
	if err != nil {
		t.Fatalf("ApplyDifferentialLateDataPolicy() error = %v", err)
	}
	want := []DifferentialRow{rows[1], rows[2]}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got = %#v, want %#v", got, want)
	}
}

func TestApplyDifferentialLateDataPolicyRejectsWithoutPartialOutput(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "on-time", Time: 5, Diff: 1, Row: Row{"value": "on-time"}},
		{Key: "late", Time: 4, Diff: 1, Row: Row{"value": "late"}},
	}
	got, err := ApplyDifferentialLateDataPolicy(rows, 5, DifferentialLateDataReject)
	if !errors.Is(err, ErrDifferentialLateDataRejected) {
		t.Fatalf("error = %v, want ErrDifferentialLateDataRejected", err)
	}
	if got != nil {
		t.Fatalf("got partial output = %#v, want nil", got)
	}
}

func TestApplyDifferentialLateDataPolicyValidatesPolicyAndEmptyInput(t *testing.T) {
	if _, err := ApplyDifferentialLateDataPolicy(nil, 1, DifferentialLateDataPolicy(99)); !errors.Is(err, ErrDifferentialLateDataPolicyInvalid) {
		t.Fatalf("invalid policy error = %v, want ErrDifferentialLateDataPolicyInvalid", err)
	}
	got, err := ApplyDifferentialLateDataPolicy(nil, 1, DifferentialLateDataAccept)
	if err != nil {
		t.Fatalf("empty input error = %v", err)
	}
	if got != nil {
		t.Fatalf("got = %#v, want nil", got)
	}
}

func BenchmarkApplyDifferentialLateDataPolicy(b *testing.B) {
	rows := make([]DifferentialRow, 1024)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  strconv.Itoa(index),
			Time: uint64(index % 512),
			Diff: 1,
			Row:  Row{"value": index},
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := ApplyDifferentialLateDataPolicy(rows, 256, DifferentialLateDataDrop); err != nil {
			b.Fatal(err)
		}
	}
}
