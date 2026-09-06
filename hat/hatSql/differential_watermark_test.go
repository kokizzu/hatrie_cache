package hatSql

import (
	"errors"
	"reflect"
	"strconv"
	"testing"
)

func TestDifferentialWatermarkAdvancesMonotonically(t *testing.T) {
	watermark, err := NewDifferentialWatermark(DifferentialLateDataReject)
	if err != nil {
		t.Fatalf("NewDifferentialWatermark() error = %v", err)
	}
	if got := watermark.Frontier(); got != 0 {
		t.Fatalf("initial Frontier() = %d, want 0", got)
	}
	if err := watermark.Advance(5); err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	if err := watermark.Advance(5); err != nil {
		t.Fatalf("idempotent Advance() error = %v", err)
	}
	if got := watermark.Frontier(); got != 5 {
		t.Fatalf("Frontier() = %d, want 5", got)
	}
	if err := watermark.Advance(4); !errors.Is(err, ErrDifferentialWatermarkRegression) {
		t.Fatalf("regression error = %v, want ErrDifferentialWatermarkRegression", err)
	}
	if got := watermark.Frontier(); got != 5 {
		t.Fatalf("Frontier() after regression = %d, want 5", got)
	}
}

func TestDifferentialWatermarkAppliesConfiguredLateDataPolicy(t *testing.T) {
	rows := []DifferentialRow{
		{Key: "late", Time: 4, Diff: 1, Row: Row{"value": "late"}},
		{Key: "boundary", Time: 5, Diff: 1, Row: Row{"value": "boundary"}},
	}
	watermark, err := NewDifferentialWatermark(DifferentialLateDataDrop)
	if err != nil {
		t.Fatalf("NewDifferentialWatermark() error = %v", err)
	}
	if err := watermark.Advance(5); err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	got, err := watermark.Apply(rows)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if !reflect.DeepEqual(got, []DifferentialRow{rows[1]}) {
		t.Fatalf("got = %#v, want %#v", got, []DifferentialRow{rows[1]})
	}

	reject, err := NewDifferentialWatermark(DifferentialLateDataReject)
	if err != nil {
		t.Fatalf("NewDifferentialWatermark() error = %v", err)
	}
	if err := reject.Advance(5); err != nil {
		t.Fatalf("reject Advance() error = %v", err)
	}
	if _, err := reject.Apply(rows); !errors.Is(err, ErrDifferentialLateDataRejected) {
		t.Fatalf("reject Apply() error = %v, want ErrDifferentialLateDataRejected", err)
	}
}

func TestDifferentialWatermarkValidatesPolicyAndNilReceiver(t *testing.T) {
	if _, err := NewDifferentialWatermark(DifferentialLateDataPolicy(99)); !errors.Is(err, ErrDifferentialLateDataPolicyInvalid) {
		t.Fatalf("invalid policy error = %v, want ErrDifferentialLateDataPolicyInvalid", err)
	}
	var watermark *DifferentialWatermark
	if err := watermark.Advance(1); !errors.Is(err, ErrDifferentialWatermarkNil) {
		t.Fatalf("nil Advance() error = %v, want ErrDifferentialWatermarkNil", err)
	}
	if _, err := watermark.Apply(nil); !errors.Is(err, ErrDifferentialWatermarkNil) {
		t.Fatalf("nil Apply() error = %v, want ErrDifferentialWatermarkNil", err)
	}
	if got := watermark.Frontier(); got != 0 {
		t.Fatalf("nil Frontier() = %d, want 0", got)
	}
}

func TestDifferentialWatermarkHandlesEmptyBatch(t *testing.T) {
	watermark, err := NewDifferentialWatermark(DifferentialLateDataAccept)
	if err != nil {
		t.Fatalf("NewDifferentialWatermark() error = %v", err)
	}
	got, err := watermark.Apply(nil)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if got != nil {
		t.Fatalf("got = %#v, want nil", got)
	}
}

func BenchmarkDifferentialWatermarkApply(b *testing.B) {
	rows := make([]DifferentialRow, 1024)
	for index := range rows {
		rows[index] = DifferentialRow{
			Key:  strconv.Itoa(index),
			Time: uint64(index % 512),
			Diff: 1,
			Row:  Row{"value": index},
		}
	}
	watermark, err := NewDifferentialWatermark(DifferentialLateDataDrop)
	if err != nil {
		b.Fatal(err)
	}
	if err := watermark.Advance(256); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := watermark.Apply(rows); err != nil {
			b.Fatal(err)
		}
	}
}
