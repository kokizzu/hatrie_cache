package hatSql

import (
	"errors"
	"sync"
	"testing"
)

func TestSQLRowBinaryCodecAccountingMeasuresSuccessfulOperations(t *testing.T) {
	var accounting SQLRowBinaryCodecAccounting

	encoded, err := accounting.MeasureEncode(100, func() ([]byte, error) {
		return make([]byte, 25), nil
	})
	if err != nil {
		t.Fatalf("MeasureEncode() error = %v", err)
	}
	if len(encoded) != 25 {
		t.Fatalf("encoded length = %d, want 25", len(encoded))
	}

	if err := accounting.MeasureDecode(encoded, 100, func(data []byte) error {
		if len(data) != 25 {
			t.Fatalf("decoded input length = %d, want 25", len(data))
		}
		return nil
	}); err != nil {
		t.Fatalf("MeasureDecode() error = %v", err)
	}

	snapshot := accounting.Snapshot()
	if snapshot.LogicalBytes != 200 {
		t.Errorf("LogicalBytes = %d, want 200", snapshot.LogicalBytes)
	}
	if snapshot.EncodedBytes != 50 {
		t.Errorf("EncodedBytes = %d, want 50", snapshot.EncodedBytes)
	}
	if snapshot.EncodeOperations != 1 {
		t.Errorf("EncodeOperations = %d, want 1", snapshot.EncodeOperations)
	}
	if snapshot.DecodeOperations != 1 {
		t.Errorf("DecodeOperations = %d, want 1", snapshot.DecodeOperations)
	}
	if snapshot.EncodeNanoseconds == 0 {
		t.Error("EncodeNanoseconds = 0, want a recorded duration")
	}
	if snapshot.DecodeNanoseconds == 0 {
		t.Error("DecodeNanoseconds = 0, want a recorded duration")
	}
	if snapshot.CompressionRatio != 4 {
		t.Errorf("CompressionRatio = %v, want 4", snapshot.CompressionRatio)
	}
}

func TestSQLRowBinaryCodecAccountingTracksFailedOperationsWithoutBytes(t *testing.T) {
	var accounting SQLRowBinaryCodecAccounting
	wantErr := errors.New("codec failed")

	if _, err := accounting.MeasureEncode(100, func() ([]byte, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("MeasureEncode() error = %v, want %v", err, wantErr)
	}
	if err := accounting.MeasureDecode([]byte{1, 2}, 100, func([]byte) error {
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("MeasureDecode() error = %v, want %v", err, wantErr)
	}

	snapshot := accounting.Snapshot()
	if snapshot.EncodeOperations != 1 || snapshot.DecodeOperations != 1 {
		t.Fatalf("operation counts = %d/%d, want 1/1", snapshot.EncodeOperations, snapshot.DecodeOperations)
	}
	if snapshot.LogicalBytes != 0 || snapshot.EncodedBytes != 0 {
		t.Fatalf("failed-operation bytes = %d/%d, want 0/0", snapshot.LogicalBytes, snapshot.EncodedBytes)
	}
	if snapshot.EncodeNanoseconds == 0 || snapshot.DecodeNanoseconds == 0 {
		t.Fatal("failed operations did not record duration")
	}
}

func TestSQLRowBinaryCodecAccountingRejectsInvalidInputs(t *testing.T) {
	var accounting SQLRowBinaryCodecAccounting

	if _, err := accounting.MeasureEncode(-1, func() ([]byte, error) { return nil, nil }); !errors.Is(err, ErrSQLRowBinaryCodecAccountingNegativeBytes) {
		t.Errorf("negative encode bytes error = %v", err)
	}
	if _, err := accounting.MeasureEncode(1, nil); !errors.Is(err, ErrSQLRowBinaryCodecAccountingNilCallback) {
		t.Errorf("nil encode callback error = %v", err)
	}
	if err := accounting.MeasureDecode(nil, -1, func([]byte) error { return nil }); !errors.Is(err, ErrSQLRowBinaryCodecAccountingNegativeBytes) {
		t.Errorf("negative decode bytes error = %v", err)
	}
	if err := accounting.MeasureDecode(nil, 1, nil); !errors.Is(err, ErrSQLRowBinaryCodecAccountingNilCallback) {
		t.Errorf("nil decode callback error = %v", err)
	}
}

func TestSQLRowBinaryCodecAccountingReset(t *testing.T) {
	var accounting SQLRowBinaryCodecAccounting
	if _, err := accounting.MeasureEncode(10, func() ([]byte, error) { return []byte{1}, nil }); err != nil {
		t.Fatalf("MeasureEncode() error = %v", err)
	}

	accounting.Reset()
	snapshot := accounting.Snapshot()
	if snapshot != (SQLRowBinaryCodecAccountingSnapshot{}) {
		t.Fatalf("Snapshot() after Reset() = %+v, want zero", snapshot)
	}
}

func TestSQLRowBinaryCodecAccountingConcurrent(t *testing.T) {
	var accounting SQLRowBinaryCodecAccounting
	const workers = 8
	const operations = 100

	var waitGroup sync.WaitGroup
	waitGroup.Add(workers)
	for range workers {
		go func() {
			defer waitGroup.Done()
			for range operations {
				encoded, err := accounting.MeasureEncode(4, func() ([]byte, error) {
					return []byte{1, 2}, nil
				})
				if err != nil {
					t.Errorf("MeasureEncode() error = %v", err)
					return
				}
				if err := accounting.MeasureDecode(encoded, 4, func([]byte) error { return nil }); err != nil {
					t.Errorf("MeasureDecode() error = %v", err)
					return
				}
			}
		}()
	}
	waitGroup.Wait()

	snapshot := accounting.Snapshot()
	wantOperations := uint64(workers * operations)
	if snapshot.EncodeOperations != wantOperations || snapshot.DecodeOperations != wantOperations {
		t.Fatalf("operation counts = %d/%d, want %d/%d", snapshot.EncodeOperations, snapshot.DecodeOperations, wantOperations, wantOperations)
	}
	if snapshot.LogicalBytes != wantOperations*8 || snapshot.EncodedBytes != wantOperations*4 {
		t.Fatalf("byte counts = %d/%d, want %d/%d", snapshot.LogicalBytes, snapshot.EncodedBytes, wantOperations*8, wantOperations*4)
	}
}

func BenchmarkSQLRowBinaryCodecAccounting(b *testing.B) {
	encoded := make([]byte, 1024)
	var accounting SQLRowBinaryCodecAccounting
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		data, err := accounting.MeasureEncode(4096, func() ([]byte, error) {
			return encoded, nil
		})
		if err != nil {
			b.Fatal(err)
		}
		if err := accounting.MeasureDecode(data, 4096, func([]byte) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLRowBinaryCodecAccountingBaseline(b *testing.B) {
	encoded := make([]byte, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if len(encoded) != 1024 {
			b.Fatal("unexpected encoded length")
		}
	}
}
