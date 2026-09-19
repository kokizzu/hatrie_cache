package hatSql

import (
	"context"
	"testing"
)

var chu39AdmissionValues [64]int64
var chu39AdmissionContext = context.Background()
var chu39AdmissionSink int64

func init() {
	for index := range chu39AdmissionValues {
		chu39AdmissionValues[index] = int64(index)
	}
}

func chu39AdmissionWork(context.Context) error {
	var total int64
	for _, value := range chu39AdmissionValues {
		total += value
	}
	chu39AdmissionSink = total
	return nil
}

func BenchmarkCHU39AdmissionDirect(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = chu39AdmissionWork(context.Background())
	}
}

func BenchmarkCHU39AdmissionAcquire(b *testing.B) {
	admission, err := NewSQLWorkloadAdmission(SQLWorkloadAdmissionOptions{MaxConcurrent: 1})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := admission.acquirePermit(chu39AdmissionContext, "default"); err != nil {
			b.Fatal(err)
		}
		admission.releaseOne()
	}
}

func BenchmarkCHU39AdmissionRun(b *testing.B) {
	admission, err := NewSQLWorkloadAdmission(SQLWorkloadAdmissionOptions{MaxConcurrent: 1})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := admission.Run(chu39AdmissionContext, "default", chu39AdmissionWork); err != nil {
			b.Fatal(err)
		}
	}
}
