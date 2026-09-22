package hatSql

import (
	"context"
	"testing"
)

func BenchmarkC231AdmissionAcquireWithMemory(b *testing.B) {
	admission, err := NewSQLWorkloadAdmission(SQLWorkloadAdmissionOptions{
		MaxConcurrent: 1,
		Classes: []SQLWorkloadClass{{
			Name:           "analytics",
			MaxConcurrent:  1,
			MaxMemoryBytes: 1 << 20,
		}},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer admission.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		release, err := admission.AcquireWithMemory(ctx, "analytics", 256)
		if err != nil {
			b.Fatal(err)
		}
		release()
	}
}

func BenchmarkC231AdmissionRunWithMemory(b *testing.B) {
	admission, err := NewSQLWorkloadAdmission(SQLWorkloadAdmissionOptions{
		MaxConcurrent: 1,
		Classes: []SQLWorkloadClass{{
			Name:           "analytics",
			MaxConcurrent:  1,
			MaxMemoryBytes: 1 << 20,
		}},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer admission.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := admission.RunWithMemory(ctx, "analytics", 256, chu39AdmissionWork); err != nil {
			b.Fatal(err)
		}
	}
}
