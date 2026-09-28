package hatWorkload

import (
	"context"
	"testing"
)

func BenchmarkC231MemoryAdmissionAcquireRelease(b *testing.B) {
	controller, err := NewMemoryAdmissionController(AdmissionOptions{MaxInFlight: 1})
	if err != nil {
		b.Fatal(err)
	}
	if err := controller.RegisterClass(MemoryClassOptions{
		ClassOptions:   ClassOptions{Name: "default"},
		MaxMemoryBytes: 1 << 20,
	}); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lease, err := controller.Acquire(ctx, "default", 64)
		if err != nil {
			b.Fatal(err)
		}
		lease.Release()
	}
}
