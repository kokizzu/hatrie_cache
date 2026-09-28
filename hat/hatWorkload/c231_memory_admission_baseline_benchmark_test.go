package hatWorkload

import (
	"context"
	"testing"
)

func BenchmarkC231ExistingAdmissionAcquireRelease(b *testing.B) {
	controller, err := NewAdmissionController(AdmissionOptions{MaxInFlight: 1})
	if err != nil {
		b.Fatal(err)
	}
	if err := controller.RegisterClass(ClassOptions{Name: "default"}); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lease, err := controller.Acquire(ctx, "default")
		if err != nil {
			b.Fatal(err)
		}
		lease.Release()
	}
}
