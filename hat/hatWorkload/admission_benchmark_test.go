package hatWorkload_test

import (
	"context"
	"sync/atomic"
	"testing"

	"hatrie_cache/hat/hatWorkload"
)

func BenchmarkDirectWorkloadCounter(b *testing.B) {
	var counter atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		counter.Add(1)
	}
}

func BenchmarkAdmissionAcquireRelease(b *testing.B) {
	controller, err := hatWorkload.NewAdmissionController(hatWorkload.AdmissionOptions{MaxInFlight: 1})
	if err != nil {
		b.Fatal(err)
	}
	if err := controller.RegisterClass(hatWorkload.ClassOptions{Name: "interactive", Priority: 10, MaxInFlight: 1}); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		lease, acquireErr := controller.Acquire(ctx, "interactive")
		if acquireErr != nil {
			b.Fatal(acquireErr)
		}
		lease.Release()
	}
}

func BenchmarkAdmissionSnapshot(b *testing.B) {
	controller, err := hatWorkload.NewAdmissionController(hatWorkload.AdmissionOptions{})
	if err != nil {
		b.Fatal(err)
	}
	for _, class := range []hatWorkload.ClassOptions{
		{Name: "background", Priority: 1},
		{Name: "interactive", Priority: 10},
	} {
		if err := controller.RegisterClass(class); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = controller.Snapshot()
	}
}
