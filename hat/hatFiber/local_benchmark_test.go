package hatFiber

import (
	"context"
	"runtime"
	"testing"
)

func BenchmarkT032FiberLocalSetGet(b *testing.B) {
	scheduler := &Scheduler{}
	fiber := &fiber{id: 1, ctx: context.Background()}
	fiber.localID.Store(1)
	ctx := Context{scheduler: scheduler, fiberState: fiber, id: 1}
	local := NewLocal[int]()
	if err := local.Set(ctx, 1); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := local.Set(ctx, index); err != nil {
			b.Fatal(err)
		}
		if _, ok := local.Get(ctx); !ok {
			b.Fatal("local value missing")
		}
	}
}

func BenchmarkT032FiberLocalGet(b *testing.B) {
	scheduler := &Scheduler{}
	fiber := &fiber{id: 1, ctx: context.Background()}
	fiber.localID.Store(1)
	ctx := Context{scheduler: scheduler, fiberState: fiber, id: 1}
	local := NewLocal[int]()
	if err := local.Set(ctx, 1); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var got int
	for index := 0; index < b.N; index++ {
		value, ok := local.Get(ctx)
		if !ok {
			b.Fatal("local value missing")
		}
		got += value + index
	}
	runtime.KeepAlive(got)
}
