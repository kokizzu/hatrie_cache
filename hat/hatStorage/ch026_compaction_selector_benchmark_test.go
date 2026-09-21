package hatStorage

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkCH026CompactionSelector(b *testing.B) {
	names := make([]string, 64)
	estimates := make([]uint64, len(names))
	for index := range names {
		names[index] = "partition-" + strconv.Itoa(index)
		estimates[index] = uint64(1) << uint(index%12)
	}
	run := func(context.Context) error { return nil }

	for _, test := range []struct {
		name   string
		policy CompactionSelectionPolicy
	}{
		{name: "priority", policy: CompactionSelectionPriority},
		{name: "size-tiered", policy: CompactionSelectionSizeTiered},
		{name: "time-aware", policy: CompactionSelectionTimeAware},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{
					MaxConcurrent:   4,
					SelectionPolicy: test.policy,
				})
				if err != nil {
					b.Fatal(err)
				}
				for index, name := range names {
					if _, err := scheduler.ScheduleWithPriorityAndIO(name, index%4+1, estimates[index], run); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := scheduler.Run(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
