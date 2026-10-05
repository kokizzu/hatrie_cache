package hatStorage_test

import (
	"context"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

func BenchmarkCHU28AdaptiveCalibrationObserve(b *testing.B) {
	calibration, err := hatStorage.NewCompactionIOCalibration(hatStorage.CompactionIOCalibrationOptions{
		InitialBytesPerSecond: 1 << 30,
		MinBytesPerSecond:     1 << 20,
		MaxBytesPerSecond:     1 << 40,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := calibration.Observe(1<<20, time.Millisecond); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU28AdaptiveSchedulerRun(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		calibration, err := hatStorage.NewCompactionIOCalibration(hatStorage.CompactionIOCalibrationOptions{
			InitialBytesPerSecond: 1 << 40,
			MinBytesPerSecond:     1 << 20,
			MaxBytesPerSecond:     1 << 40,
		})
		if err != nil {
			b.Fatal(err)
		}
		scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{IOCalibration: calibration})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := scheduler.ScheduleWithIO("adaptive", 1, func(context.Context) error { return nil }); err != nil {
			b.Fatal(err)
		}
		if _, err := scheduler.Run(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
