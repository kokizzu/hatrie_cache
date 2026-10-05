package hatStorage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

func TestCHU28AdaptiveCalibrationConvergesWithinBounds(t *testing.T) {
	calibration, err := hatStorage.NewCompactionIOCalibration(hatStorage.CompactionIOCalibrationOptions{
		MinBytesPerSecond: 100,
		MaxBytesPerSecond: 1_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := calibration.Observe(1_000, time.Second); err != nil || got != 1_000 {
		t.Fatalf("first Observe() = %d, %v, want 1000, nil", got, err)
	}
	got, err := calibration.Observe(1_000, 10*time.Second)
	if err != nil {
		t.Fatalf("second Observe() error = %v", err)
	}
	if got != 775 {
		t.Fatalf("smoothed rate = %d, want 775", got)
	}
	snapshot := calibration.Snapshot()
	if snapshot.SampleCount != 2 || snapshot.LastObservedBytesPerSecond != 100 || snapshot.ObservedBytes != 2_000 {
		t.Fatalf("calibration snapshot = %#v", snapshot)
	}
	calibration.Reset()
	if got := calibration.BytesPerSecond(); got != 0 {
		t.Fatalf("rate after Reset() = %d, want 0", got)
	}
}

func TestCHU28AdaptiveCalibrationValidatesOptionsAndObservations(t *testing.T) {
	if _, err := hatStorage.NewCompactionIOCalibration(hatStorage.CompactionIOCalibrationOptions{
		MinBytesPerSecond: 10,
		MaxBytesPerSecond: 9,
	}); !errors.Is(err, hatStorage.ErrCompactionIOCalibrationOptionsInvalid) {
		t.Fatalf("invalid options error = %v", err)
	}
	calibration, err := hatStorage.NewCompactionIOCalibration(hatStorage.CompactionIOCalibrationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := calibration.Observe(0, time.Second); !errors.Is(err, hatStorage.ErrCompactionIOCalibrationObservationInvalid) {
		t.Fatalf("zero-byte observation error = %v", err)
	}
	if _, err := calibration.Observe(1, 0); !errors.Is(err, hatStorage.ErrCompactionIOCalibrationObservationInvalid) {
		t.Fatalf("zero-duration observation error = %v", err)
	}
	var nilCalibration *hatStorage.CompactionIOCalibration
	if _, err := nilCalibration.Observe(1, time.Second); !errors.Is(err, hatStorage.ErrCompactionIOCalibrationNil) {
		t.Fatalf("nil calibration error = %v", err)
	}
}

func TestCHU28AdaptiveCalibrationFeedsSchedulerWithoutChangingDefault(t *testing.T) {
	calibration, err := hatStorage.NewCompactionIOCalibration(hatStorage.CompactionIOCalibrationOptions{
		InitialBytesPerSecond: 1 << 30,
		MinBytesPerSecond:     1 << 20,
		MaxBytesPerSecond:     1 << 40,
	})
	if err != nil {
		t.Fatal(err)
	}
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{IOCalibration: calibration})
	if err != nil {
		t.Fatal(err)
	}
	if got := scheduler.Stats().IOBytesPerSecond; got != 1<<30 {
		t.Fatalf("initial scheduler IO rate = %d, want %d", got, 1<<30)
	}
	if _, err := scheduler.ScheduleWithIO("adaptive", 1<<20, func(context.Context) error {
		time.Sleep(time.Millisecond)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := calibration.Snapshot()
	if snapshot.SampleCount != 1 || snapshot.LastObservedBytesPerSecond == 0 {
		t.Fatalf("post-run calibration snapshot = %#v", snapshot)
	}
	if got := scheduler.Stats().IOBytesPerSecond; got != snapshot.BytesPerSecond {
		t.Fatalf("scheduler IO rate = %d, calibration rate = %d", got, snapshot.BytesPerSecond)
	}

	defaultScheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stats := defaultScheduler.Stats(); stats.IOBytesPerSecond != 0 || stats.IOThrottledTaskCount != 0 || stats.IOWaitNanoseconds != 0 {
		t.Fatalf("default scheduler IO stats = %#v, want zero", stats)
	}
}
