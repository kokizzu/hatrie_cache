package hatPipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMZ007FrontierBackpressureBoundsAdmissionAndAdvancesMonotonically(t *testing.T) {
	gate, err := NewFrontierBackpressure(FrontierBackpressureOptions{MaxLag: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, frontier := range []uint64{0, 1, 2} {
		admitted, err := gate.TryAdmit(frontier)
		if err != nil || !admitted {
			t.Fatalf("TryAdmit(%d) = admitted %v, err %v", frontier, admitted, err)
		}
	}
	if admitted, err := gate.TryAdmit(3); err != nil || admitted {
		t.Fatalf("TryAdmit(3) = admitted %v, err %v; want blocked", admitted, err)
	}
	if err := gate.AdvanceConsumed(1); err != nil {
		t.Fatal(err)
	}
	if admitted, err := gate.TryAdmit(3); err != nil || !admitted {
		t.Fatalf("TryAdmit(3) after consume = admitted %v, err %v; want admitted", admitted, err)
	}
	if err := gate.AdvanceConsumed(0); !errors.Is(err, ErrFrontierBackpressureRegression) {
		t.Fatalf("regressing AdvanceConsumed() error = %v", err)
	}
	stats := gate.Stats()
	if stats.Produced != 3 || stats.Consumed != 1 || stats.Lag != 2 || stats.BlockedAttempts != 1 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestMZ007FrontierBackpressureWaitWakesAndHonorsCancellation(t *testing.T) {
	gate, err := NewFrontierBackpressure(FrontierBackpressureOptions{MaxLag: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gate.Close)
	if admitted, err := gate.TryAdmit(1); err != nil || !admitted {
		t.Fatalf("initial TryAdmit() = admitted %v, err %v", admitted, err)
	}
	result := make(chan error, 1)
	go func() { result <- gate.Wait(context.Background(), 2) }()
	waitForMZ007FrontierBackpressure(t, gate)
	if err := gate.AdvanceConsumed(1); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("woken Wait() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait() did not wake after frontier advancement")
	}
	if gate.Stats().Blocked {
		t.Fatal("Stats().Blocked = true after Wait() completed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := gate.Wait(ctx, 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Wait() error = %v", err)
	}
}

func TestMZ007FrontierBackpressureRejectsNilContextAndSupportsZeroValue(t *testing.T) {
	var zero FrontierBackpressure
	if stats := zero.Stats(); stats.MaxLag != DefaultFrontierBackpressureMaxLag {
		t.Fatalf("zero-value MaxLag = %d", stats.MaxLag)
	}
	if admitted, err := zero.TryAdmit(DefaultFrontierBackpressureMaxLag); err != nil || !admitted {
		t.Fatalf("zero-value TryAdmit() = admitted %v, err %v", admitted, err)
	}
	if admitted, err := zero.TryAdmit(DefaultFrontierBackpressureMaxLag + 1); err != nil || admitted {
		t.Fatalf("zero-value over-limit TryAdmit() = admitted %v, err %v", admitted, err)
	}
	if err := zero.Wait(nil, DefaultFrontierBackpressureMaxLag+1); !errors.Is(err, ErrFrontierBackpressureContextNil) {
		t.Fatalf("nil context Wait() error = %v", err)
	}
}

func TestMZ007FrontierBackpressureSaturatesFrontierLimit(t *testing.T) {
	gate, err := NewFrontierBackpressure(FrontierBackpressureOptions{MaxLag: ^uint64(0)})
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.AdvanceConsumed(^uint64(0) - 1); err != nil {
		t.Fatal(err)
	}
	if admitted, err := gate.TryAdmit(^uint64(0)); err != nil || !admitted {
		t.Fatalf("saturated TryAdmit() = admitted %v, err %v", admitted, err)
	}
}

func TestMZ007FrontierBackpressureCloseWakesBlockedWait(t *testing.T) {
	gate, err := NewFrontierBackpressure(FrontierBackpressureOptions{MaxLag: 1})
	if err != nil {
		t.Fatal(err)
	}
	if admitted, err := gate.TryAdmit(1); err != nil || !admitted {
		t.Fatalf("initial TryAdmit() = admitted %v, err %v", admitted, err)
	}
	result := make(chan error, 1)
	go func() { result <- gate.Wait(context.Background(), 2) }()
	waitForMZ007FrontierBackpressure(t, gate)
	gate.Close()
	select {
	case err := <-result:
		if !errors.Is(err, ErrFrontierBackpressureClosed) {
			t.Fatalf("closed Wait() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() did not wake blocked Wait()")
	}
}

func waitForMZ007FrontierBackpressure(t *testing.T, gate *FrontierBackpressure) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		stats := gate.Stats()
		if stats.BlockedAttempts > 0 && stats.Blocked {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("Wait() did not enter backpressure")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestMZ007FrontierBackpressureCloseAndOptions(t *testing.T) {
	if _, err := NewFrontierBackpressure(FrontierBackpressureOptions{MaxLag: 0}); err != nil {
		t.Fatalf("zero MaxLag error = %v", err)
	}
	gate, err := NewFrontierBackpressure(FrontierBackpressureOptions{MaxLag: 1})
	if err != nil {
		t.Fatal(err)
	}
	gate.Close()
	gate.Close()
	if admitted, err := gate.TryAdmit(0); !errors.Is(err, ErrFrontierBackpressureClosed) || admitted {
		t.Fatalf("closed TryAdmit() = admitted %v, err %v", admitted, err)
	}
	if err := gate.Wait(context.Background(), 0); !errors.Is(err, ErrFrontierBackpressureClosed) {
		t.Fatalf("closed Wait() error = %v", err)
	}
	if err := gate.AdvanceConsumed(1); !errors.Is(err, ErrFrontierBackpressureClosed) {
		t.Fatalf("closed AdvanceConsumed() error = %v", err)
	}
}
