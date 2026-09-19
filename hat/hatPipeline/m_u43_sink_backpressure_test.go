package hatPipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestSinkBackpressureRegistryTracksFrontiersAndHysteresis(t *testing.T) {
	registry, err := NewSinkBackpressureRegistry(SinkBackpressureRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("orders", SinkBackpressureSinkOptions{HighWatermark: 4, LowWatermark: 2}); err != nil {
		t.Fatal(err)
	}
	if err := registry.WaitUntilWritable(context.Background(), "orders"); err != nil {
		t.Fatalf("initial WaitUntilWritable() error = %v", err)
	}
	if err := registry.Record("orders", 4, 0); err != nil {
		t.Fatal(err)
	}
	status, ok := registry.Status("orders")
	if !ok {
		t.Fatal("status missing")
	}
	if status.EmittedFrontier != 4 || status.AcknowledgedFrontier != 0 || status.Lag != 4 || !status.Blocked {
		t.Fatalf("blocked status = %#v", status)
	}
	if err := registry.Acknowledge("orders", 1); err != nil {
		t.Fatal(err)
	}
	status, _ = registry.Status("orders")
	if !status.Blocked || status.Lag != 3 {
		t.Fatalf("status below high watermark = %#v, want blocked lag 3", status)
	}

	waitResult := make(chan error, 1)
	go func() {
		waitResult <- registry.WaitUntilWritable(context.Background(), "orders")
	}()
	select {
	case err := <-waitResult:
		t.Fatalf("WaitUntilWritable() returned before low watermark: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := registry.Acknowledge("orders", 2); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waitResult:
		if err != nil {
			t.Fatalf("WaitUntilWritable() after low watermark error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitUntilWritable() did not wake at low watermark")
	}
	status, _ = registry.Status("orders")
	if status.Blocked || status.Lag != 2 {
		t.Fatalf("unblocked status = %#v", status)
	}
	if err := registry.Advance("orders", 6); err != nil {
		t.Fatal(err)
	}
	status, _ = registry.Status("orders")
	if !status.Blocked || status.Lag != 4 {
		t.Fatalf("status after re-block = %#v", status)
	}
	if err := registry.Acknowledge("orders", 4); err != nil {
		t.Fatal(err)
	}
	if status, _ := registry.Status("orders"); status.Blocked || status.Lag != 2 {
		t.Fatalf("status after second unblock = %#v", status)
	}
}

func TestSinkBackpressureRegistryValidatesMonotoneUpdatesAndOptions(t *testing.T) {
	if _, err := NewSinkBackpressureRegistry(SinkBackpressureRegistryOptions{DefaultHighWatermark: 4, DefaultLowWatermark: 4}); !errors.Is(err, ErrSinkBackpressureOptionsInvalid) {
		t.Fatalf("invalid registry options error = %v", err)
	}
	if _, err := NewSinkBackpressureRegistry(SinkBackpressureRegistryOptions{DefaultHighWatermark: MaxSinkBackpressureWatermark + 1}); !errors.Is(err, ErrSinkBackpressureOptionsInvalid) {
		t.Fatalf("oversized registry options error = %v", err)
	}
	registry, err := NewSinkBackpressureRegistry(SinkBackpressureRegistryOptions{DefaultHighWatermark: 8, DefaultLowWatermark: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(" ", SinkBackpressureSinkOptions{}); !errors.Is(err, ErrSinkBackpressureNameRequired) {
		t.Fatalf("empty sink error = %v", err)
	}
	if err := registry.Register("orders", SinkBackpressureSinkOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("orders", SinkBackpressureSinkOptions{}); !errors.Is(err, ErrSinkBackpressureAlreadyRegistered) {
		t.Fatalf("duplicate sink error = %v", err)
	}
	status, ok := registry.Status("orders")
	if !ok || status.HighWatermark != 8 || status.LowWatermark != 3 {
		t.Fatalf("defaulted status = %#v", status)
	}
	if err := registry.Record("orders", 5, 6); !errors.Is(err, ErrSinkBackpressureAcknowledgementAhead) {
		t.Fatalf("ack ahead error = %v", err)
	}
	if err := registry.Record("orders", 5, 2); err != nil {
		t.Fatal(err)
	}
	if err := registry.Advance("orders", 4); !errors.Is(err, ErrSinkBackpressureRegression) {
		t.Fatalf("emitted regression error = %v", err)
	}
	if err := registry.Acknowledge("orders", 1); !errors.Is(err, ErrSinkBackpressureRegression) {
		t.Fatalf("ack regression error = %v", err)
	}
	status, _ = registry.Status("orders")
	if status.EmittedFrontier != 5 || status.AcknowledgedFrontier != 2 {
		t.Fatalf("state changed after rejected update = %#v", status)
	}
	if err := registry.Record("missing", 1, 0); !errors.Is(err, ErrSinkBackpressureMissing) {
		t.Fatalf("missing sink error = %v", err)
	}
}

func TestSinkBackpressureSinkHighWatermarkDerivesItsOwnLowWatermark(t *testing.T) {
	registry, err := NewSinkBackpressureRegistry(SinkBackpressureRegistryOptions{DefaultHighWatermark: 8, DefaultLowWatermark: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("fast", SinkBackpressureSinkOptions{HighWatermark: 4}); err != nil {
		t.Fatal(err)
	}
	status, ok := registry.Status("fast")
	if !ok || status.HighWatermark != 4 || status.LowWatermark != 2 {
		t.Fatalf("sink-specific watermark defaults = %#v, want high 4 low 2", status)
	}
}

func TestSinkBackpressureRegistryWaitHonorsCancellationAndClose(t *testing.T) {
	registry, err := NewSinkBackpressureRegistry(SinkBackpressureRegistryOptions{DefaultHighWatermark: 2, DefaultLowWatermark: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("orders", SinkBackpressureSinkOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Record("orders", 2, 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := registry.WaitUntilWritable(ctx, "orders"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled WaitUntilWritable() error = %v", err)
	}

	waitResult := make(chan error, 1)
	go func() {
		waitResult <- registry.WaitUntilWritable(context.Background(), "orders")
	}()
	select {
	case err := <-waitResult:
		t.Fatalf("WaitUntilWritable() returned before close: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waitResult:
		if !errors.Is(err, ErrSinkBackpressureClosed) {
			t.Fatalf("WaitUntilWritable() after close error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitUntilWritable() did not wake when registry closed")
	}
	if err := registry.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if err := registry.Advance("orders", 3); !errors.Is(err, ErrSinkBackpressureClosed) {
		t.Fatalf("Advance() after close error = %v", err)
	}
}

func TestSinkBackpressureRegistrySnapshotsAndUnregisters(t *testing.T) {
	registry, err := NewSinkBackpressureRegistry(SinkBackpressureRegistryOptions{DefaultHighWatermark: 10, DefaultLowWatermark: 5})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("zeta", SinkBackpressureSinkOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("alpha", SinkBackpressureSinkOptions{HighWatermark: 4, LowWatermark: 2}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Record("zeta", 3, 1); err != nil {
		t.Fatal(err)
	}
	snapshot := registry.Snapshot()
	want := []SinkBackpressureStatus{
		{Sink: "alpha", HighWatermark: 4, LowWatermark: 2},
		{Sink: "zeta", EmittedFrontier: 3, AcknowledgedFrontier: 1, Lag: 2, HighWatermark: 10, LowWatermark: 5},
	}
	if len(snapshot) != len(want) || snapshot[0].Sink != "alpha" || snapshot[1].Sink != "zeta" {
		t.Fatalf("snapshot ordering = %#v", snapshot)
	}
	for i := range want {
		if snapshot[i].Sink != want[i].Sink || snapshot[i].EmittedFrontier != want[i].EmittedFrontier || snapshot[i].AcknowledgedFrontier != want[i].AcknowledgedFrontier || snapshot[i].Lag != want[i].Lag || snapshot[i].HighWatermark != want[i].HighWatermark || snapshot[i].LowWatermark != want[i].LowWatermark {
			t.Fatalf("snapshot[%d] = %#v, want %#v", i, snapshot[i], want[i])
		}
	}
	if err := registry.Unregister("zeta"); err != nil {
		t.Fatal(err)
	}
	if registry.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", registry.Len())
	}
	if _, ok := registry.Status("zeta"); ok {
		t.Fatal("unregistered sink still has status")
	}
	if err := registry.Acknowledge("zeta", 1); !errors.Is(err, ErrSinkBackpressureMissing) {
		t.Fatalf("unregistered acknowledge error = %v", err)
	}
}

func TestSinkBackpressureRegistrySnapshotIsIndependent(t *testing.T) {
	registry, err := NewSinkBackpressureRegistry(SinkBackpressureRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("orders", SinkBackpressureSinkOptions{}); err != nil {
		t.Fatal(err)
	}
	snapshot := registry.Snapshot()
	snapshot[0].Sink = "changed"
	current, ok := registry.Status("orders")
	if !ok || current.Sink != "orders" {
		t.Fatalf("snapshot mutation changed registry: %#v", current)
	}
	if reflect.DeepEqual(snapshot[0], current) {
		t.Fatal("snapshot should be an independent value")
	}
}
