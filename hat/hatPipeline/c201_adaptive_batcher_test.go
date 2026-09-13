package hatPipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestC201AdaptiveFlushIntervalTracksArrivalRate(t *testing.T) {
	tests := []struct {
		name   string
		base   time.Duration
		target int
		rate   float64
		min    time.Duration
		max    time.Duration
		want   time.Duration
	}{
		{name: "fast arrivals", base: 10 * time.Millisecond, target: 4, rate: 1000, min: time.Millisecond, max: 50 * time.Millisecond, want: 4 * time.Millisecond},
		{name: "steady arrivals", base: 10 * time.Millisecond, target: 4, rate: 100, min: time.Millisecond, max: 50 * time.Millisecond, want: 40 * time.Millisecond},
		{name: "slow arrivals clamp high", base: 10 * time.Millisecond, target: 4, rate: 1, min: time.Millisecond, max: 50 * time.Millisecond, want: 50 * time.Millisecond},
		{name: "unknown rate keeps base", base: 10 * time.Millisecond, target: 4, rate: 0, min: time.Millisecond, max: 50 * time.Millisecond, want: 10 * time.Millisecond},
		{name: "fast arrivals clamp low", base: 10 * time.Millisecond, target: 4, rate: 100000, min: 5 * time.Millisecond, max: 50 * time.Millisecond, want: 5 * time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := asyncBatcherAdaptiveInterval(test.base, test.target, test.rate, test.min, test.max); got != test.want {
				t.Fatalf("adaptive interval = %s, want %s", got, test.want)
			}
		})
	}
}

func TestC201AdaptiveBatcherUpdatesIntervalFromArrivals(t *testing.T) {
	batcher := &AsyncBatcher[int]{
		interval:         10 * time.Millisecond,
		adaptive:         true,
		adaptiveTarget:   4,
		adaptiveMin:      time.Millisecond,
		adaptiveMax:      50 * time.Millisecond,
		adaptiveInterval: 10 * time.Millisecond,
	}
	start := time.Unix(0, 0)
	batcher.observeArrival(start)
	batcher.observeArrival(start.Add(time.Millisecond))
	if batcher.adaptiveInterval != 4*time.Millisecond {
		t.Fatalf("adaptive interval = %s, want 4ms", batcher.adaptiveInterval)
	}
}

func TestC201AdaptiveBatcherIsOptInAndUsesBoundedDefaults(t *testing.T) {
	handler := func(context.Context, []int) error { return nil }
	fixed, err := NewAsyncBatcher(AsyncBatcherOptions[int]{Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	if fixed.adaptive {
		t.Fatal("fixed batcher unexpectedly enabled adaptive flushing")
	}
	if err := fixed.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	adaptive, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		FlushInterval: 10 * time.Millisecond,
		AdaptiveFlush: true,
		Handler:       handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	if adaptive.adaptiveTarget != 32 || adaptive.adaptiveMin != 2500*time.Microsecond || adaptive.adaptiveMax != 40*time.Millisecond || adaptive.adaptiveInterval != 10*time.Millisecond {
		t.Fatalf("adaptive defaults = target %d min %s max %s interval %s", adaptive.adaptiveTarget, adaptive.adaptiveMin, adaptive.adaptiveMax, adaptive.adaptiveInterval)
	}
	if err := adaptive.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestC201AdaptiveBatcherValidatesAdaptiveOptions(t *testing.T) {
	handler := func(context.Context, []int) error { return nil }
	for _, test := range []struct {
		name    string
		options AsyncBatcherOptions[int]
		want    error
	}{
		{name: "target", options: AsyncBatcherOptions[int]{AdaptiveFlush: true, AdaptiveTargetBatchSize: -1, Handler: handler}, want: ErrAsyncBatcherAdaptiveTargetInvalid},
		{name: "minimum", options: AsyncBatcherOptions[int]{AdaptiveFlush: true, AdaptiveMinFlushInterval: -1, Handler: handler}, want: ErrAsyncBatcherAdaptiveMinIntervalInvalid},
		{name: "maximum", options: AsyncBatcherOptions[int]{AdaptiveFlush: true, AdaptiveMinFlushInterval: 5 * time.Millisecond, AdaptiveMaxFlushInterval: time.Millisecond, Handler: handler}, want: ErrAsyncBatcherAdaptiveMaxIntervalInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewAsyncBatcher(test.options); !errors.Is(err, test.want) {
				t.Fatalf("NewAsyncBatcher() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestC201AdaptiveBatcherFlushesPartialBatchInOrder(t *testing.T) {
	batches := make(chan []int, 1)
	batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:                 8,
		MaxBatchSize:             4,
		FlushInterval:            10 * time.Millisecond,
		AdaptiveFlush:            true,
		AdaptiveTargetBatchSize:  2,
		AdaptiveMinFlushInterval: time.Millisecond,
		AdaptiveMaxFlushInterval: 20 * time.Millisecond,
		Handler: func(_ context.Context, batch []int) error {
			batches <- append([]int(nil), batch...)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewAsyncBatcher() error = %v", err)
	}

	for _, value := range []int{10, 20, 30} {
		if err := batcher.Submit(context.Background(), value); err != nil {
			t.Fatalf("Submit(%d) error = %v", value, err)
		}
	}

	select {
	case batch := <-batches:
		if want := []int{10, 20, 30}; !reflect.DeepEqual(batch, want) {
			t.Fatalf("batch = %v, want %v", batch, want)
		}
	case <-time.After(time.Second):
		t.Fatal("adaptive batch did not flush")
	}

	if err := batcher.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

var c201AdaptiveBatcherSink AsyncBatcherStats

func BenchmarkC201AsyncBatcherSubmitModes(b *testing.B) {
	for _, test := range []struct {
		name     string
		adaptive bool
	}{
		{name: "fixed", adaptive: false},
		{name: "adaptive", adaptive: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
				Capacity:      1024,
				MaxBatchSize:  64,
				FlushInterval: time.Hour,
				AdaptiveFlush: test.adaptive,
				Handler:       func(context.Context, []int) error { return nil },
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if err := batcher.Submit(context.Background(), index); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err := batcher.Close(context.Background()); err != nil {
				b.Fatal(err)
			}
			stats := batcher.Stats()
			b.ReportMetric(float64(stats.FlushedBatches), "batches")
			b.ReportMetric(float64(stats.FlushedItems), "items")
			c201AdaptiveBatcherSink = stats
		})
	}
}

func BenchmarkC201AdaptiveArrivalObservation(b *testing.B) {
	batcher := &AsyncBatcher[int]{
		interval:         10 * time.Millisecond,
		adaptive:         true,
		adaptiveTarget:   32,
		adaptiveMin:      time.Millisecond,
		adaptiveMax:      40 * time.Millisecond,
		adaptiveInterval: 10 * time.Millisecond,
	}
	now := time.Unix(0, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		now = now.Add(time.Millisecond)
		batcher.observeArrival(now)
	}
	b.StopTimer()
	c201AdaptiveBatcherSink = batcher.Stats()
}
