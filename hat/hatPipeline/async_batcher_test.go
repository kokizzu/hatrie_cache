package hatPipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestAsyncBatcherFlushesBySizeAndExplicitFlush(t *testing.T) {
	var batches [][]int
	batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:      8,
		MaxBatchSize:  3,
		FlushInterval: time.Hour,
		Handler: func(_ context.Context, values []int) error {
			batches = append(batches, append([]int(nil), values...))
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewAsyncBatcher() error = %v", err)
	}

	for _, value := range []int{1, 2, 3, 4, 5} {
		if err := batcher.Submit(context.Background(), value); err != nil {
			t.Fatalf("Submit(%d) error = %v", value, err)
		}
	}
	if err := batcher.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if want := [][]int{{1, 2, 3}, {4, 5}}; !reflect.DeepEqual(batches, want) {
		t.Fatalf("batches = %#v, want %#v", batches, want)
	}
	if err := batcher.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	stats := batcher.Stats()
	if stats.Submitted != 5 || stats.FlushedBatches != 2 || stats.FlushedItems != 5 || stats.Pending != 0 {
		t.Fatalf("Stats() = %#v, want submitted=5 batches=2 items=5 pending=0", stats)
	}
}

func TestAsyncBatcherFlushesOnInterval(t *testing.T) {
	flushed := make(chan []int, 1)
	batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:      2,
		MaxBatchSize:  8,
		FlushInterval: 5 * time.Millisecond,
		Handler: func(_ context.Context, values []int) error {
			flushed <- append([]int(nil), values...)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewAsyncBatcher() error = %v", err)
	}
	if err := batcher.Submit(context.Background(), 7); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	select {
	case got := <-flushed:
		if !reflect.DeepEqual(got, []int{7}) {
			t.Fatalf("interval batch = %#v, want [7]", got)
		}
	case <-time.After(time.Second):
		t.Fatal("interval flush did not run")
	}
	if err := batcher.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestAsyncBatcherReturnsHandlerErrorsOnce(t *testing.T) {
	wantErr := errors.New("insert failed")
	call := 0
	batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:      4,
		MaxBatchSize:  2,
		FlushInterval: time.Hour,
		Handler: func(_ context.Context, _ []int) error {
			call++
			if call == 1 {
				return wantErr
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewAsyncBatcher() error = %v", err)
	}
	for _, value := range []int{1, 2} {
		if err := batcher.Submit(context.Background(), value); err != nil {
			t.Fatalf("Submit(%d) error = %v", value, err)
		}
	}
	if err := batcher.Flush(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("first Flush() error = %v, want %v", err, wantErr)
	}
	if err := batcher.Flush(context.Background()); err != nil {
		t.Fatalf("second Flush() error = %v, want nil", err)
	}
	if got := batcher.Stats().HandlerErrors; got != 1 {
		t.Fatalf("HandlerErrors = %d, want 1", got)
	}
	if err := batcher.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestAsyncBatcherBackpressureAndCancellableClose(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	first := true
	batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:      1,
		MaxBatchSize:  1,
		FlushInterval: time.Hour,
		Handler: func(_ context.Context, _ []int) error {
			if first {
				first = false
				close(started)
				<-release
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewAsyncBatcher() error = %v", err)
	}
	if err := batcher.Submit(context.Background(), 1); err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first batch did not start")
	}
	if err := batcher.Submit(context.Background(), 2); err != nil {
		t.Fatalf("second Submit() error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := batcher.Submit(canceled, 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("backpressured Submit() error = %v, want context.Canceled", err)
	}

	closeResult := make(chan error, 1)
	closeContext, cancelClose := context.WithCancel(context.Background())
	go func() { closeResult <- batcher.Close(closeContext) }()
	select {
	case err := <-closeResult:
		t.Fatalf("Close() returned before handler release: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	cancelClose()
	if err := <-closeResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellable Close() error = %v, want context.Canceled", err)
	}
	close(release)
	if err := batcher.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if got := batcher.Stats().FlushedItems; got != 2 {
		t.Fatalf("FlushedItems = %d, want 2", got)
	}
}

func TestAsyncBatcherValidatesOptionsAndNilReceiver(t *testing.T) {
	handler := func(context.Context, []int) error { return nil }
	cases := []struct {
		name    string
		want    error
		options AsyncBatcherOptions[int]
	}{
		{name: "handler", want: ErrAsyncBatcherHandlerRequired},
		{name: "capacity", want: ErrAsyncBatcherCapacityInvalid, options: AsyncBatcherOptions[int]{Handler: handler, Capacity: -1}},
		{name: "batch size", want: ErrAsyncBatcherMaxBatchSizeInvalid, options: AsyncBatcherOptions[int]{Handler: handler, MaxBatchSize: -1}},
		{name: "interval", want: ErrAsyncBatcherFlushIntervalInvalid, options: AsyncBatcherOptions[int]{Handler: handler, FlushInterval: -1}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewAsyncBatcher(test.options); !errors.Is(err, test.want) {
				t.Fatalf("NewAsyncBatcher() error = %v, want %v", err, test.want)
			}
		})
	}
	var nilBatcher *AsyncBatcher[int]
	if err := nilBatcher.Submit(context.Background(), 1); !errors.Is(err, ErrAsyncBatcherNil) {
		t.Fatalf("nil Submit() error = %v", err)
	}
	if err := nilBatcher.Flush(context.Background()); !errors.Is(err, ErrAsyncBatcherNil) {
		t.Fatalf("nil Flush() error = %v", err)
	}
	if err := nilBatcher.Close(context.Background()); !errors.Is(err, ErrAsyncBatcherNil) {
		t.Fatalf("nil Close() error = %v", err)
	}
	if got := nilBatcher.Stats(); got != (AsyncBatcherStats{}) {
		t.Fatalf("nil Stats() = %#v, want zero", got)
	}
}

func BenchmarkAsyncBatcherSubmit(b *testing.B) {
	benchmarkAsyncBatcherSubmit(b, 64)
}

func BenchmarkAsyncBatcherSubmitMaxOne(b *testing.B) {
	benchmarkAsyncBatcherSubmit(b, 1)
}

func benchmarkAsyncBatcherSubmit(b *testing.B, maxBatchSize int) {
	batcher, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:      1024,
		MaxBatchSize:  maxBatchSize,
		FlushInterval: time.Hour,
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
	b.ReportMetric(float64(batcher.Stats().FlushedBatches), "batches")
}
