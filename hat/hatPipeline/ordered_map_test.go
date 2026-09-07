package hatPipeline_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatPipeline"
)

func TestOrderedMapPreservesInputOrderAcrossConcurrentWorkers(t *testing.T) {
	input := make([]int, 64)
	for index := range input {
		input[index] = index
	}
	got, err := hatPipeline.OrderedMap(context.Background(), input, 4, func(_ context.Context, value int) (int, error) {
		if value%4 == 0 {
			time.Sleep(time.Millisecond)
		}
		return value * 2, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := make([]int, len(input))
	for index, value := range input {
		want[index] = value * 2
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ordered map = %v, want %v", got, want)
	}
}

func TestOrderedMapCancelsOnFirstProcessingErrorAndValidatesInputs(t *testing.T) {
	wantErr := errors.New("process failed")
	_, err := hatPipeline.OrderedMap(context.Background(), []int{1, 2, 3, 4}, 2, func(ctx context.Context, value int) (int, error) {
		if value == 1 {
			return 0, wantErr
		}
		<-ctx.Done()
		return 0, ctx.Err()
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("processing error = %v, want %v", err, wantErr)
	}

	if _, err := hatPipeline.OrderedMap(context.Background(), []int{1}, 0, func(context.Context, int) (int, error) { return 0, nil }); !errors.Is(err, hatPipeline.ErrOrderedMapInvalid) {
		t.Fatalf("zero workers error = %v", err)
	}
	if _, err := hatPipeline.OrderedMap[int, int](context.Background(), []int{1}, 1, nil); !errors.Is(err, hatPipeline.ErrOrderedMapInvalid) {
		t.Fatalf("nil process error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := hatPipeline.OrderedMap(canceled, []int{1}, 1, func(context.Context, int) (int, error) { return 0, nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled map error = %v", err)
	}
}
