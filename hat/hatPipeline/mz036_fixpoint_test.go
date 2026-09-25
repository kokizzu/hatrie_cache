package hatPipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestMZ036FixpointPropagatesUntilQuiescence(t *testing.T) {
	result, err := RunFixpoint(context.Background(), []FixpointItem[int, int]{{Key: 0, Value: 0}}, FixpointOptions[int, int]{
		Step: func(_ context.Context, item FixpointItem[int, int], emit FixpointEmitter[int, int]) error {
			if item.Key >= 4 {
				return nil
			}
			return emit(FixpointItem[int, int]{Key: item.Key + 1, Value: item.Value + 1})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Steps != 5 || result.Enqueued != 5 || result.MaxPending != 1 {
		t.Fatalf("result = %#v, want five steps/enqueues and one pending", result)
	}
}

func TestMZ036FixpointCoalescesPendingKeysWithMerge(t *testing.T) {
	var seen []int
	result, err := RunFixpoint(context.Background(), []FixpointItem[int, int]{
		{Key: 1, Value: 2},
		{Key: 1, Value: 3},
	}, FixpointOptions[int, int]{
		Merge: func(existing, incoming int) int { return existing + incoming },
		Step: func(_ context.Context, item FixpointItem[int, int], _ FixpointEmitter[int, int]) error {
			seen = append(seen, item.Value)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Steps != 1 || !reflect.DeepEqual(seen, []int{5}) {
		t.Fatalf("result/seen = %#v/%v, want one merged item with value 5", result, seen)
	}
}

func TestMZ036FixpointRejectsInvalidAndBoundedWork(t *testing.T) {
	if _, err := RunFixpoint[int, int](context.Background(), nil, FixpointOptions[int, int]{}); !errors.Is(err, ErrFixpointInvalid) {
		t.Fatalf("nil step error = %v, want invalid", err)
	}
	step := func(_ context.Context, item FixpointItem[int, int], emit FixpointEmitter[int, int]) error {
		return emit(FixpointItem[int, int]{Key: item.Key + 1, Value: item.Value + 1})
	}
	if _, err := RunFixpoint(context.Background(), []FixpointItem[int, int]{{Key: 0}}, FixpointOptions[int, int]{MaxSteps: 2, Step: step}); !errors.Is(err, ErrFixpointStepLimit) {
		t.Fatalf("step limit error = %v, want step limit", err)
	}
	if _, err := RunFixpoint(context.Background(), []FixpointItem[int, int]{{Key: 0}}, FixpointOptions[int, int]{MaxPending: 1, Step: func(_ context.Context, _ FixpointItem[int, int], emit FixpointEmitter[int, int]) error {
		if err := emit(FixpointItem[int, int]{Key: 1}); err != nil {
			return err
		}
		return emit(FixpointItem[int, int]{Key: 2})
	}}); !errors.Is(err, ErrFixpointPendingLimit) {
		t.Fatalf("pending limit error = %v, want pending limit", err)
	}
}

func TestMZ036FixpointPropagatesCancellationAndStepErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RunFixpoint(ctx, []FixpointItem[int, int]{{Key: 1}}, FixpointOptions[int, int]{Step: func(context.Context, FixpointItem[int, int], FixpointEmitter[int, int]) error {
		return nil
	}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v, want context canceled", err)
	}
	wantErr := errors.New("step failed")
	if _, err := RunFixpoint(context.Background(), []FixpointItem[int, int]{{Key: 1}}, FixpointOptions[int, int]{Step: func(context.Context, FixpointItem[int, int], FixpointEmitter[int, int]) error {
		return wantErr
	}}); !errors.Is(err, wantErr) {
		t.Fatalf("step error = %v, want %v", err, wantErr)
	}
}
