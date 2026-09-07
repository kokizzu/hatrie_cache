package hatSql

import (
	"context"
	"errors"
	"testing"
)

func TestReadReplicaSetRoundRobinsResolvers(t *testing.T) {
	first := SourceResolverFunc(func(_, _ string) ([]Row, error) { return []Row{{"name": "first"}}, nil })
	second := SourceResolverFunc(func(_, _ string) ([]Row, error) { return []Row{{"name": "second"}}, nil })
	set, err := NewReadReplicaSet(first, second)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"first", "second", "first"} {
		result, err := set.Execute(context.Background(), "FROM CACHE('people') SELECT name", nil, QueryOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got := result.Rows[0]["name"]; got != want {
			t.Fatalf("replica result = %q, want %q", got, want)
		}
	}
}

func TestReadReplicaSetExecuteWithRetryRotatesAndBoundsAttempts(t *testing.T) {
	transient := errors.New("replica unavailable")
	attempts := 0
	first := SourceResolverFunc(func(_, _ string) ([]Row, error) {
		attempts++
		return nil, transient
	})
	second := SourceResolverFunc(func(_, _ string) ([]Row, error) {
		attempts++
		return []Row{{"name": "second"}}, nil
	})
	set, err := NewReadReplicaSet(first, second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := set.ExecuteWithRetry(context.Background(), "FROM CACHE('people') SELECT name", nil, QueryOptions{}, ReadReplicaRetryOptions{
		MaxAttempts: 2,
		Retryable:   func(error) bool { return true },
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0]["name"]; got != "second" {
		t.Fatalf("retry result = %q, want second", got)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestReadReplicaSetExecuteWithRetryDoesNotRetryUnclassifiedErrors(t *testing.T) {
	transient := errors.New("replica unavailable")
	attempts := 0
	first := SourceResolverFunc(func(_, _ string) ([]Row, error) {
		attempts++
		return nil, transient
	})
	second := SourceResolverFunc(func(_, _ string) ([]Row, error) {
		attempts++
		return []Row{{"name": "second"}}, nil
	})
	set, err := NewReadReplicaSet(first, second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = set.ExecuteWithRetry(context.Background(), "FROM CACHE('people') SELECT name", nil, QueryOptions{}, ReadReplicaRetryOptions{
		MaxAttempts: 4,
		Retryable:   func(error) bool { return false },
	})
	if !errors.Is(err, transient) {
		t.Fatalf("non-retryable error = %v, want transient", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestReadReplicaSetExecuteWithRetryRejectsUnboundedConfiguration(t *testing.T) {
	set, err := NewReadReplicaSet(SourceResolverFunc(func(_, _ string) ([]Row, error) { return nil, nil }))
	if err != nil {
		t.Fatal(err)
	}
	_, err = set.ExecuteWithRetry(context.Background(), "FROM CACHE('people') SELECT name", nil, QueryOptions{}, ReadReplicaRetryOptions{MaxAttempts: 9})
	if !errors.Is(err, ErrReadReplicaRetryOptionsInvalid) {
		t.Fatalf("invalid retry options error = %v, want invalid options", err)
	}
}

func TestReadReplicaSetExecuteWithRetryHonorsCanceledContextWithoutBackoff(t *testing.T) {
	transient := errors.New("replica unavailable")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempts := 0
	first := SourceResolverFunc(func(_, _ string) ([]Row, error) {
		attempts++
		cancel()
		return nil, transient
	})
	second := SourceResolverFunc(func(_, _ string) ([]Row, error) {
		attempts++
		return []Row{{"name": "second"}}, nil
	})
	set, err := NewReadReplicaSet(first, second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = set.ExecuteWithRetry(ctx, "FROM CACHE('people') SELECT name", nil, QueryOptions{}, ReadReplicaRetryOptions{
		MaxAttempts: 2,
		Retryable:   func(error) bool { return true },
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled retry error = %v, want context canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts after cancellation = %d, want 1", attempts)
	}
}
