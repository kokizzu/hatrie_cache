package hatStorage

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func ch021TestPolicy(t *testing.T) StorageTierPolicy {
	t.Helper()
	hot, err := NewDiskPlacementPolicy("hot", []DiskPlacementRule{{Path: "/data/hot", Weight: 1}})
	if err != nil {
		t.Fatal(err)
	}
	cold, err := NewDiskPlacementPolicy("cold", []DiskPlacementRule{{Path: "/data/cold", Weight: 1}})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := NewStorageTierPolicy([]StorageTierRule{
		{Name: "hot", MinAge: 0, Placement: hot},
		{Name: "cold", MinAge: 24 * time.Hour, Placement: cold},
	})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func TestStorageTierReaderPrefersCurrentTier(t *testing.T) {
	policy := ch021TestPolicy(t)
	var calls []StorageTierSelection
	reader, err := NewStorageTierReader(policy, func(_ context.Context, selection StorageTierSelection) ([]byte, error) {
		calls = append(calls, selection)
		return []byte("hot-data"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Read(context.Background(), StorageTierReadPart{Key: "part-1", CurrentTier: "hot", Age: 48 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Data) != "hot-data" || result.Fallback || result.Selection.Tier != "hot" {
		t.Fatalf("result = %+v, want current hot tier without fallback", result)
	}
	if want := []StorageTierSelection{{Tier: "hot", Path: "/data/hot"}}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %#v, want %#v", calls, want)
	}
}

func TestStorageTierReaderFallsBackOnlyOnNotFound(t *testing.T) {
	policy := ch021TestPolicy(t)
	var calls []string
	reader, err := NewStorageTierReader(policy, func(_ context.Context, selection StorageTierSelection) ([]byte, error) {
		calls = append(calls, selection.Tier)
		if selection.Tier == "hot" {
			return nil, ErrStorageTierPartNotFound
		}
		return []byte("cold-data"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Read(context.Background(), StorageTierReadPart{Key: "part-1", CurrentTier: "hot", Age: 48 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Data) != "cold-data" || !result.Fallback || result.Selection.Tier != "cold" {
		t.Fatalf("result = %+v, want cold fallback", result)
	}
	if want := []string{"hot", "cold"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %#v, want %#v", calls, want)
	}
}

func TestStorageTierReaderDoesNotFallbackOnOtherErrors(t *testing.T) {
	policy := ch021TestPolicy(t)
	wantErr := errors.New("permission denied")
	calls := 0
	reader, err := NewStorageTierReader(policy, func(context.Context, StorageTierSelection) ([]byte, error) {
		calls++
		return nil, wantErr
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.Read(context.Background(), StorageTierReadPart{Key: "part-1", CurrentTier: "hot", Age: 48 * time.Hour})
	if !errors.Is(err, wantErr) || calls != 1 {
		t.Fatalf("error = %v, calls = %d, want original error and one read", err, calls)
	}
}

func TestStorageTierReaderDoesNotRetryWhenCurrentTierIsSelected(t *testing.T) {
	policy := ch021TestPolicy(t)
	calls := 0
	reader, err := NewStorageTierReader(policy, func(context.Context, StorageTierSelection) ([]byte, error) {
		calls++
		return nil, ErrStorageTierPartNotFound
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = reader.Read(context.Background(), StorageTierReadPart{Key: "part-1", CurrentTier: "hot", Age: time.Hour})
	if !errors.Is(err, ErrStorageTierPartNotFound) || calls != 1 {
		t.Fatalf("error = %v, calls = %d, want not-found and one read", err, calls)
	}
}

func TestStorageTierReaderValidatesInputs(t *testing.T) {
	policy := ch021TestPolicy(t)
	if _, err := NewStorageTierReader(policy, nil); !errors.Is(err, ErrStorageTierReaderInvalid) {
		t.Fatalf("nil reader error = %v, want invalid", err)
	}
	reader, err := NewStorageTierReader(policy, func(context.Context, StorageTierSelection) ([]byte, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, part := range map[string]StorageTierReadPart{
		"empty key":     {CurrentTier: "hot"},
		"empty current": {Key: "part"},
		"negative age":  {Key: "part", CurrentTier: "hot", Age: -time.Second},
	} {
		if _, err := reader.Read(context.Background(), part); !errors.Is(err, ErrStorageTierReaderInvalid) {
			t.Errorf("%s error = %v, want invalid", name, err)
		}
	}
	if _, err := reader.Read(nil, StorageTierReadPart{Key: "part", CurrentTier: "hot"}); !errors.Is(err, ErrStorageTierReaderInvalid) {
		t.Fatalf("nil context error = %v, want invalid", err)
	}
}
