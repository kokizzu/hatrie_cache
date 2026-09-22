package hatPipeline

import (
	"errors"
	"testing"
)

func TestM247FrontierRetentionExpiredErrorIdentifiesResumeBoundary(t *testing.T) {
	frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatalf("NewFrontierRegistry() error = %v", err)
	}
	if err := frontiers.Register("orders"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := frontiers.Advance("orders", 100, 120); err != nil {
		t.Fatalf("Advance() error = %v", err)
	}
	retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})
	if err != nil {
		t.Fatalf("NewFrontierRetentionRegistry() error = %v", err)
	}
	defer retention.Close()

	_, err = retention.Acquire("orders", 99)
	if !errors.Is(err, ErrFrontierRetentionExpired) {
		t.Fatalf("Acquire(expired) error = %v, want %v", err, ErrFrontierRetentionExpired)
	}
	var expired *FrontierRetentionExpiredError
	if !errors.As(err, &expired) {
		t.Fatalf("Acquire(expired) error type = %T, want *FrontierRetentionExpiredError", err)
	}
	if expired.FrontierID != "orders" || expired.RequestedAsOf != 99 || expired.CurrentLower != 100 || expired.CurrentUpper != 120 {
		t.Fatalf("expired error = %#v", expired)
	}
	if expired.Error() == "" {
		t.Fatal("expired error has empty message")
	}
}
