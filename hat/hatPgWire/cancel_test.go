package hatPgWire_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatPgWire"
)

func TestCancelRegistryCancelsOnlyMatchingSession(t *testing.T) {
	registry := hatPgWire.NewCancelRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	processID, secret := registry.Register(cancel)
	if registry.Cancel(processID, secret+1) {
		t.Fatal("Cancel accepted an incorrect secret")
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("incorrect secret cancelled context: %v", err)
	}
	if !registry.Cancel(processID, secret) {
		t.Fatal("Cancel rejected a matching session")
	}
	if err := ctx.Err(); err == nil {
		t.Fatal("matching secret did not cancel context")
	}
}
