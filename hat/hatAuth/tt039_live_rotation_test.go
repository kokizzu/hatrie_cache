package hatAuth

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTT039TokenRotatorRotatesWithoutReplacingIdentityProvider(t *testing.T) {
	now := time.Unix(100, 0)
	rotator := NewTokenRotator("old-token", "", time.Time{})
	provider := RotatingTokenIdentity{
		Rotator: rotator,
		Now:     func() time.Time { return now },
	}

	oldRequest := httptest.NewRequest("GET", "http://example.test", nil)
	oldRequest.Header.Set("Authorization", "Bearer old-token")
	if _, authenticated, err := provider.Authenticate(context.Background(), oldRequest); err != nil || !authenticated {
		t.Fatalf("old credential before rotation = %t/%v, want authenticated", authenticated, err)
	}

	if err := rotator.Rotate("new-token", "old-token", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	newRequest := httptest.NewRequest("GET", "http://example.test", nil)
	newRequest.Header.Set("Authorization", "Bearer new-token")
	if _, authenticated, err := provider.Authenticate(context.Background(), newRequest); err != nil || !authenticated {
		t.Fatalf("new credential during rotation = %t/%v, want authenticated", authenticated, err)
	}
	if _, authenticated, err := provider.Authenticate(context.Background(), oldRequest); err != nil || !authenticated {
		t.Fatalf("old credential during rotation = %t/%v, want authenticated", authenticated, err)
	}

	now = now.Add(2 * time.Minute)
	if _, authenticated, err := provider.Authenticate(context.Background(), oldRequest); err != nil || authenticated {
		t.Fatalf("expired old credential = %t/%v, want rejected", authenticated, err)
	}
	if _, authenticated, err := provider.Authenticate(context.Background(), newRequest); err != nil || !authenticated {
		t.Fatalf("new credential after rotation = %t/%v, want authenticated", authenticated, err)
	}
}

func TestTT039TokenRotatorIsSafeForConcurrentReadsAndRotations(t *testing.T) {
	rotator := NewTokenRotator("token-a", "token-b", time.Now().Add(time.Minute))
	var invalid atomic.Int32
	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for iteration := 0; iteration < 500; iteration++ {
				token := "token-a"
				if (worker+iteration)%2 == 1 {
					token = "token-b"
				}
				if !rotator.Matches(token, time.Now()) {
					invalid.Add(1)
				}
			}
		}(worker)
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		for iteration := 0; iteration < 500; iteration++ {
			if iteration%2 == 0 {
				if err := rotator.Rotate("token-a", "token-b", time.Now().Add(time.Minute)); err != nil {
					t.Error(err)
				}
			} else {
				if err := rotator.Rotate("token-b", "token-a", time.Now().Add(time.Minute)); err != nil {
					t.Error(err)
				}
			}
		}
	}()
	wait.Wait()
	if got := invalid.Load(); got != 0 {
		t.Fatalf("concurrent rotations rejected %d valid overlap reads", got)
	}
}

func TestTT039RotatingTokenIdentityHandlesNilAndInvalidRequests(t *testing.T) {
	var provider RotatingTokenIdentity
	if _, authenticated, err := provider.Authenticate(context.Background(), nil); err != nil || authenticated {
		t.Fatalf("nil provider request = %t/%v, want unauthenticated", authenticated, err)
	}
	rotator := NewTokenRotator("token", "", time.Time{})
	provider = RotatingTokenIdentity{Rotator: rotator}
	request := httptest.NewRequest("GET", "http://example.test", nil)
	request.Header.Set("Authorization", "Bearer wrong")
	identity, authenticated, err := provider.Authenticate(context.Background(), request)
	if err != nil || authenticated || identity != "" {
		t.Fatalf("invalid credential = %q/%t/%v, want empty/false/nil", identity, authenticated, err)
	}
	if err := rotator.Rotate("", "", time.Time{}); !errors.Is(err, ErrTokenRotationInvalid) {
		t.Fatalf("empty rotation error = %v, want ErrTokenRotationInvalid", err)
	}
}
