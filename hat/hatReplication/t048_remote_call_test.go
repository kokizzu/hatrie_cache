package hatReplication

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestT048ReadRetryCarriesStableRequestEnvelope(t *testing.T) {
	var requests []RemoteCallRequest
	value, err := RetryRemoteCall(context.Background(), func(_ context.Context, request RemoteCallRequest) (string, error) {
		requests = append(requests, request)
		if len(requests) < 3 {
			return "", errors.New("temporary read failure")
		}
		return "ok", nil
	}, RemoteCallPolicy{Method: RemoteCallRead, MaxAttempts: 3})
	if err != nil {
		t.Fatalf("RetryRemoteCall() error = %v", err)
	}
	if value != "ok" {
		t.Fatalf("RetryRemoteCall() value = %q, want ok", value)
	}
	if len(requests) != 3 {
		t.Fatalf("calls = %d, want 3", len(requests))
	}
	for index, request := range requests {
		if request.Method != RemoteCallRead || request.IdempotencyKey != "" || request.FencingToken != 0 || request.Attempt != index+1 {
			t.Fatalf("request %d = %#v, want read attempt %d", index, request, index+1)
		}
	}
}

func TestT048IdempotentWriteRequiresIdentityAndEmitsRetryEvent(t *testing.T) {
	var events []RemoteCallRetryEvent
	calls := 0
	value, err := RetryRemoteCall(context.Background(), func(_ context.Context, request RemoteCallRequest) (int, error) {
		calls++
		if request.Method != RemoteCallIdempotentWrite || request.IdempotencyKey != "order-42" || request.FencingToken != 9 {
			t.Fatalf("request = %#v, want stable write identity", request)
		}
		if calls < 2 {
			return 0, errors.New("temporary write failure")
		}
		return 42, nil
	}, RemoteCallPolicy{
		Method:         RemoteCallIdempotentWrite,
		IdempotencyKey: "order-42",
		FencingToken:   9,
		MaxAttempts:    2,
		OnRetry: func(event RemoteCallRetryEvent) {
			events = append(events, event)
		},
	})
	if err != nil || value != 42 {
		t.Fatalf("RetryRemoteCall() = %d/%v, want 42/nil", value, err)
	}
	if len(events) != 1 || events[0].Attempt != 1 || events[0].NextAttempt != 2 || events[0].Method != RemoteCallIdempotentWrite {
		t.Fatalf("retry events = %#v, want one event for attempt 1 -> 2", events)
	}
}

func TestT048RejectsUnsafeRetryPolicies(t *testing.T) {
	cases := []struct {
		name   string
		policy RemoteCallPolicy
		want   error
	}{
		{
			name:   "non-idempotent retry",
			policy: RemoteCallPolicy{Method: RemoteCallNonIdempotent, MaxAttempts: 2},
			want:   ErrRemoteCallNonIdempotent,
		},
		{
			name:   "write without key",
			policy: RemoteCallPolicy{Method: RemoteCallIdempotentWrite, FencingToken: 1},
			want:   ErrRemoteCallPolicyInvalid,
		},
		{
			name:   "write without fencing token",
			policy: RemoteCallPolicy{Method: RemoteCallIdempotentWrite, IdempotencyKey: "order-42"},
			want:   ErrRemoteCallPolicyInvalid,
		},
		{
			name:   "negative backoff",
			policy: RemoteCallPolicy{Method: RemoteCallRead, InitialBackoff: -time.Second},
			want:   ErrRemoteCallPolicyInvalid,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := RetryRemoteCall(context.Background(), func(context.Context, RemoteCallRequest) (int, error) {
				return 0, errors.New("must not call")
			}, test.policy)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestT048ContextCancellationStopsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	_, err := RetryRemoteCall(ctx, func(context.Context, RemoteCallRequest) (int, error) {
		calls++
		cancel()
		return 0, errors.New("temporary failure")
	}, RemoteCallPolicy{
		Method:         RemoteCallRead,
		MaxAttempts:    3,
		InitialBackoff: time.Hour,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}
