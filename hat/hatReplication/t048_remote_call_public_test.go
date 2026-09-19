package hatReplication_test

import (
    "context"
    "testing"

    "hatrie_cache/hat/hatReplication"
)

func TestT048RemoteCallPolicyIsUsableByImporters(t *testing.T) {
    value, err := hatReplication.RetryRemoteCall(context.Background(), func(_ context.Context, request hatReplication.RemoteCallRequest) (int, error) {
        if request.Attempt != 1 || request.Method != hatReplication.RemoteCallRead {
            t.Fatalf("request = %#v, want first read attempt", request)
        }
        return 42, nil
    }, hatReplication.RemoteCallPolicy{Method: hatReplication.RemoteCallRead, MaxAttempts: 1})
    if err != nil || value != 42 {
        t.Fatalf("RetryRemoteCall() = %d/%v, want 42/nil", value, err)
    }
}
