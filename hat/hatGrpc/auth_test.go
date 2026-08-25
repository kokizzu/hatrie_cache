package hatGrpc

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"hatrie_cache/hat/hatAuth"
)

func TestPrincipalAcceptsHeaderAndBearerTokens(t *testing.T) {
	tokens := hatAuth.NewTokenSet("current", "previous", time.Now().Add(time.Hour))
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-hatrie-auth-token", "current"))
	if got := Principal(ctx, tokens, "x-hatrie-auth-token"); got != "current" {
		t.Fatalf("header principal = %q, want current", got)
	}
	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer previous"))
	if got := Principal(ctx, tokens, "x-hatrie-auth-token"); got != "previous" {
		t.Fatalf("bearer principal = %q, want previous", got)
	}
}
