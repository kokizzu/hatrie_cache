// Package hatGrpc provides portable gRPC transport helpers for hatrie-cache.
package hatGrpc

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
	"hatrie_cache/hat/hatAuth"
)

// Principal returns the authenticated metadata token for header, falling back
// to a standard bearer token. An empty result is unauthenticated.
func Principal(ctx context.Context, tokens hatAuth.TokenSet, header string) string {
	if !tokens.Configured() || ctx == nil {
		return ""
	}
	metadataValues, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	header = strings.TrimSpace(header)
	now := time.Now()
	if header != "" {
		for _, candidate := range metadataValues.Get(header) {
			if tokens.Matches(candidate, now) {
				return candidate
			}
		}
	}
	for _, candidate := range metadataValues.Get("authorization") {
		principal := hatAuth.BearerToken(candidate)
		if tokens.Matches(principal, now) {
			return principal
		}
	}
	return ""
}
