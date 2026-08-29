package hatAuth

import (
	"context"
	"net/http"
	"strings"
	"time"
)

// IdentityProvider authenticates an HTTP request and returns its principal.
// A provider that does not recognize a request returns authenticated as false.
type IdentityProvider interface {
	Authenticate(context.Context, *http.Request) (identity string, authenticated bool, err error)
}

// IdentityChain tries providers in order and returns the first authenticated
// identity. Configure a reverse-proxy provider only on a listener whose proxy
// boundary removes client-supplied identity headers.
type IdentityChain struct {
	Providers []IdentityProvider
}

func (chain IdentityChain) Authenticate(ctx context.Context, request *http.Request) (string, bool, error) {
	for _, provider := range chain.Providers {
		if provider == nil {
			continue
		}
		identity, authenticated, err := provider.Authenticate(ctx, request)
		if err != nil || authenticated {
			return identity, authenticated, err
		}
	}
	return "", false, nil
}

// LocalTokenIdentity authenticates Bearer tokens against a rotating TokenSet.
// Now is optional and exists to make token-expiry tests deterministic.
type LocalTokenIdentity struct {
	Tokens TokenSet
	Now    func() time.Time
}

func (provider LocalTokenIdentity) Authenticate(_ context.Context, request *http.Request) (string, bool, error) {
	if !provider.Tokens.Configured() || request == nil {
		return "", false, nil
	}
	token := BearerToken(request.Header.Get("Authorization"))
	if token == "" {
		return "", false, nil
	}
	now := time.Now()
	if provider.Now != nil {
		now = provider.Now()
	}
	if !provider.Tokens.Matches(token, now) {
		return "", false, nil
	}
	return token, true, nil
}

// OIDCIdentityFunc validates a Bearer token with an OIDC implementation.
// It is a function adapter so OIDC clients stay outside the core package.
type OIDCIdentityFunc func(context.Context, string) (identity string, authenticated bool, err error)

func (validator OIDCIdentityFunc) Authenticate(ctx context.Context, request *http.Request) (string, bool, error) {
	if validator == nil || request == nil {
		return "", false, nil
	}
	token := BearerToken(request.Header.Get("Authorization"))
	if token == "" {
		return "", false, nil
	}
	return validator(ctx, token)
}

// ReverseProxyIdentity trusts a normalized identity header injected by a
// reverse proxy. It must only be used behind a proxy that strips this header
// from incoming client requests.
type ReverseProxyIdentity struct {
	Header string
}

func (provider ReverseProxyIdentity) Authenticate(_ context.Context, request *http.Request) (string, bool, error) {
	if request == nil || provider.Header == "" {
		return "", false, nil
	}
	identity := strings.TrimSpace(request.Header.Get(provider.Header))
	if identity == "" {
		return "", false, nil
	}
	return identity, true, nil
}
