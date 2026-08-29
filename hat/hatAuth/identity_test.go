package hatAuth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestIdentityChainUsesTokenOIDCAndTrustedProxyHeader(t *testing.T) {
	chain := IdentityChain{Providers: []IdentityProvider{
		LocalTokenIdentity{Tokens: NewTokenSet("local", "", time.Time{})},
		OIDCIdentityFunc(func(context.Context, string) (string, bool, error) { return "oidc-user", true, nil }),
		ReverseProxyIdentity{Header: "X-Forwarded-User"},
	}}
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header = http.Header{"Authorization": []string{"Bearer local"}, "X-Forwarded-User": []string{"proxy-user"}}
	if identity, ok, err := chain.Authenticate(request.Context(), request); err != nil || !ok || identity != "local" {
		t.Fatalf("token identity = %q, %v, %v", identity, ok, err)
	}
	request = httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header = http.Header{"Authorization": []string{"Bearer oidc"}}
	if identity, ok, err := chain.Authenticate(request.Context(), request); err != nil || !ok || identity != "oidc-user" {
		t.Fatalf("OIDC identity = %q, %v, %v", identity, ok, err)
	}
	request = httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header = http.Header{"X-Forwarded-User": []string{" proxy-user "}}
	if identity, ok, err := chain.Authenticate(request.Context(), request); err != nil || !ok || identity != "proxy-user" {
		t.Fatalf("proxy identity = %q, %v, %v", identity, ok, err)
	}
}
