package hatPeer

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
)

func TestT241RoleAuthorizerMatchesRoleCommandAndSpace(t *testing.T) {
	authorizer, err := NewCompactPeerRoleAuthorizer(CompactPeerRoleAuthorizerOptions{
		Rules: []CompactPeerRoleRule{
			{Role: "reader", Command: "get", Space: "tenant-a"},
			{Role: "writer", Command: "put", Space: "*"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		request CompactPeerAuthorizationRequest
		wantErr error
	}{
		{
			name: "reader can read its space",
			request: CompactPeerAuthorizationRequest{
				PeerID:  "peer-a",
				Roles:   []string{"reader"},
				Command: []byte("get"),
				Space:   "tenant-a",
			},
		},
		{
			name: "writer can write every space",
			request: CompactPeerAuthorizationRequest{
				Roles:   []string{"writer"},
				Command: []byte("put"),
				Space:   "tenant-b",
			},
		},
		{
			name: "reader cannot use another space",
			request: CompactPeerAuthorizationRequest{
				Roles:   []string{"reader"},
				Command: []byte("get"),
				Space:   "tenant-b",
			},
			wantErr: ErrCompactPeerRequestDenied,
		},
		{
			name: "reader cannot write",
			request: CompactPeerAuthorizationRequest{
				Roles:   []string{"reader"},
				Command: []byte("put"),
				Space:   "tenant-a",
			},
			wantErr: ErrCompactPeerRequestDenied,
		},
		{
			name: "unknown role denied",
			request: CompactPeerAuthorizationRequest{
				Roles:   []string{"operator"},
				Command: []byte("get"),
				Space:   "tenant-a",
			},
			wantErr: ErrCompactPeerRequestDenied,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			err := authorizer.Authorize(context.Background(), test.request)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Authorize() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestT241RoleAuthorizerValidatesRulesAndCancellation(t *testing.T) {
	if _, err := NewCompactPeerRoleAuthorizer(CompactPeerRoleAuthorizerOptions{}); !errors.Is(err, ErrCompactPeerRolePolicyInvalid) {
		t.Fatalf("empty policy error = %v, want policy error", err)
	}
	for _, rule := range []CompactPeerRoleRule{
		{Command: "get", Space: "tenant-a"},
		{Role: "reader", Space: "tenant-a"},
	} {
		if _, err := NewCompactPeerRoleAuthorizer(CompactPeerRoleAuthorizerOptions{Rules: []CompactPeerRoleRule{rule}}); !errors.Is(err, ErrCompactPeerRolePolicyInvalid) {
			t.Fatalf("invalid rule %#v error = %v, want policy error", rule, err)
		}
	}
	authorizer, err := NewCompactPeerRoleAuthorizer(CompactPeerRoleAuthorizerOptions{
		Rules: []CompactPeerRoleRule{{Role: "reader", Command: "get"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := authorizer.Authorize(ctx, CompactPeerAuthorizationRequest{
		Roles: []string{"reader"}, Command: []byte("get"),
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Authorize() error = %v, want context.Canceled", err)
	}
}

func TestT241SessionAuthorizesBeforeHandlerAndExtractsSpace(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	var handled atomic.Int32
	authorizer, err := NewCompactPeerRoleAuthorizer(CompactPeerRoleAuthorizerOptions{
		Rules: []CompactPeerRoleRule{{Role: "reader", Command: "get", Space: "tenant-a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		PeerID: "peer-a",
		Roles:  []string{"reader"},
		SpaceExtractor: func(_ context.Context, frame CompactFrame) (string, error) {
			return string(frame.Payload), nil
		},
		AuthorizeRequest: authorizer.Authorize,
		Handler: func(_ context.Context, request CompactFrame) (CompactFrame, error) {
			handled.Add(1)
			return CompactFrame{Payload: request.Payload}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{})
	if err != nil {
		_ = server.Close()
		t.Fatal(err)
	}
	defer func() {
		_ = client.Close()
		_ = server.Close()
	}()

	if response, err := client.Call(context.Background(), []byte("get"), []byte("tenant-a")); err != nil || string(response.Payload) != "tenant-a" {
		t.Fatalf("authorized call response=%q error=%v", response.Payload, err)
	}
	if _, err := client.Call(context.Background(), []byte("get"), []byte("tenant-b")); err == nil || !strings.Contains(err.Error(), ErrCompactPeerRequestDenied.Error()) {
		t.Fatalf("unauthorized call error = %v, want request denial", err)
	}
	if got := handled.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want one authorized request", got)
	}
}

func BenchmarkT241RoleAuthorizer(b *testing.B) {
	authorizer, err := NewCompactPeerRoleAuthorizer(CompactPeerRoleAuthorizerOptions{
		Rules: []CompactPeerRoleRule{{Role: "reader", Command: "get", Space: "tenant-a"}},
	})
	if err != nil {
		b.Fatal(err)
	}
	request := CompactPeerAuthorizationRequest{
		Roles: []string{"reader"}, Command: []byte("get"), Space: "tenant-a",
	}
	b.ReportAllocs()
	for range b.N {
		if err := authorizer.Authorize(context.Background(), request); err != nil {
			b.Fatal(err)
		}
	}
}
