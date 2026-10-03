package hatPeer

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestT027RemoteWatchWireValidation(t *testing.T) {
	valid := CompactPeerWatchRequest{Prefix: "cfg:", Buffer: 4, Coalesce: true}
	payload, err := encodeCompactPeerWatchRegistration(7, valid)
	if err != nil {
		t.Fatal(err)
	}
	id, decoded, err := decodeCompactPeerWatchRegistration(payload)
	if err != nil || id != 7 || decoded.Prefix != valid.Prefix || !decoded.Coalesce {
		t.Fatalf("decoded id=%d request=%#v err=%v", id, decoded, err)
	}
	for _, malformed := range [][]byte{
		{},
		{compactPeerWatchWireVersion},
		{compactPeerWatchWireVersion, compactPeerWatchFlagKey | compactPeerWatchFlagPrefix},
		append([]byte(nil), payload[:len(payload)-1]...),
	} {
		if _, _, err := decodeCompactPeerWatchRegistration(malformed); !errors.Is(err, ErrCompactPeerWatchProtocol) && !errors.Is(err, ErrCompactPeerWatchOptionsInvalid) {
			t.Fatalf("decode malformed payload error = %v", err)
		}
	}
	if _, err := normalizeCompactPeerWatchRequest(CompactPeerWatchRequest{Key: "a", Prefix: "b"}); !errors.Is(err, ErrCompactPeerWatchOptionsInvalid) {
		t.Fatalf("ambiguous request error = %v", err)
	}
	if _, err := normalizeCompactPeerWatchRequest(CompactPeerWatchRequest{Prefix: "cfg:", Buffer: MaxCompactPeerWatchBuffer + 1}); !errors.Is(err, ErrCompactPeerWatchOptionsInvalid) {
		t.Fatalf("oversized buffer error = %v", err)
	}
}

func TestT027RemoteWatchClientRejectsInvalidProviderAndLimits(t *testing.T) {
	if _, err := NewCompactPeerWatchServer(nil, CompactPeerWatchServerOptions{}); !errors.Is(err, ErrCompactPeerWatchProviderRequired) {
		t.Fatalf("nil provider error = %v", err)
	}
	if _, err := NewCompactPeerWatchClient(nil, CompactPeerWatchClientOptions{MaxWatches: maxCompactPeerWatchMaxWatches + 1}); !errors.Is(err, ErrCompactPeerWatchOptionsInvalid) {
		t.Fatalf("oversized client limit error = %v", err)
	}
}

func TestT027RemoteWatchServerPreservesOrdinaryHandler(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerWatchServer(serverConn, CompactPeerWatchServerOptions{
		Provider: t027EmptyWatchProvider{},
		Session: CompactPeerSessionOptions{
			Handler: func(_ context.Context, frame CompactFrame) (CompactFrame, error) {
				return CompactFrame{Payload: append([]byte("ok:"), frame.Payload...)}, nil
			},
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
	defer server.Close()
	defer client.Close()
	response, err := client.Call(context.Background(), []byte("ordinary"), []byte("payload"))
	if err != nil || string(response.Payload) != "ok:payload" {
		t.Fatalf("ordinary response=%q err=%v", response.Payload, err)
	}
}

type t027EmptyWatchProvider struct{}

func (t027EmptyWatchProvider) OpenCompactPeerWatch(context.Context, CompactPeerWatchRequest) (CompactPeerWatchSource, error) {
	return t027EmptyWatchSource{}, nil
}

type t027EmptyWatchSource struct{}

func (t027EmptyWatchSource) Events() <-chan CompactPeerWatchEvent {
	return make(chan CompactPeerWatchEvent)
}
func (t027EmptyWatchSource) Close() error { return nil }
