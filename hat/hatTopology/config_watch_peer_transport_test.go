package hatTopology_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"hatrie_cache/hat/hatPeer"
	"hatrie_cache/hat/hatTopology"
)

func TestConfigWatchPeerCompactSession(t *testing.T) {
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		Authorizer: func(context.Context, hatTopology.ConfigWatchAuthorization) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewConfigWatchLog() error = %v", err)
	}
	serverConn, clientConn := net.Pipe()
	server, err := hatPeer.NewCompactPeerSession(serverConn, hatPeer.CompactPeerSessionOptions{
		EnableRequestCancellation: true,
		Handler: func(ctx context.Context, frame hatPeer.CompactFrame) (hatPeer.CompactFrame, error) {
			if string(frame.Command) != hatTopology.ConfigWatchPeerCommand {
				return hatPeer.CompactFrame{}, errors.New("unexpected command")
			}
			payload, handleErr := log.HandleConfigWatchPeerRequestForPrincipal(ctx, "ops", frame.Payload)
			if handleErr != nil {
				return hatPeer.CompactFrame{}, handleErr
			}
			return hatPeer.CompactFrame{Kind: hatPeer.CompactResponse, Payload: payload}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(server) error = %v", err)
	}
	clientSession, err := hatPeer.NewCompactPeerSession(clientConn, hatPeer.CompactPeerSessionOptions{
		EnableRequestCancellation: true,
	})
	if err != nil {
		_ = server.Close()
		t.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}
	t.Cleanup(func() {
		_ = clientSession.Close()
		_ = server.Close()
	})
	client, err := hatTopology.NewConfigWatchPeerClient(hatTopology.ConfigWatchPeerClientOptions{
		Call: func(ctx context.Context, command, payload []byte) ([]byte, error) {
			frame, callErr := clientSession.Call(ctx, command, payload)
			return frame.Payload, callErr
		},
		Principal: "ops",
		Prefix:    "feature/",
	})
	if err != nil {
		t.Fatalf("NewConfigWatchPeerClient() error = %v", err)
	}
	if err := log.Publish(context.Background(), "ops", hatTopology.ConfigWatchEvent{
		Version: 1,
		Source:  "node-a",
		Key:     "feature/peer",
		Value:   []byte("on"),
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	events, cursor, err := client.Read(context.Background(), 0, 1)
	if err != nil || len(events) != 1 || events[0].Version != 1 || cursor != 1 {
		t.Fatalf("Read() = events=%#v cursor=%d err=%v, want version 1", events, cursor, err)
	}
}
