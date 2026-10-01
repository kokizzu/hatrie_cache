package hatPeer

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"

	"hatrie_cache/hat/hatTopology"
)

func TestCompactPeerConfigWatchDecoderRejectsMalformedDeletedEvent(t *testing.T) {
	payload := []byte{compactPeerConfigWatchVersion, compactPeerConfigWatchSuccess}
	payload = binary.AppendUvarint(payload, 1)
	payload = binary.AppendUvarint(payload, 1)
	payload = binary.AppendUvarint(payload, 1)
	payload = append(payload, 1)
	payload = append(payload, 1, 's', 1, 'k', 1, 'v')
	if _, _, _, err := decodeCompactPeerConfigWatchResponse(payload); !errors.Is(err, ErrCompactPeerConfigWatchResponseInvalid) {
		t.Fatalf("decode malformed deleted event error = %v, want response invalid", err)
	}
}

func TestCompactPeerConfigWatchReplaysPrefixAndAdvancesCursor(t *testing.T) {
	log := newConfigWatchTestLog(t, 8)
	publishConfigWatchTestEvent(t, log, "db/a", "1")
	publishConfigWatchTestEvent(t, log, "cache/a", "x")
	publishConfigWatchTestEvent(t, log, "db/b", "2")

	clientSession, closePair := newConfigWatchTestPair(t, log)
	defer closePair()
	client, err := NewCompactPeerConfigWatchClient(CompactPeerConfigWatchClientOptions{
		Session:   clientSession,
		Principal: "reader",
		KeyPrefix: "db/",
		Limit:     1,
	})
	if err != nil {
		t.Fatalf("NewCompactPeerConfigWatchClient() error = %v", err)
	}
	events, err := client.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(events) != 1 || events[0].Key != "db/a" || client.Cursor() != 1 {
		t.Fatalf("first read = %#v, cursor %d, want db/a and 1", events, client.Cursor())
	}
	events, err = client.Read(context.Background())
	if err != nil {
		t.Fatalf("second Read() error = %v", err)
	}
	if len(events) != 1 || events[0].Key != "db/b" || client.Cursor() != 3 {
		t.Fatalf("second read = %#v, cursor %d, want db/b and 3", events, client.Cursor())
	}
}

func TestCompactPeerConfigWatchReportsGapAndCanReset(t *testing.T) {
	log := newConfigWatchTestLog(t, 1)
	publishConfigWatchTestEvent(t, log, "db/a", "1")
	publishConfigWatchTestEvent(t, log, "db/b", "2")
	clientSession, closePair := newConfigWatchTestPair(t, log)
	defer closePair()
	client, err := NewCompactPeerConfigWatchClient(CompactPeerConfigWatchClientOptions{
		Session:   clientSession,
		Principal: "reader",
		KeyPrefix: "db/",
	})
	if err != nil {
		t.Fatalf("NewCompactPeerConfigWatchClient() error = %v", err)
	}
	if _, err := client.Read(context.Background()); !errors.Is(err, hatTopology.ErrConfigWatchHistoryGap) {
		t.Fatalf("Read() error = %v, want history gap", err)
	}
	var gap *hatTopology.ConfigWatchGapError
	if !errors.As(client.LastError(), &gap) || gap.EarliestVersion != 2 {
		t.Fatalf("LastError() = %v, want earliest version 2 gap", client.LastError())
	}
	if err := client.ResetCursor(gap.EarliestVersion - 1); err != nil {
		t.Fatalf("ResetCursor() error = %v", err)
	}
	events, err := client.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() after reset error = %v", err)
	}
	if len(events) != 1 || events[0].Key != "db/b" || client.Cursor() != 2 {
		t.Fatalf("read after reset = %#v, cursor %d, want db/b and 2", events, client.Cursor())
	}
}

func TestCompactPeerConfigWatchReconnectsFromCursor(t *testing.T) {
	log := newConfigWatchTestLog(t, 8)
	publishConfigWatchTestEvent(t, log, "db/a", "1")
	clientSession, closePair := newConfigWatchTestPair(t, log)
	defer closePair()
	var reconnectMu sync.Mutex
	var reconnectClose func()
	client, err := NewCompactPeerConfigWatchClient(CompactPeerConfigWatchClientOptions{
		Session:   clientSession,
		Principal: "reader",
		KeyPrefix: "db/",
		Reconnect: func(ctx context.Context) (*CompactPeerSession, error) {
			reconnectMu.Lock()
			defer reconnectMu.Unlock()
			if reconnectClose != nil {
				reconnectClose()
			}
			client, server := net.Pipe()
			serverSession, err := NewCompactPeerSession(server, CompactPeerSessionOptions{
				Handler: NewCompactPeerConfigWatchHandler(log),
			})
			if err != nil {
				return nil, err
			}
			reconnectClose = func() {
				_ = serverSession.Close()
				_ = client.Close()
			}
			returnClient, err := NewCompactPeerSession(client, CompactPeerSessionOptions{})
			if err != nil {
				reconnectClose()
				return nil, err
			}
			_ = ctx
			return returnClient, nil
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerConfigWatchClient() error = %v", err)
	}
	events, err := client.Read(context.Background())
	if err != nil || len(events) != 1 || client.Cursor() != 1 {
		t.Fatalf("initial Read() = %#v/%v cursor %d", events, err, client.Cursor())
	}
	publishConfigWatchTestEvent(t, log, "db/b", "2")
	_ = clientSession.Close()
	events, err = client.Read(context.Background())
	if err != nil {
		t.Fatalf("reconnected Read() error = %v", err)
	}
	if len(events) != 1 || events[0].Key != "db/b" || client.Cursor() != 2 {
		t.Fatalf("reconnected read = %#v, cursor %d, want db/b and 2", events, client.Cursor())
	}
}

func newConfigWatchTestLog(t *testing.T, history int) *hatTopology.ConfigWatchLog {
	t.Helper()
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		HistoryLimit: history,
		Authorizer: func(context.Context, hatTopology.ConfigWatchAuthorization) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewConfigWatchLog() error = %v", err)
	}
	return log
}

func publishConfigWatchTestEvent(t *testing.T, log *hatTopology.ConfigWatchLog, key, value string) {
	t.Helper()
	if err := log.Publish(context.Background(), "writer", hatTopology.ConfigWatchEvent{Source: "test", Key: key, Value: []byte(value)}); err != nil {
		t.Fatalf("Publish(%q) error = %v", key, err)
	}
}

func newConfigWatchTestPair(t *testing.T, log *hatTopology.ConfigWatchLog) (*CompactPeerSession, func()) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: NewCompactPeerConfigWatchHandler(log),
	})
	if err != nil {
		_ = clientConn.Close()
		_ = serverConn.Close()
		t.Fatalf("NewCompactPeerSession(server) error = %v", err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{})
	if err != nil {
		_ = server.Close()
		t.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}
	return client, func() {
		_ = client.Close()
		_ = server.Close()
	}
}
