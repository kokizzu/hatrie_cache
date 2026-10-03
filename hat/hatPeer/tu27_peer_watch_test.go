package hatPeer

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
)

func TestTU27CompactPeerConfigWatchRoundTripAndReconnect(t *testing.T) {
	server, err := NewCompactPeerConfigWatchServer(CompactPeerConfigWatchOptions{MaxHistory: 8, MaxEvents: 2})
	if err != nil {
		t.Fatalf("new watch server: %v", err)
	}
	if _, err := server.Publish("region/us", []byte("v1")); err != nil {
		t.Fatalf("publish first event: %v", err)
	}

	clientSession, serverSession := newTU27PeerPair(t, server.Handler(nil))
	client, err := NewCompactPeerConfigWatchClient(CompactPeerConfigWatchClientOptions{Prefix: "region/"})
	if err != nil {
		t.Fatalf("new watch client: %v", err)
	}
	ctx := context.Background()
	state, err := client.Open(ctx, clientSession)
	if err != nil {
		t.Fatalf("open watch: %v", err)
	}
	if state.Revision != 1 || state.OldestRevision != 1 {
		t.Fatalf("unexpected initial state: %+v", state)
	}

	events, err := client.Poll(ctx)
	if err != nil {
		t.Fatalf("poll first event: %v", err)
	}
	assertTU27Events(t, events, []CompactPeerConfigWatchEvent{{Revision: 1, Path: "region/us", Value: []byte("v1")}})

	if _, err := server.Publish("region/eu", []byte("v2")); err != nil {
		t.Fatalf("publish matching event: %v", err)
	}
	if _, err := server.Publish("other/key", []byte("ignored")); err != nil {
		t.Fatalf("publish non-matching event: %v", err)
	}
	events, err = client.Poll(ctx)
	if err != nil {
		t.Fatalf("poll filtered event: %v", err)
	}
	assertTU27Events(t, events, []CompactPeerConfigWatchEvent{{Revision: 2, Path: "region/eu", Value: []byte("v2")}})

	if err := clientSession.Close(); err != nil {
		t.Fatalf("close first client session: %v", err)
	}
	if err := serverSession.Close(); err != nil {
		t.Fatalf("close first server session: %v", err)
	}

	reconnectedClientSession, reconnectedServerSession := newTU27PeerPair(t, server.Handler(nil))
	state, err = client.Reconnect(ctx, reconnectedClientSession)
	if err != nil {
		t.Fatalf("reconnect watch: %v", err)
	}
	if state.Revision != 3 || client.Revision() != 3 {
		t.Fatalf("unexpected reconnect state: state=%+v revision=%d", state, client.Revision())
	}

	events, err = client.Poll(ctx)
	if err != nil {
		t.Fatalf("replay after reconnect: %v", err)
	}
	assertTU27Events(t, events, nil)
	if _, err := server.Publish("region/ap", []byte("v4")); err != nil {
		t.Fatalf("publish after reconnect: %v", err)
	}
	events, err = client.Poll(ctx)
	if err != nil {
		t.Fatalf("poll after reconnect: %v", err)
	}
	assertTU27Events(t, events, []CompactPeerConfigWatchEvent{{Revision: 4, Path: "region/ap", Value: []byte("v4")}})

	if err := client.Close(ctx); err != nil {
		t.Fatalf("close watch client: %v", err)
	}
	if err := reconnectedClientSession.Close(); err != nil {
		t.Fatalf("close reconnected client session: %v", err)
	}
	if err := reconnectedServerSession.Close(); err != nil {
		t.Fatalf("close reconnected server session: %v", err)
	}
}

func TestTU27CompactPeerConfigWatchBoundsAndHistoryGap(t *testing.T) {
	server, err := NewCompactPeerConfigWatchServer(CompactPeerConfigWatchOptions{
		MaxHistory:     2,
		MaxEvents:      1,
		MaxPrefixBytes: 4,
		MaxPathBytes:   8,
		MaxValueBytes:  3,
	})
	if err != nil {
		t.Fatalf("new bounded watch server: %v", err)
	}
	if _, err := server.Publish("", []byte("ok")); !errors.Is(err, ErrCompactPeerConfigWatchPathInvalid) {
		t.Fatalf("empty path error = %v", err)
	}
	if _, err := server.Publish("toolong!!", []byte("ok")); !errors.Is(err, ErrCompactPeerConfigWatchPathTooLarge) {
		t.Fatalf("long path error = %v", err)
	}
	if _, err := server.Publish("key", []byte("toolong")); !errors.Is(err, ErrCompactPeerConfigWatchValueTooLarge) {
		t.Fatalf("large value error = %v", err)
	}
	for _, value := range []string{"v1", "v2", "v3"} {
		if _, err := server.Publish("key", []byte(value)); err != nil {
			t.Fatalf("publish %q: %v", value, err)
		}
	}

	clientSession, serverSession := newTU27PeerPair(t, server.Handler(nil))
	defer clientSession.Close()
	defer serverSession.Close()
	client, err := NewCompactPeerConfigWatchClient(CompactPeerConfigWatchClientOptions{Prefix: ""})
	if err != nil {
		t.Fatalf("new bounded watch client: %v", err)
	}
	_, err = client.Open(context.Background(), clientSession)
	if err == nil || !errors.Is(err, ErrCompactPeerRemote) {
		t.Fatalf("history gap error = %v", err)
	}

	if _, err := NewCompactPeerConfigWatchClient(CompactPeerConfigWatchClientOptions{Prefix: "12345", MaxPrefixBytes: 4}); !errors.Is(err, ErrCompactPeerConfigWatchPrefixTooLarge) {
		t.Fatalf("large client prefix error = %v", err)
	}
}

func TestTU27CompactPeerConfigWatchWireRoundTrip(t *testing.T) {
	events := []CompactPeerConfigWatchEvent{{Revision: 7, Path: "region/us", Value: []byte("enabled")}}
	encoded, err := marshalCompactPeerConfigWatchEvents(9, events)
	if err != nil {
		t.Fatalf("marshal events: %v", err)
	}
	latest, decoded, err := unmarshalCompactPeerConfigWatchEvents(encoded, CompactPeerConfigWatchOptions{})
	if err != nil {
		t.Fatalf("unmarshal events: %v", err)
	}
	if latest != 9 {
		t.Fatalf("latest revision = %d", latest)
	}
	assertTU27Events(t, decoded, events)
}

func TestTU27CompactPeerConfigWatchRejectsMalformedPayload(t *testing.T) {
	for _, payload := range [][]byte{
		{},
		{compactPeerConfigWatchVersion, 1},
		{compactPeerConfigWatchVersion, 1, 0, 99},
		{compactPeerConfigWatchVersion, 1, 1, 1, 1, 'x', 1},
	} {
		if _, _, err := unmarshalCompactPeerConfigWatchEvents(payload, CompactPeerConfigWatchOptions{}); err == nil {
			t.Fatalf("malformed payload accepted: %v", payload)
		}
	}
}

func TestTU27CompactPeerConfigWatchWireSize(t *testing.T) {
	jsonPayload, err := json.Marshal(tu27ConfigWatchJSONBenchmarkEvent{
		Revision: tu27ConfigWatchBenchmarkEvent.Revision,
		Path:     tu27ConfigWatchBenchmarkEvent.Path,
		Value:    tu27ConfigWatchBenchmarkEvent.Value,
	})
	if err != nil {
		t.Fatalf("marshal json payload: %v", err)
	}
	binaryPayload, err := marshalCompactPeerConfigWatchEvents(tu27ConfigWatchBenchmarkEvent.Revision, []CompactPeerConfigWatchEvent{tu27ConfigWatchBenchmarkEvent})
	if err != nil {
		t.Fatalf("marshal binary payload: %v", err)
	}
	if len(binaryPayload) >= len(jsonPayload) {
		t.Fatalf("binary payload is not smaller: binary=%d json=%d", len(binaryPayload), len(jsonPayload))
	}
	t.Logf("json=%d binary=%d reduction=%.2fx", len(jsonPayload), len(binaryPayload), float64(len(jsonPayload))/float64(len(binaryPayload)))
}

func newTU27PeerPair(t *testing.T, handler CompactPeerHandler) (*CompactPeerSession, *CompactPeerSession) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{Handler: handler, MaxInFlight: 4})
	if err != nil {
		clientConn.Close()
		serverConn.Close()
		t.Fatalf("new server session: %v", err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{MaxInFlight: 4})
	if err != nil {
		server.Close()
		clientConn.Close()
		t.Fatalf("new client session: %v", err)
	}
	return client, server
}

func assertTU27Events(t *testing.T, got, want []CompactPeerConfigWatchEvent) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("event count = %d, want %d: %#v", len(got), len(want), got)
	}
	for index := range want {
		if got[index].Revision != want[index].Revision || got[index].Path != want[index].Path || string(got[index].Value) != string(want[index].Value) {
			t.Fatalf("event[%d] = %+v, want %+v", index, got[index], want[index])
		}
	}
}
