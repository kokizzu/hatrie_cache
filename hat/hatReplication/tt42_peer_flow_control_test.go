package hatReplication_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestTTG42PeerRelayBackpressureKeepsPeerStateIndependent(t *testing.T) {
	controller, err := hatReplication.NewPeerRelayBackpressure(hatReplication.PeerRelayBackpressureOptions{
		Enabled:         true,
		HighWatermark:   10,
		ResumeWatermark: 4,
		MaxPeers:        2,
	})
	if err != nil {
		t.Fatalf("NewPeerRelayBackpressure() error = %v", err)
	}

	west, err := controller.Observe(" west ", 10)
	if err != nil || west.Peer != "west" || west.Allowed || !west.Paused || !west.Transitioned || west.Transitions != 1 {
		t.Fatalf("west high-water decision = %#v, %v", west, err)
	}
	east, err := controller.Observe("east", 3)
	if err != nil || east.Peer != "east" || !east.Allowed || east.Paused || east.Transitioned {
		t.Fatalf("east independent decision = %#v, %v", east, err)
	}
	west, err = controller.Observe("west", 9)
	if err != nil || west.Allowed || !west.Paused || west.Transitioned || west.Transitions != 1 {
		t.Fatalf("west hysteresis decision = %#v, %v", west, err)
	}
	west, err = controller.Observe("west", 4)
	if err != nil || !west.Allowed || west.Paused || !west.Transitioned || west.Transitions != 2 {
		t.Fatalf("west recovery decision = %#v, %v", west, err)
	}
}

func TestTTG42PeerRelayBackpressureBoundsNamesAndPeers(t *testing.T) {
	controller, err := hatReplication.NewPeerRelayBackpressure(hatReplication.PeerRelayBackpressureOptions{
		Enabled:          true,
		MaxPeers:         2,
		MaxPeerNameBytes: 5,
	})
	if err != nil {
		t.Fatalf("NewPeerRelayBackpressure() error = %v", err)
	}
	if _, err := controller.Observe(" ", 1); !errors.Is(err, hatReplication.ErrPeerRelayBackpressurePeerRequired) {
		t.Fatalf("blank peer error = %v", err)
	}
	if _, err := controller.Observe("123456", 1); !errors.Is(err, hatReplication.ErrPeerRelayBackpressurePeerNameTooLong) {
		t.Fatalf("long peer error = %v", err)
	}
	if _, err := controller.Observe("one", 1); err != nil {
		t.Fatalf("first peer error = %v", err)
	}
	if _, err := controller.Observe("two", 1); err != nil {
		t.Fatalf("second peer error = %v", err)
	}
	if _, err := controller.Observe("three", 1); !errors.Is(err, hatReplication.ErrPeerRelayBackpressurePeerLimit) {
		t.Fatalf("third peer error = %v", err)
	}
	if !controller.Remove("one") {
		t.Fatal("Remove(one) = false, want true")
	}
	if _, err := controller.Observe("three", 1); err != nil {
		t.Fatalf("peer after removal error = %v", err)
	}
}

func TestTTG42PeerRelayBackpressureSnapshotIsSortedAndDisabledIsNoOp(t *testing.T) {
	disabled, err := hatReplication.NewPeerRelayBackpressure(hatReplication.PeerRelayBackpressureOptions{})
	if err != nil {
		t.Fatalf("disabled constructor error = %v", err)
	}
	decision, err := disabled.Observe("peer", 100_000)
	if err != nil || !decision.Allowed || decision.Paused || decision.Transitioned {
		t.Fatalf("disabled decision = %#v, %v", decision, err)
	}
	if snapshot := disabled.Snapshot(); len(snapshot.Peers) != 0 || snapshot.Enabled {
		t.Fatalf("disabled snapshot = %#v, want no retained peers", snapshot)
	}

	controller, err := hatReplication.NewPeerRelayBackpressure(hatReplication.PeerRelayBackpressureOptions{
		Enabled:       true,
		HighWatermark: 10,
		MaxPeers:      4,
	})
	if err != nil {
		t.Fatalf("enabled constructor error = %v", err)
	}
	for _, peer := range []string{"zeta", "alpha", "beta"} {
		if _, err := controller.Observe(peer, 1); err != nil {
			t.Fatalf("Observe(%q) error = %v", peer, err)
		}
	}
	snapshot := controller.Snapshot()
	if !snapshot.Enabled || len(snapshot.Peers) != 3 || snapshot.Peers[0].Peer != "alpha" || snapshot.Peers[1].Peer != "beta" || snapshot.Peers[2].Peer != "zeta" {
		t.Fatalf("snapshot = %#v, want sorted peers", snapshot)
	}
	copySnapshot := controller.Snapshot()
	if !reflect.DeepEqual(snapshot, copySnapshot) {
		t.Fatalf("repeated snapshot changed: %#v != %#v", snapshot, copySnapshot)
	}
}
