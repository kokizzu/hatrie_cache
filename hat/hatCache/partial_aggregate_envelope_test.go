package hatCache_test

import (
	"testing"

	hatCache "hatrie_cache/hat/hatCache"
	hatDataStructure "hatrie_cache/hat/hatDataStructure"
)

func TestCountMinSketchAggregateStateRoundTrip(t *testing.T) {
	want, err := hatCache.NewCountMinSketch(32, 3)
	if err != nil {
		t.Fatalf("NewCountMinSketch() error = %v", err)
	}
	want.Add("alpha", 3)
	want.Add("beta", 2)
	want.Add("alpha", 4)

	wire, err := want.MarshalAggregateState()
	if err != nil {
		t.Fatalf("MarshalAggregateState() error = %v", err)
	}
	envelope, err := hatDataStructure.UnmarshalAggregateStateEnvelope(wire)
	if err != nil {
		t.Fatalf("UnmarshalAggregateStateEnvelope() error = %v", err)
	}
	if envelope.Kind != hatDataStructure.AggregateStateKindCountMinSketch || envelope.Version != 1 {
		t.Fatalf("envelope metadata = %#v, want CountMinSketch v1", envelope)
	}

	got, err := hatCache.NewCountMinSketchFromAggregateState(wire)
	if err != nil {
		t.Fatalf("NewCountMinSketchFromAggregateState() error = %v", err)
	}
	if got.Info() != want.Info() || got.Snapshot() != want.Snapshot() {
		t.Fatalf("round trip info/snapshot = %#v/%#v, want %#v/%#v", got.Info(), got.Snapshot(), want.Info(), want.Snapshot())
	}

	empty, err := hatCache.NewCountMinSketch(32, 3)
	if err != nil {
		t.Fatalf("NewCountMinSketch(empty) error = %v", err)
	}
	emptyWire, err := empty.MarshalAggregateState()
	if err != nil {
		t.Fatalf("MarshalAggregateState(empty) error = %v", err)
	}
	emptyRoundTrip, err := hatCache.NewCountMinSketchFromAggregateState(emptyWire)
	if err != nil {
		t.Fatalf("NewCountMinSketchFromAggregateState(empty) error = %v", err)
	}
	if emptyRoundTrip.Info() != empty.Info() {
		t.Fatalf("empty round trip info = %#v, want %#v", emptyRoundTrip.Info(), empty.Info())
	}
}
