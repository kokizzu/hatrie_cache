package hatReplication_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func TestSelectReadReplicaWithConsistencyLevels(t *testing.T) {
	candidates := []hatReplication.ReadReplicaProgress{{Node: "stale", Frontier: 8, HealthScore: 100}}
	policy := hatReplication.ReadReplicaPolicy{ObservedFrontier: 10, RequiredFrontier: 9, MaxLag: 1}

	selected, err := hatReplication.SelectReadReplicaWithConsistency(candidates, policy, hatReplication.ReadConsistencyEventual)
	if err != nil || selected.Node != "stale" {
		t.Fatalf("eventual selection = %#v, %v, want stale", selected, err)
	}
	if _, err := hatReplication.SelectReadReplicaWithConsistency(candidates, policy, hatReplication.ReadConsistencyBoundedStaleness); !errors.Is(err, hatReplication.ErrNoEligibleReadReplica) {
		t.Fatalf("bounded-staleness error = %v, want ErrNoEligibleReadReplica", err)
	}
	if _, err := hatReplication.SelectReadReplicaWithConsistency(candidates, policy, hatReplication.ReadConsistencyReadAfterWrite); !errors.Is(err, hatReplication.ErrNoEligibleReadReplica) {
		t.Fatalf("read-after-write error = %v, want ErrNoEligibleReadReplica", err)
	}

	selected, err = hatReplication.SelectReadReplicaWithConsistency(
		[]hatReplication.ReadReplicaProgress{{Node: "session", Frontier: 9}},
		policy,
		hatReplication.ReadConsistencyReadAfterWrite,
	)
	if err != nil || selected.Node != "session" {
		t.Fatalf("read-after-write selection = %#v, %v, want session", selected, err)
	}
	if _, err := hatReplication.SelectReadReplicaWithConsistency(candidates, policy, hatReplication.ReadConsistencyLevel("invalid")); !errors.Is(err, hatReplication.ErrReadConsistencyInvalid) {
		t.Fatalf("invalid consistency error = %v, want ErrReadConsistencyInvalid", err)
	}
}

func TestSelectReadReplicaRetainsReadAfterWriteCompatibility(t *testing.T) {
	candidates := []hatReplication.ReadReplicaProgress{{Node: "fresh", Frontier: 10}}
	policy := hatReplication.ReadReplicaPolicy{ObservedFrontier: 10, MaxLag: 0}
	legacy, err := hatReplication.SelectReadReplica(candidates, policy)
	if err != nil {
		t.Fatalf("SelectReadReplica() error = %v", err)
	}
	level, err := hatReplication.SelectReadReplicaWithConsistency(candidates, policy, hatReplication.ReadConsistencyReadAfterWrite)
	if err != nil || legacy != level {
		t.Fatalf("legacy = %#v, explicit read-after-write = %#v, error = %v", legacy, level, err)
	}
}

func TestParseReadConsistencyLevel(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  hatReplication.ReadConsistencyLevel
	}{
		{name: "empty defaults to read after write", want: hatReplication.ReadConsistencyReadAfterWrite},
		{name: "eventual", input: " EVENTUAL ", want: hatReplication.ReadConsistencyEventual},
		{name: "bounded alias", input: "bounded", want: hatReplication.ReadConsistencyBoundedStaleness},
		{name: "session alias", input: "session", want: hatReplication.ReadConsistencyReadAfterWrite},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := hatReplication.ParseReadConsistencyLevel(test.input)
			if err != nil || got != test.want {
				t.Fatalf("ParseReadConsistencyLevel(%q) = %q, %v; want %q", test.input, got, err, test.want)
			}
		})
	}
	if _, err := hatReplication.ParseReadConsistencyLevel("linearizable"); !errors.Is(err, hatReplication.ErrReadConsistencyInvalid) {
		t.Fatalf("invalid parse error = %v, want ErrReadConsistencyInvalid", err)
	}
}

func TestSelectReadReplicaWithConsistencySeparatesBoundedStalenessFromRequiredFrontier(t *testing.T) {
	policy := hatReplication.ReadReplicaPolicy{
		ObservedFrontier: 10,
		RequiredFrontier: 100,
		MaxLag:           1,
	}
	selected, err := hatReplication.SelectReadReplicaWithConsistency(
		[]hatReplication.ReadReplicaProgress{{Node: "bounded", Frontier: 9}},
		policy,
		hatReplication.ReadConsistencyBoundedStaleness,
	)
	if err != nil || selected.Node != "bounded" {
		t.Fatalf("bounded-staleness selection = %#v, %v; want bounded", selected, err)
	}
}

func TestSelectReadReplicaWithConsistencyRejectsBlankNode(t *testing.T) {
	_, err := hatReplication.SelectReadReplicaWithConsistency(
		[]hatReplication.ReadReplicaProgress{{Node: "  ", Frontier: 10}},
		hatReplication.ReadReplicaPolicy{},
		hatReplication.ReadConsistencyEventual,
	)
	if !errors.Is(err, hatReplication.ErrReadReplicaNameRequired) {
		t.Fatalf("blank node error = %v, want ErrReadReplicaNameRequired", err)
	}
}
