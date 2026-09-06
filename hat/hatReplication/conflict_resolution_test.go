package hatReplication_test

import (
	"errors"
	"testing"

	hatReplication "hatrie_cache/hat/hatReplication"
)

func TestResolveConflictVersionUsesDeterministicOrdering(t *testing.T) {
	base := hatReplication.ConflictVersion{Timestamp: 100, NodeID: "node-a", Sequence: 1}
	cases := []struct {
		name      string
		candidate hatReplication.ConflictVersion
		want      hatReplication.ConflictVersion
	}{
		{
			name:      "newer timestamp wins",
			candidate: hatReplication.ConflictVersion{Timestamp: 101, NodeID: "node-z", Sequence: 1},
			want:      hatReplication.ConflictVersion{Timestamp: 101, NodeID: "node-z", Sequence: 1},
		},
		{
			name:      "older timestamp loses",
			candidate: hatReplication.ConflictVersion{Timestamp: 99, NodeID: "node-z", Sequence: 99},
			want:      base,
		},
		{
			name:      "node id breaks timestamp tie",
			candidate: hatReplication.ConflictVersion{Timestamp: 100, NodeID: "node-b", Sequence: 0},
			want:      hatReplication.ConflictVersion{Timestamp: 100, NodeID: "node-b", Sequence: 0},
		},
		{
			name:      "sequence breaks same node tie",
			candidate: hatReplication.ConflictVersion{Timestamp: 100, NodeID: "node-a", Sequence: 2},
			want:      hatReplication.ConflictVersion{Timestamp: 100, NodeID: "node-a", Sequence: 2},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got, err := hatReplication.ResolveConflictVersion(base, test.candidate)
			if err != nil {
				t.Fatalf("ResolveConflictVersion() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("ResolveConflictVersion() = %#v, want %#v", got, test.want)
			}

			reversed, err := hatReplication.ResolveConflictVersion(test.candidate, base)
			if err != nil {
				t.Fatalf("ResolveConflictVersion() reversed error = %v", err)
			}
			if reversed != test.want {
				t.Fatalf("ResolveConflictVersion() reversed = %#v, want %#v", reversed, test.want)
			}
		})
	}
}

func TestResolveConflictVersionRejectsMissingNodeID(t *testing.T) {
	valid := hatReplication.ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	invalid := hatReplication.ConflictVersion{Timestamp: 1, Sequence: 1}

	for name, pair := range map[string][2]hatReplication.ConflictVersion{
		"left":  {invalid, valid},
		"right": {valid, invalid},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := hatReplication.ResolveConflictVersion(pair[0], pair[1])
			if !errors.Is(err, hatReplication.ErrConflictVersionInvalid) {
				t.Fatalf("ResolveConflictVersion() error = %v, want ErrConflictVersionInvalid", err)
			}
		})
	}
}

func BenchmarkResolveConflictVersion(b *testing.B) {
	left := hatReplication.ConflictVersion{Timestamp: 100, NodeID: "node-a", Sequence: 1}
	right := hatReplication.ConflictVersion{Timestamp: 100, NodeID: "node-b", Sequence: 2}
	b.ReportAllocs()
	for range b.N {
		if _, err := hatReplication.ResolveConflictVersion(left, right); err != nil {
			b.Fatal(err)
		}
	}
}
