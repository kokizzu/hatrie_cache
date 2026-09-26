package hatTopology

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestC153ePartitionOwnershipConsensusVoteWireRoundTrips(t *testing.T) {
	for name, vote := range map[string]PartitionOwnershipConsensusVote{
		"authenticated": c153eBenchmarkVote(),
		"legacy unsigned": {
			NodeID: "node-a",
			Ownership: PartitionOwnership{
				ShardID:             7,
				Primary:             "node-a",
				TopologyFingerprint: "topology-v7-fingerprint",
				FencingToken:        42,
			},
			Accepted: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			first, err := MarshalPartitionOwnershipConsensusVote(vote)
			if err != nil {
				t.Fatalf("MarshalPartitionOwnershipConsensusVote() error = %v", err)
			}
			second, err := MarshalPartitionOwnershipConsensusVote(vote)
			if err != nil {
				t.Fatalf("second MarshalPartitionOwnershipConsensusVote() error = %v", err)
			}
			if !bytes.Equal(first, second) {
				t.Fatalf("wire encoding is not deterministic: first=%x second=%x", first, second)
			}
			decoded, err := UnmarshalPartitionOwnershipConsensusVote(first)
			if err != nil {
				t.Fatalf("UnmarshalPartitionOwnershipConsensusVote() error = %v", err)
			}
			if !reflect.DeepEqual(decoded, vote) {
				t.Fatalf("decoded = %#v, want %#v", decoded, vote)
			}
			decoded.Ownership.Replicas = append(decoded.Ownership.Replicas, "mutated")
			if len(vote.Ownership.Replicas) == len(decoded.Ownership.Replicas) {
				t.Fatal("decoded replica slice aliases input")
			}
			if len(decoded.Signature) > 0 {
				decoded.Signature[0]++
				if decoded.Signature[0] == vote.Signature[0] {
					t.Fatal("decoded signature aliases input")
				}
			}
		})
	}
}

func TestC153ePartitionOwnershipConsensusVoteWirePreservesAuthentication(t *testing.T) {
	authenticator, err := NewPartitionOwnershipConsensusAuthenticator("cluster-key-v1", []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewPartitionOwnershipConsensusAuthenticator() error = %v", err)
	}
	vote, err := authenticator.Sign(PartitionOwnershipConsensusVote{
		NodeID:    "node-a",
		Ownership: c153eBenchmarkVote().Ownership,
		Accepted:  true,
	})
	if err != nil {
		t.Fatalf("Sign() error = %v", err)
	}
	payload, err := MarshalPartitionOwnershipConsensusVote(vote)
	if err != nil {
		t.Fatalf("MarshalPartitionOwnershipConsensusVote() error = %v", err)
	}
	decoded, err := UnmarshalPartitionOwnershipConsensusVote(payload)
	if err != nil {
		t.Fatalf("UnmarshalPartitionOwnershipConsensusVote() error = %v", err)
	}
	if err := authenticator.Verify(decoded); err != nil {
		t.Fatalf("Verify(decoded) error = %v", err)
	}
}

func TestC153ePartitionOwnershipConsensusVoteWireIsSmallerThanJSON(t *testing.T) {
	vote := c153eBenchmarkVote()
	jsonPayload, err := json.Marshal(vote)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	wirePayload, err := MarshalPartitionOwnershipConsensusVote(vote)
	if err != nil {
		t.Fatalf("MarshalPartitionOwnershipConsensusVote() error = %v", err)
	}
	t.Logf("json bytes=%d binary bytes=%d", len(jsonPayload), len(wirePayload))
	if len(wirePayload) >= len(jsonPayload) {
		t.Fatalf("binary payload is not smaller: binary=%d json=%d", len(wirePayload), len(jsonPayload))
	}
}

func TestC153ePartitionOwnershipConsensusVoteWireRejectsMalformedInput(t *testing.T) {
	valid, err := MarshalPartitionOwnershipConsensusVote(c153eBenchmarkVote())
	if err != nil {
		t.Fatalf("MarshalPartitionOwnershipConsensusVote() error = %v", err)
	}
	malformed := map[string][]byte{
		"empty":     nil,
		"bad magic": append([]byte("BAD1"), valid[4:]...),
		"truncated": valid[:len(valid)-1],
		"trailing":  append(append([]byte(nil), valid...), 0),
		"reserved flag": func() []byte {
			payload := append([]byte(nil), valid...)
			payload[4] |= 0x80
			return payload
		}(),
		"oversized": make([]byte, MaxPartitionOwnershipConsensusVoteWireBytes+1),
	}
	for name, payload := range malformed {
		t.Run(name, func(t *testing.T) {
			if _, err := UnmarshalPartitionOwnershipConsensusVote(payload); !errors.Is(err, ErrPartitionOwnershipConsensusWireInvalid) && !errors.Is(err, ErrPartitionOwnershipConsensusWireTooLarge) {
				t.Fatalf("error = %v, want wire validation error", err)
			}
		})
	}
}

func TestC153ePartitionOwnershipConsensusVoteWireRejectsInvalidValues(t *testing.T) {
	base := c153eBenchmarkVote()
	for name, mutate := range map[string]func(*PartitionOwnershipConsensusVote){
		"blank node":            func(vote *PartitionOwnershipConsensusVote) { vote.NodeID = " node-a" },
		"blank key id":          func(vote *PartitionOwnershipConsensusVote) { vote.KeyID = " key-v1" },
		"key without signature": func(vote *PartitionOwnershipConsensusVote) { vote.Signature = nil },
		"short signature":       func(vote *PartitionOwnershipConsensusVote) { vote.Signature = []byte{1} },
		"missing primary":       func(vote *PartitionOwnershipConsensusVote) { vote.Ownership.Primary = "" },
	} {
		t.Run(name, func(t *testing.T) {
			vote := base
			vote.Signature = append([]byte(nil), base.Signature...)
			mutate(&vote)
			if _, err := MarshalPartitionOwnershipConsensusVote(vote); !errors.Is(err, ErrPartitionOwnershipConsensusWireInvalid) {
				t.Fatalf("error = %v, want ErrPartitionOwnershipConsensusWireInvalid", err)
			}
		})
	}
}

func BenchmarkC153ePartitionOwnershipVoteWireJSON(b *testing.B) {
	vote := c153eBenchmarkVote()
	payload, err := json.Marshal(vote)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("marshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(payload)), "payload-B")
		b.ResetTimer()
		for range b.N {
			if _, err := json.Marshal(vote); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(payload)), "payload-B")
		b.ResetTimer()
		for range b.N {
			var decoded PartitionOwnershipConsensusVote
			if err := json.Unmarshal(payload, &decoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkC153ePartitionOwnershipVoteWire(b *testing.B) {
	vote := c153eBenchmarkVote()
	payload, err := MarshalPartitionOwnershipConsensusVote(vote)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("marshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(payload)), "payload-B")
		b.ResetTimer()
		for range b.N {
			if _, err := MarshalPartitionOwnershipConsensusVote(vote); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(payload)), "payload-B")
		b.ResetTimer()
		for range b.N {
			if _, err := UnmarshalPartitionOwnershipConsensusVote(payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func c153eBenchmarkVote() PartitionOwnershipConsensusVote {
	return PartitionOwnershipConsensusVote{
		NodeID: "node-a",
		Ownership: PartitionOwnership{
			ShardID:             7,
			Primary:             "node-a",
			Replicas:            []string{"node-b", "node-c"},
			TopologyFingerprint: "topology-v7-fingerprint",
			FencingToken:        42,
		},
		Accepted: true,
		KeyID:    "cluster-key-v1",
		Signature: []byte{
			0, 1, 2, 3, 4, 5, 6, 7,
			8, 9, 10, 11, 12, 13, 14, 15,
			16, 17, 18, 19, 20, 21, 22, 23,
			24, 25, 26, 27, 28, 29, 30, 31,
		},
	}
}
