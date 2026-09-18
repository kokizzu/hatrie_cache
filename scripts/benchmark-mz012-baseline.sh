#!/usr/bin/env bash
set -euo pipefail

tmpdir=$(mktemp -d /tmp/hatrie-mz012-baseline.XXXXXX)
trap 'rm -rf "$tmpdir"' EXIT

cat > "$tmpdir/mz012_baseline_test.go" <<'EOF'
package mz012baseline

import (
	"runtime"
	"testing"
)

type manualLease struct {
	group      string
	member     string
	partition  int32
	generation uint64
}

func benchmarkAssignments() [64]struct {
	partition int32
	member    string
} {
	var assignments [64]struct {
		partition int32
		member    string
	}
	for index := range assignments {
		assignments[index] = struct {
			partition int32
			member    string
		}{partition: int32(index), member: "consumer-0"}
	}
	return assignments
}

func BenchmarkMZ012ManualValidate(b *testing.B) {
	owners := make(map[int32]string, 64)
	for index := 0; index < 64; index++ {
		owners[int32(index)] = "consumer-0"
	}
	lease := manualLease{group: "orders", member: "consumer-0", partition: 31, generation: 1}
	b.ReportAllocs()
	b.ResetTimer()
	var accepted uint64
	for index := 0; index < b.N; index++ {
		owner, ok := owners[lease.partition]
		if lease.group != "orders" || lease.generation != 1 || !ok || owner != lease.member {
			b.Fatal("manual lease unexpectedly rejected")
		}
		accepted++
	}
	b.StopTimer()
	runtime.KeepAlive(accepted)
}

func BenchmarkMZ012ManualRebalance(b *testing.B) {
	assignments := benchmarkAssignments()
	b.ReportAllocs()
	b.ResetTimer()
	var accepted uint64
	for index := 0; index < b.N; index++ {
		owners := make(map[int32]string, len(assignments))
		for _, assignment := range assignments {
			owners[assignment.partition] = assignment.member
		}
		accepted += uint64(len(owners))
	}
	b.StopTimer()
	runtime.KeepAlive(accepted)
}
EOF

(cd "$tmpdir" && GO111MODULE=off go test . -run '^$' -bench 'BenchmarkMZ012Manual' -benchmem -count=5)
