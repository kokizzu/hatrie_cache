#!/usr/bin/env bash
set -euo pipefail

tmpdir=$(mktemp -d /tmp/hatrie-tr003-baseline.XXXXXX)
trap 'rm -rf "$tmpdir"' EXIT

cat > "$tmpdir/tr003_baseline_test.go" <<'EOF'
package tr003baseline

import "testing"

type manualToken struct {
	node       string
	required   uint64
	generation uint64
}

func BenchmarkTR003ManualCapture(b *testing.B) {
	replicas := map[string]uint64{"standby-a": 100}
	source := uint64(100)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		applied := replicas["standby-a"]
		if applied < source {
			b.Fatal("manual replica is not caught up")
		}
		_ = manualToken{node: "standby-a", required: source, generation: 0}
	}
}

func BenchmarkTR003ManualPromote(b *testing.B) {
	replicas := map[string]uint64{"standby-a": 100}
	source := uint64(100)
	generation := uint64(0)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		token := manualToken{node: "standby-a", required: source, generation: generation}
		if token.required != source || replicas[token.node] < token.required || token.generation != generation {
			b.Fatal("manual promotion barrier rejected caught-up replica")
		}
		generation++
	}
}
EOF

(cd "$tmpdir" && GO111MODULE=off go test . -run '^$' -bench 'BenchmarkTR003Manual' -benchmem -count=5)
