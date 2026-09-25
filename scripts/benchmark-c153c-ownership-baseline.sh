#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"
cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c153c-baseline-cache.XXXXXX")
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c153c-baseline-work.XXXXXX")
trap 'rm -rf "$cache_dir" "$work_dir"' EXIT
cat > "$work_dir/main.go" <<'EOF'
package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatPipeline"
)

func fixture() *hatPipeline.QueuePartitionOwnership {
	owners := make([]string, 256)
	for partition := range owners {
		if partition%2 == 0 {
			owners[partition] = "node-a"
		} else {
			owners[partition] = "node-b"
		}
	}
	ownership, err := hatPipeline.NewQueuePartitionOwnership(hatPipeline.QueuePartitionOwnershipOptions{
		PartitionCount: len(owners),
		InitialOwners:  owners,
	})
	if err != nil {
		panic(err)
	}
	return ownership
}

func main() {
	for sample := 0; sample < 5; sample++ {
		ownership := fixture()
		result := testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for index := 0; index < b.N; index++ {
				payload, err := json.Marshal(ownership.Assignments())
				if err != nil {
					b.Fatal(err)
				}
				b.SetBytes(int64(len(payload)))
			}
		})
		fmt.Printf("C153cOwnershipJSONSnapshotBaseline: %d ns/op %d B/op %d allocs/op\n", result.NsPerOp(), result.AllocedBytesPerOp(), result.AllocsPerOp())
	}
}
EOF
GOCACHE="$cache_dir" go run "$work_dir/main.go"
