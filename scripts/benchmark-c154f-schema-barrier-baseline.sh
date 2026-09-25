#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"
cache_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c154f-baseline-cache.XXXXXX")
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c154f-baseline-work.XXXXXX")
trap 'rm -rf "$cache_dir" "$work_dir"' EXIT
cat > "$work_dir/main.go" <<'EOF'
package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatPipeline"
)

func fixture() *hatPipeline.SchemaMigrationBarrier {
	barrier, err := hatPipeline.NewSchemaMigrationBarrier(hatPipeline.SchemaMigrationBarrierOptions{MaxBarriers: 64, MaxDependencies: 8})
	if err != nil {
		panic(err)
	}
	for index := 0; index < 32; index++ {
		id := "migration-" + string(rune('a'+index))
		if _, err := barrier.Prepare(hatPipeline.SchemaMigrationBarrierSpec{ID: id, Version: uint64(index + 1), Dependencies: []string{"cache", "index", "reader", "sink"}}); err != nil {
			panic(err)
		}
		if index%3 == 0 {
			if err := barrier.Acknowledge(id, "reader", uint64(index+1)); err != nil {
				panic(err)
			}
		} else if index%3 == 1 {
			for _, dependency := range []string{"cache", "index", "reader", "sink"} {
				if err := barrier.Acknowledge(id, dependency, uint64(index+1)); err != nil {
					panic(err)
				}
			}
			if _, err := barrier.Commit(id, uint64(index+1)); err != nil {
				panic(err)
			}
		} else if _, err := barrier.Abort(id, uint64(index+1)); err != nil {
			panic(err)
		}
	}
	return barrier
}

func main() {
	for sample := 0; sample < 5; sample++ {
		barrier := fixture()
		result := testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for index := 0; index < b.N; index++ {
				payload, err := json.Marshal(barrier.Snapshot())
				if err != nil {
					b.Fatal(err)
				}
				b.SetBytes(int64(len(payload)))
			}
		})
		fmt.Printf("C154fSchemaBarrierJSONSnapshotBaseline: %d ns/op %d B/op %d allocs/op\n", result.NsPerOp(), result.AllocedBytesPerOp(), result.AllocsPerOp())
	}
}
EOF
GOCACHE="$cache_dir" go run "$work_dir/main.go"
