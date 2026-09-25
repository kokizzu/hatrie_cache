#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-c153c-stage.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

feature_paths=(
  BENCHMARK.md
  C153C_PARTITION_OWNERSHIP_SNAPSHOT.md
  INSPIRATION.md
  Makefile
  hat/hatPipeline/c153c_partition_ownership_snapshot.go
  hat/hatPipeline/c153c_partition_ownership_snapshot_baseline_benchmark_test.go
  hat/hatPipeline/c153c_partition_ownership_snapshot_test.go
  scripts/benchmark-c153c-ownership-baseline.sh
  scripts/benchmark-c153c-ownership-snapshot.sh
  scripts/commit-c153c-ownership-snapshot.sh
  scripts/format-c153c-ownership-snapshot.sh
  scripts/push-c153c-ownership-snapshot.sh
  scripts/race-c153c-ownership-snapshot.sh
  scripts/stage-c153c-ownership-snapshot.sh
  scripts/test-c153c-ownership-package.sh
  scripts/test-c153c-ownership-snapshot.sh
  scripts/vet-c153c-ownership-snapshot.sh
)

git show HEAD:Makefile > "$tmp_dir/Makefile"
cat >> "$tmp_dir/Makefile" <<'EOF'

test-c153c-ownership-snapshot:

	bash scripts/test-c153c-ownership-snapshot.sh

benchmark-c153c-ownership-baseline:

	bash scripts/benchmark-c153c-ownership-baseline.sh

format-c153c-ownership-snapshot:

	bash scripts/format-c153c-ownership-snapshot.sh

benchmark-c153c-ownership-snapshot:

	bash scripts/benchmark-c153c-ownership-snapshot.sh

test-c153c-ownership-package:

	bash scripts/test-c153c-ownership-package.sh

race-c153c-ownership-snapshot:

	bash scripts/race-c153c-ownership-snapshot.sh

vet-c153c-ownership-snapshot:

	bash scripts/vet-c153c-ownership-snapshot.sh

stage-c153c-ownership-snapshot:

	bash scripts/stage-c153c-ownership-snapshot.sh

commit-c153c-ownership-snapshot:

	bash scripts/commit-c153c-ownership-snapshot.sh

push-c153c-ownership-snapshot:

	bash scripts/push-c153c-ownership-snapshot.sh
EOF
makefile_blob=$(git hash-object -w "$tmp_dir/Makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git show HEAD:INSPIRATION.md > "$tmp_dir/INSPIRATION.md"
old_c153='- [ ] C153 Metadata consensus for partition ownership.'
new_c153='- [ ] C153 Metadata consensus for partition ownership. Quorum admission and validated ownership metadata are implemented; durable snapshot/restore is now available as an opt-in caller-owned primitive, while transport, authentication, and automatic control-plane integration remain open.'
c153c='- [x] C153c CRC-protected bounded binary partition-ownership snapshots with atomic restore, migration-state validation, and no change to the routing hot path. See [C153C_PARTITION_OWNERSHIP_SNAPSHOT.md](C153C_PARTITION_OWNERSHIP_SNAPSHOT.md).'
awk -v old="$old_c153" -v replacement="$new_c153" -v row="$c153c" '
  $0 == old { print replacement; replaced++; next }
  { print }
  index($0, "[PARTITION_OWNERSHIP_CONSENSUS.md](PARTITION_OWNERSHIP_CONSENSUS.md).") > 0 { print row; inserted++ }
  END {
    if (replaced != 1 || inserted != 1) exit 51
  }
' "$tmp_dir/INSPIRATION.md" > "$tmp_dir/INSPIRATION.updated.md"
inspiration_blob=$(git hash-object -w "$tmp_dir/INSPIRATION.updated.md")
git update-index --add --cacheinfo "100644,$inspiration_blob,INSPIRATION.md"

cat > "$tmp_dir/BENCHMARK.section" <<'EOF'
<a id="c153c-partition-ownership-snapshots"></a>
## C153c Partition Ownership Snapshots

Commands: `make benchmark-c153c-ownership-baseline` and
`make benchmark-c153c-ownership-snapshot`.

The fixture contains 256 partitions alternating between two owners. The JSON
reference represents the pre-change caller-owned backup path; the binary path
is deterministic, CRC-protected, bounded, and atomic on restore. Five samples
ran on the AMD Ryzen 9 5950X with `-benchmem`.

| Operation | JSON reference median | Binary median | Improvement |
| --- | ---: | ---: | ---: |
| Encode | 54,094 ns/op, 46,031 B/op, 3 allocs | 4,390 ns/op, 9,472 B/op, 1 alloc | 12.32x faster, 4.86x lower bytes, 3x fewer allocs |
| Restore | 350,285 ns/op, 41,792 B/op, 271 allocs | 9,561 ns/op, 25,392 B/op, 259 allocs | 36.64x faster, 1.65x lower bytes, 12 fewer allocs |

Raw post-change output:

```text
BenchmarkC153cOwnershipJSONSnapshotBaseline: 52433 ns/op 45983 B/op 3 allocs/op
BenchmarkC153cOwnershipJSONSnapshotBaseline: 56643 ns/op 46063 B/op 3 allocs/op
BenchmarkC153cOwnershipJSONSnapshotBaseline: 52976 ns/op 46036 B/op 3 allocs/op
BenchmarkC153cOwnershipJSONSnapshotBaseline: 54094 ns/op 45985 B/op 3 allocs/op
BenchmarkC153cOwnershipJSONSnapshotBaseline: 54992 ns/op 46040 B/op 3 allocs/op
BenchmarkC153cOwnershipBinarySnapshot: 4634 ns/op 9472 B/op 1 allocs/op
BenchmarkC153cOwnershipBinarySnapshot: 4390 ns/op 9472 B/op 1 allocs/op
BenchmarkC153cOwnershipBinarySnapshot: 4381 ns/op 9472 B/op 1 allocs/op
BenchmarkC153cOwnershipBinarySnapshot: 4383 ns/op 9472 B/op 1 allocs/op
BenchmarkC153cOwnershipBinarySnapshot: 4399 ns/op 9472 B/op 1 allocs/op
BenchmarkC153cOwnershipJSONRestoreBaseline: 355111 ns/op 41792 B/op 271 allocs/op
BenchmarkC153cOwnershipJSONRestoreBaseline: 347029 ns/op 41792 B/op 271 allocs/op
BenchmarkC153cOwnershipJSONRestoreBaseline: 347543 ns/op 41792 B/op 271 allocs/op
BenchmarkC153cOwnershipJSONRestoreBaseline: 350285 ns/op 41792 B/op 271 allocs/op
BenchmarkC153cOwnershipJSONRestoreBaseline: 354522 ns/op 41792 B/op 271 allocs/op
BenchmarkC153cOwnershipBinaryRestore: 9561 ns/op 25392 B/op 259 allocs/op
BenchmarkC153cOwnershipBinaryRestore: 9398 ns/op 25392 B/op 259 allocs/op
BenchmarkC153cOwnershipBinaryRestore: 9637 ns/op 25392 B/op 259 allocs/op
BenchmarkC153cOwnershipBinaryRestore: 9596 ns/op 25392 B/op 259 allocs/op
BenchmarkC153cOwnershipBinaryRestore: 9537 ns/op 25392 B/op 259 allocs/op
```

The existing ownership registry and ordinary replication defaults are
unchanged; callers opt into snapshot persistence or transfer explicitly. See
[C153C_PARTITION_OWNERSHIP_SNAPSHOT.md](C153C_PARTITION_OWNERSHIP_SNAPSHOT.md).

EOF
git show HEAD:BENCHMARK.md > "$tmp_dir/BENCHMARK.md"
awk -v section_file="$tmp_dir/BENCHMARK.section" '
  index($0, "<a id=\"t047f-participant-reconciliation\"></a>") == 1 {
    while ((getline line < section_file) > 0) print line
    close(section_file)
    inserted++
  }
  { print }
  END {
    if (inserted != 1) exit 52
  }
' "$tmp_dir/BENCHMARK.md" > "$tmp_dir/BENCHMARK.updated.md"
benchmark_blob=$(git hash-object -w "$tmp_dir/BENCHMARK.updated.md")
git update-index --add --cacheinfo "100644,$benchmark_blob,BENCHMARK.md"

git add -- \
  C153C_PARTITION_OWNERSHIP_SNAPSHOT.md \
  hat/hatPipeline/c153c_partition_ownership_snapshot.go \
  hat/hatPipeline/c153c_partition_ownership_snapshot_baseline_benchmark_test.go \
  hat/hatPipeline/c153c_partition_ownership_snapshot_test.go \
  scripts/benchmark-c153c-ownership-baseline.sh \
  scripts/benchmark-c153c-ownership-snapshot.sh \
  scripts/commit-c153c-ownership-snapshot.sh \
  scripts/format-c153c-ownership-snapshot.sh \
  scripts/push-c153c-ownership-snapshot.sh \
  scripts/race-c153c-ownership-snapshot.sh \
  scripts/stage-c153c-ownership-snapshot.sh \
  scripts/test-c153c-ownership-package.sh \
  scripts/test-c153c-ownership-snapshot.sh \
  scripts/vet-c153c-ownership-snapshot.sh

expected_file="$tmp_dir/expected"
staged_file="$tmp_dir/staged"
printf '%s\n' "${feature_paths[@]}" | sort > "$expected_file"
git diff --cached --name-only | sort > "$staged_file"
if ! cmp -s "$expected_file" "$staged_file"; then
  printf '%s\n' 'Unexpected staged paths:' >&2
  git diff --cached --name-only >&2
  exit 53
fi
git diff --cached --check
printf 'Staged C153c ownership snapshot paths: %s\n' "${#feature_paths[@]}"
