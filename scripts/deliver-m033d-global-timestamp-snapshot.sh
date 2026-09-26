#!/usr/bin/env bash
set -euo pipefail

feature_files=(
  M033D_GLOBAL_TIMESTAMP_SNAPSHOT.md
  hat/hatReplication/global_timestamp_oracle_snapshot_store.go
  hat/hatReplication/m033d_global_timestamp_snapshot_test.go
  scripts/benchmark-m033d-global-timestamp-snapshot.sh
  scripts/format-m033d-global-timestamp-snapshot.sh
  scripts/race-m033d-global-timestamp-snapshot.sh
  scripts/test-m033d-global-timestamp-package.sh
  scripts/test-m033d-global-timestamp-snapshot.sh
  scripts/vet-m033d-global-timestamp-snapshot.sh
  scripts/deliver-m033d-global-timestamp-snapshot.sh
)

makefile_targets='benchmark-m033d-global-timestamp-snapshot:
	bash ./scripts/benchmark-m033d-global-timestamp-snapshot.sh

test-m033d-global-timestamp-snapshot:
	bash ./scripts/test-m033d-global-timestamp-snapshot.sh

format-m033d-global-timestamp-snapshot:
	bash ./scripts/format-m033d-global-timestamp-snapshot.sh

race-m033d-global-timestamp-snapshot:
	bash ./scripts/race-m033d-global-timestamp-snapshot.sh

vet-m033d-global-timestamp-snapshot:
	bash ./scripts/vet-m033d-global-timestamp-snapshot.sh

test-m033d-global-timestamp-package:
	bash ./scripts/test-m033d-global-timestamp-package.sh'

benchmark_section='# M033d Durable Global Timestamp Snapshot Codec

Command: `make benchmark-m033d-global-timestamp-snapshot`.

Host: Linux/amd64, AMD Ryzen 9 5950X. Fixture: two global timestamp oracle
nodes. Each case was sampled five times; the table preserves the raw samples.

| Operation | Median ns/op | B/op | Allocs/op | Wire bytes | Raw ns/op samples |
| --- | ---: | ---: | ---: | ---: | --- |
| JSON encode | 930.0 | 496 | 2 | 426 | 963.8, 925.7, 932.8, 930.0, 916.3 |
| Binary encode | 216.2 | 112 | 1 | 108 | 213.0, 212.8, 217.5, 216.6, 216.2 |
| JSON decode | 6,051 | 792 | 15 | 426 | 6,038, 6,035, 6,051, 6,137, 6,227 |
| Binary decode | 291.1 | 272 | 5 | 108 | 284.4, 288.0, 291.1, 292.6, 293.5 |

Binary is approximately 4.3x faster for encoding, 20.8x faster for decoding,
and 3.9x smaller on the wire for this bounded snapshot. Existing JSON encode
performance remains the compatibility baseline; the binary codec is opt-in.'

inspiration_line='- [x] M033d Bounded deterministic binary global timestamp snapshots with CRC32C validation and atomic `0600` file persistence; consensus publication timing and cryptographic authentication remain caller-owned. See [M033D_GLOBAL_TIMESTAMP_SNAPSHOT.md](M033D_GLOBAL_TIMESTAMP_SNAPSHOT.md).'

usage() {
  printf '%s\n' "usage: $0 status|stage|deliver"
}

stage_shared_files() (
	local temp_dir makefile_temp benchmark_temp inspiration_temp hash
	temp_dir=$(mktemp -d)
	trap 'rm -rf "$temp_dir"' EXIT

  if ! git diff --cached --quiet -- Makefile BENCHMARK.md INSPIRATION.md; then
    printf '%s\n' 'refusing to replace already-staged shared-file changes' >&2
    return 1
  fi

  makefile_temp="$temp_dir/Makefile"
  {
    git show HEAD:Makefile
    printf '\n%s\n' "$makefile_targets"
  } > "$makefile_temp"
  hash=$(git hash-object -w "$makefile_temp")
  git update-index --add --cacheinfo "100644,$hash,Makefile"

  benchmark_temp="$temp_dir/BENCHMARK.md"
  {
    printf '%s\n\n' "$benchmark_section"
    git show HEAD:BENCHMARK.md
  } > "$benchmark_temp"
  hash=$(git hash-object -w "$benchmark_temp")
  git update-index --add --cacheinfo "100644,$hash,BENCHMARK.md"

  inspiration_temp="$temp_dir/INSPIRATION.md"
  git show HEAD:INSPIRATION.md | awk -v addition="$inspiration_line" '
    /^- \[x\] M033c / { print; print addition; next }
    { print }
  ' > "$inspiration_temp"
	hash=$(git hash-object -w "$inspiration_temp")
	git update-index --add --cacheinfo "100644,$hash,INSPIRATION.md"
)

stage_feature() {
	git add -- "${feature_files[@]}"
	if git diff --cached --quiet -- Makefile BENCHMARK.md INSPIRATION.md; then
		stage_shared_files
	fi
	git diff --cached --check
	git diff --cached --name-status
}

case "${1:-}" in
  status)
    git status --short
    ;;
  stage)
    stage_feature
    ;;
  deliver)
    stage_feature
    git commit -m 'feat: persist global timestamp oracle snapshots [skip ci]'
    git push origin HEAD
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
