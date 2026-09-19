#!/usr/bin/env bash
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
current_head=$(git rev-parse HEAD)
base_ref=${T044_BASE_REF:-HEAD}
if [[ "$base_ref" == "origin/master" ]]; then
	git fetch origin master
fi
base=$(git rev-parse "$base_ref")
stage=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-t044-stage.XXXXXX")
archive=$(mktemp "${TMPDIR:-/tmp}/hatrie-t044-archive.XXXXXX.tar")
index=$(mktemp "${TMPDIR:-/tmp}/hatrie-t044-index.XXXXXX")
trap 'rm -rf "$stage" "$archive" "$index"' EXIT

git archive --format=tar --output="$archive" "$base"
tar -xf "$archive" -C "$stage"

copy_file() {
	local path=$1
	mkdir -p "$stage/$(dirname "$path")"
	cp "$repo/$path" "$stage/$path"
}

copy_file T044_SLAB_FRAGMENTATION.md
copy_file hat/hatMetrics/t044_heap_fragmentation.go
copy_file hat/hatMetrics/t044_heap_fragmentation_test.go
copy_file hat/hatMetrics/t044_heap_fragmentation_public_test.go
copy_file hat/hatMetrics/t044_heap_fragmentation_benchmark_test.go
copy_file scripts/format-t044.sh
copy_file scripts/test-t044.sh
copy_file scripts/benchmark-t044.sh
copy_file scripts/test-t044-package.sh
copy_file scripts/race-t044.sh
copy_file scripts/vet-t044.sh
copy_file scripts/verify-t044.sh
copy_file scripts/commit-t044.sh
copy_file scripts/push-t044.sh

cat >> "$stage/Makefile" <<'EOF'

# BEGIN T044 heap fragmentation diagnostics
.PHONY: format-t044 test-t044 benchmark-t044 test-t044-package race-t044 vet-t044 verify-t044 commit-t044 push-t044
format-t044:
	bash ./scripts/format-t044.sh
test-t044:
	bash ./scripts/test-t044.sh
benchmark-t044:
	bash ./scripts/benchmark-t044.sh
test-t044-package:
	bash ./scripts/test-t044-package.sh
race-t044:
	bash ./scripts/race-t044.sh
vet-t044:
	bash ./scripts/vet-t044.sh
verify-t044:
	bash ./scripts/verify-t044.sh
commit-t044:
	bash ./scripts/commit-t044.sh
push-t044:
	bash ./scripts/push-t044.sh
# END T044 heap fragmentation diagnostics
EOF

cat >> "$stage/README.md" <<'EOF'

## Heap fragmentation diagnostics

The opt-in T-U44 report exposes portable Go heap placement, reusable idle bytes, allocator metadata, and stack footprint without adding background work. See [T044_SLAB_FRAGMENTATION.md](T044_SLAB_FRAGMENTATION.md) for field semantics, cost, and verification.
EOF

cat >> "$stage/BENCHMARK.md" <<'EOF'

## T-U44 heap fragmentation diagnostics

ReadHeapFragmentationReport retained the direct runtime sampling allocation profile at 0 B/op and 0 allocs/op. The five-run medians were 19,614 ns/op for direct ReadMemStats and 19,116 ns/op for the report; the overlapping samples are treated as noise rather than a throughput claim. Raw samples and the diagnostic tradeoff are in [T044_SLAB_FRAGMENTATION.md](T044_SLAB_FRAGMENTATION.md).
EOF

perl -0pi -e '
my $old = "| T-U44 | Slab fragmentation diagnostics | Runtime heap metrics do not expose allocator classes, fragmentation, and reusable free space in a portable read-only report. | Platform portability and sampling cost. |";
my $new = "| T-U44 | Slab fragmentation diagnostics | Implemented: opt-in portable heap placement, allocator metadata, reusable idle bytes, and stack footprint from runtime counters. | Platform portability and sampling cost; no default-path work or measured allocations. |";
my $count = s/Q$oldE/$new/;
die "expected exactly one T-U44 matrix row, got $count" if $count != 1;
' "$stage/PRODUCT_IDEA_GAPS.md"

rm -f "$index"
export GIT_INDEX_FILE="$index"
git read-tree "$base"
git --work-tree="$stage" add -- BENCHMARK.md Makefile PRODUCT_IDEA_GAPS.md README.md T044_SLAB_FRAGMENTATION.md hat/hatMetrics/t044_heap_fragmentation.go hat/hatMetrics/t044_heap_fragmentation_benchmark_test.go hat/hatMetrics/t044_heap_fragmentation_public_test.go hat/hatMetrics/t044_heap_fragmentation_test.go scripts/benchmark-t044.sh scripts/commit-t044.sh scripts/format-t044.sh scripts/push-t044.sh scripts/race-t044.sh scripts/test-t044-package.sh scripts/test-t044.sh scripts/verify-t044.sh scripts/vet-t044.sh
tree=$(git write-tree)
commit=$(git commit-tree "$tree" -p "$base" -m "feat: add heap fragmentation diagnostics")
git update-ref HEAD "$commit" "$current_head"

printf 'T-U44 isolated commit: %s\\n' "$commit"
git diff-tree --stat --oneline "$commit"
