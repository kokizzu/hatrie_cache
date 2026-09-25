#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to commit: the index already contains staged changes' >&2
  exit 1
fi

git add -- \
	TT006_HOT_STANDBY_WAL.md \
	hat/hatReplication/tt006_hot_standby.go \
	hat/hatReplication/tt006_hot_standby_test.go \
  hat/hatReplication/tt006_hot_standby_benchmark_test.go \
  scripts/benchmark-tt006-hot-standby.sh \
  scripts/compile-tt006-all.sh \
  scripts/format-tt006-hot-standby.sh \
  scripts/race-tt006-hot-standby.sh \
  scripts/test-tt006-hot-standby.sh \
	scripts/test-tt006-package.sh \
	scripts/vet-tt006-hot-standby.sh \
	scripts/commit-tt006-hot-standby.sh \
	scripts/push-tt006-hot-standby.sh

makefile_base=$(mktemp)
makefile_block=$(mktemp)
makefile_patch=$(mktemp)
benchmark_base=$(mktemp)
benchmark_block=$(mktemp)
benchmark_patch=$(mktemp)
ideas_line=$(mktemp)
ideas_patch=$(mktemp)
ideas_base=$(mktemp)
ideas_new=$(mktemp)
trap 'rm -f "$makefile_base" "$makefile_block" "$makefile_patch" "$benchmark_base" "$benchmark_block" "$benchmark_patch" "$ideas_line" "$ideas_patch" "$ideas_base" "$ideas_new"' EXIT

cat > "$makefile_block" <<'TARGETS'
.PHONY: test-tt006-hot-standby
test-tt006-hot-standby:
	bash ./scripts/test-tt006-hot-standby.sh

.PHONY: format-tt006-hot-standby
format-tt006-hot-standby:
	bash ./scripts/format-tt006-hot-standby.sh

.PHONY: benchmark-tt006-hot-standby
benchmark-tt006-hot-standby:
	bash ./scripts/benchmark-tt006-hot-standby.sh

.PHONY: test-tt006-package
test-tt006-package:
	bash ./scripts/test-tt006-package.sh

.PHONY: race-tt006-hot-standby
race-tt006-hot-standby:
	bash ./scripts/race-tt006-hot-standby.sh

.PHONY: vet-tt006-hot-standby
vet-tt006-hot-standby:
	bash ./scripts/vet-tt006-hot-standby.sh

.PHONY: compile-tt006-all
compile-tt006-all:
	bash ./scripts/compile-tt006-all.sh

.PHONY: commit-tt006-hot-standby
commit-tt006-hot-standby:
	bash ./scripts/commit-tt006-hot-standby.sh

.PHONY: push-tt006-hot-standby
push-tt006-hot-standby:
	bash ./scripts/push-tt006-hot-standby.sh
TARGETS
git show HEAD:Makefile > "$makefile_base"
makefile_lines=$(wc -l < "$makefile_base")
makefile_block_lines=$(wc -l < "$makefile_block")
{
	printf '%s\n' 'diff --git a/Makefile b/Makefile' '--- a/Makefile' '+++ b/Makefile'
	printf '@@ -%s,0 +%s,%s @@\n' "$makefile_lines" "$((makefile_lines + 1))" "$makefile_block_lines"
	while IFS= read -r line || [[ -n "$line" ]]; do
		printf '+%s\n' "$line"
	done < "$makefile_block"
} > "$makefile_patch"
printf '%s\n' 'staging TT-006 Makefile targets' >&2
git apply --cached --verbose "$makefile_patch"

cat > "$benchmark_block" <<'BENCHMARK'
## TT-006 Hot-Standby WAL Catch-Up

`make benchmark-tt006-hot-standby` (five samples, Linux/amd64, AMD Ryzen 9
5950X, 64 one-record batches):

| Path | Time | Memory | Allocations | Interpretation |
| --- | ---: | ---: | ---: | --- |
| `HotStandby` replay/status loop | 11.820-12.243 us/op | 14,720 B/op | 69 allocs/op | Opt-in safety and failover path |
| Raw contiguous-loop control | 20.25-22.20 ns/op | 0 B/op | 0 allocs/op | Lower bound; no callbacks, locks, context, or status |

The control is not an equivalent replica implementation. The feature is
opt-in, so ordinary command execution has no new hot-standby work unless a
caller starts a runner. Full interpretation and adapter guidance are in
[TT006_HOT_STANDBY_WAL.md](TT006_HOT_STANDBY_WAL.md).
BENCHMARK
git show HEAD:BENCHMARK.md > "$benchmark_base"
benchmark_lines=$(wc -l < "$benchmark_base")
benchmark_block_lines=$(wc -l < "$benchmark_block")
{
	printf '%s\n' 'diff --git a/BENCHMARK.md b/BENCHMARK.md' '--- a/BENCHMARK.md' '+++ b/BENCHMARK.md'
	printf '@@ -%s,0 +%s,%s @@\n' "$benchmark_lines" "$((benchmark_lines + 1))" "$benchmark_block_lines"
	while IFS= read -r line || [[ -n "$line" ]]; do
		printf '+%s\n' "$line"
	done < "$benchmark_block"
} > "$benchmark_patch"
printf '%s\n' 'staging TT-006 benchmark section' >&2
git apply --cached --verbose "$benchmark_patch"

old_idea='| TT-006 | Hot-standby WAL catch-up | No read-only standby that continuously replays and can be promoted without restore. | High |'
new_idea='| TT-006 | Hot-standby WAL catch-up | Partially adopted as importable `hatReplication.HotStandby`: bounded continuous journal-batch replay, strict contiguous-sequence validation, lag/status accounting, cancellation, and generation/fencing-guarded promotion; storage applier, transport, source fencing, and topology publication remain caller-owned. See [TT006_HOT_STANDBY_WAL.md](TT006_HOT_STANDBY_WAL.md). | High |'
git show HEAD:ENGINE_IDEAS.md > "$ideas_base"
rg -n -F "$old_idea" "$ideas_base" > "$ideas_line"
while IFS= read -r line || [[ -n "$line" ]]; do
	if [[ "$line" == "$old_idea" ]]; then
		printf '%s\n' "$new_idea"
	else
		printf '%s\n' "$line"
	fi
done < "$ideas_base" > "$ideas_new"
ideas_blob=$(git hash-object -w "$ideas_new")
printf '%s\n' 'staging TT-006 idea registry row' >&2
git update-index --add --cacheinfo 100644 "$ideas_blob" ENGINE_IDEAS.md

git diff --cached --check
git commit -m 'Add TT-006 hot standby WAL catch-up'
