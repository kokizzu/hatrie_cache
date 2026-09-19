#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

index_file="$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-chu35-index.XXXXXX")"
stage_root="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-chu35-stage.XXXXXX")"
trap 'rm -f "$index_file"; rm -rf "$stage_root"' EXIT

GIT_INDEX_FILE="$index_file" git read-tree HEAD

paths=(
  "BENCHMARK.md"
  "CHU35_OPTIMIZE_CONTROL.md"
  "Makefile"
  "PRODUCT_IDEA_GAPS.md"
  "README.md"
  "hat/hatStorage/compaction_control.go"
  "hat/hatStorage/chu35_optimize_control_benchmark_test.go"
  "hat/hatStorage/chu35_optimize_control_test.go"
  "scripts/benchmark-chu35-baseline.sh"
  "scripts/benchmark-chu35.sh"
  "scripts/commit-chu35.sh"
  "scripts/format-chu35.sh"
  "scripts/push-chu35.sh"
  "scripts/race-chu35.sh"
  "scripts/test-chu35-package.sh"
  "scripts/test-chu35.sh"
  "scripts/verify-chu35.sh"
  "scripts/vet-chu35.sh"
)

for path in "${paths[@]}"; do
  source_path="$repo_root/$path"
  if [[ ! -f "$source_path" ]]; then
    printf 'missing CH-U35 path: %s\n' "$path" >&2
    exit 1
  fi
  target_path="$stage_root/$path"
  mkdir -p "$(dirname "$target_path")"
  if [[ "$path" == "Makefile" ]]; then
    git show HEAD:Makefile > "$target_path"
    cat >> "$target_path" <<'MAKEFILE_APPEND'

.PHONY: test-chu35 test-chu35-package benchmark-chu35-baseline benchmark-chu35 format-chu35 race-chu35 vet-chu35 verify-chu35
test-chu35:
	bash ./scripts/test-chu35.sh

test-chu35-package:
	bash ./scripts/test-chu35-package.sh

benchmark-chu35-baseline:
	bash ./scripts/benchmark-chu35-baseline.sh

benchmark-chu35:
	bash ./scripts/benchmark-chu35.sh

format-chu35:
	bash ./scripts/format-chu35.sh

race-chu35:
	bash ./scripts/race-chu35.sh

vet-chu35:
	bash ./scripts/vet-chu35.sh

verify-chu35:
	bash ./scripts/verify-chu35.sh

.PHONY: commit-chu35 push-chu35
commit-chu35:
	bash ./scripts/commit-chu35.sh

push-chu35:
	bash ./scripts/push-chu35.sh
MAKEFILE_APPEND
  elif [[ "$path" == "README.md" ]]; then
    git show HEAD:README.md > "$target_path"
    cat >> "$target_path" <<'README_APPEND'

### Bounded optimize control

The importable opt-in `hatStorage.CompactionController` adds bounded target
coalescing, job status/history, retry state, context cancellation, priority,
and estimated-I/O pacing over the existing compaction scheduler. It starts no
worker and changes no default path; callers own authentication, target
validation, backend merge callbacks, and any HTTP/SQL command wiring. See
[CHU35_OPTIMIZE_CONTROL.md](CHU35_OPTIMIZE_CONTROL.md).
README_APPEND
  elif [[ "$path" == "BENCHMARK.md" ]]; then
    git show HEAD:BENCHMARK.md > "$target_path"
    cat >> "$target_path" <<'BENCHMARK_APPEND'

## CH-U35 Bounded Optimize Control

Five `-benchmem` samples used the same 64-task, no-op callback workload as the
existing scheduler benchmark. This is a control-plane comparison, not a real
compaction throughput claim.

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Existing `CompactionScheduler` | 34,521; 29,589; 30,360; 37,445; 36,661 | 34,521 | 17,513 | 35 | 1.00x |
| Opt-in `CompactionController` | 162,499; 172,362; 153,163; 148,213; 150,976 | 153,163 | 39,453 | 193 | 4.44x |

The controller is retained as an opt-in status/control capability, not as a
replacement for the lower-overhead scheduler. Full semantics and safety
boundaries are in [CHU35_OPTIMIZE_CONTROL.md](CHU35_OPTIMIZE_CONTROL.md).
BENCHMARK_APPEND
  elif [[ "$path" == "PRODUCT_IDEA_GAPS.md" ]]; then
    git show HEAD:PRODUCT_IDEA_GAPS.md > "$target_path"
    sed -i 's#| CH-U35 | `OPTIMIZE`/merge-control command | Operators cannot request a bounded targeted part merge and observe its completion. | No unbounded memory use, cancellation, and write throttling. |#| CH-U35 | `OPTIMIZE`/merge-control command | Partially adopted as importable opt-in `hatStorage.CompactionController`: bounded target coalescing, job IDs/status, retry state, context cancellation, priority, and I/O estimates are available; HTTP/SQL command wiring and backend-specific target catalogs remain caller-owned. See [CHU35_OPTIMIZE_CONTROL.md](CHU35_OPTIMIZE_CONTROL.md). | No unbounded memory use, cancellation, write throttling, and safe operator integration. |#' "$target_path"
  else
    cp -- "$source_path" "$target_path"
  fi
  blob="$(git hash-object -w "$target_path")"
  GIT_INDEX_FILE="$index_file" git update-index --add --cacheinfo "100644,$blob,$path"
done

printf 'CH-U35 synthetic staged paths (parallel staged changes excluded):\n'
GIT_INDEX_FILE="$index_file" git diff --cached --name-status
GIT_INDEX_FILE="$index_file" git diff --cached --check
GIT_INDEX_FILE="$index_file" git commit -m "feat(storage): add bounded optimize control"
