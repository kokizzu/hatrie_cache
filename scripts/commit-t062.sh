#!/usr/bin/env bash
set -euo pipefail

mode="${1:-verify}"
commit_message="ops: add replication wire metrics"
feature_files=(
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  INSPIRATION.md
  README.md
  REPLICATION_METRICS.md
  hat/hatCache/monitoring.go
  hat/hatCache/replication.go
  hat/hatCache/replication_bandwidth.go
  hat/hatCache/replication_bandwidth_test.go
  hat/hatReplication/metrics.go
  hat/hatReplication/metrics_benchmark_test.go
  hat/hatReplication/metrics_test.go
  scripts/benchmark-t062.sh
  scripts/commit-t062.sh
  scripts/format-t062.sh
  scripts/review-t062.sh
  scripts/test-race-t062.sh
  scripts/test-t062.sh
  scripts/vet-t062.sh
)

makefile_targets=(
  '.PHONY: test-t062'
  '.PHONY: format-t062'
  '.PHONY: test-race-t062'
  '.PHONY: vet-t062'
  '.PHONY: benchmark-t062'
  '.PHONY: stage-t062'
  '.PHONY: commit-t062'
  '.PHONY: push-t062'
  '.PHONY: review-t062'
)

verify_worktree() {
  for path in "${feature_files[@]}"; do
    if [[ ! -e "$path" ]]; then
      printf 'missing T062 file: %s\n' "$path" >&2
      return 1
    fi
  done
  for target in "${makefile_targets[@]}"; do
    if ! grep -Fq "$target" Makefile; then
      printf 'missing T062 Makefile target: %s\n' "$target" >&2
      return 1
    fi
  done
}

verify_staged() {
  local staged path
  staged="$(git diff --cached --name-only)"
  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    case " ${feature_files[*]} Makefile " in
      *" $path "*) ;;
      *)
        printf 'unexpected staged path: %s\n' "$path" >&2
        return 1
        ;;
    esac
  done <<< "$staged"
  for path in Makefile "${feature_files[@]}"; do
    if ! grep -Fxq "$path" <<< "$staged"; then
      printf 'missing staged path: %s\n' "$path" >&2
      return 1
    fi
  done
  git diff --cached --check
}

stage_feature() {
  verify_worktree
  if ! git diff --cached --quiet; then
    verify_staged
    git add -- "${feature_files[@]}"
    verify_staged
    return 0
  fi

  git add -- "${feature_files[@]}"

  local base feature patch diff_status
  base="$(mktemp)"
  feature="$(mktemp)"
  patch="$(mktemp)"
  trap 'rm -f "$base" "$feature" "$patch"' RETURN
  git show HEAD:Makefile > "$base"
  cp "$base" "$feature"
  printf '\n' >> "$feature"
  printf '%s\n' \
    '.PHONY: test-t062' \
    'test-t062:' \
    $'\tbash ./scripts/test-t062.sh' \
    '.PHONY: format-t062' \
    'format-t062:' \
    $'\tbash ./scripts/format-t062.sh' \
    '.PHONY: test-race-t062' \
    'test-race-t062:' \
    $'\tbash ./scripts/test-race-t062.sh' \
    '.PHONY: vet-t062' \
    'vet-t062:' \
    $'\tbash ./scripts/vet-t062.sh' \
    '.PHONY: benchmark-t062' \
    'benchmark-t062:' \
    $'\tbash ./scripts/benchmark-t062.sh' \
    '.PHONY: stage-t062' \
    'stage-t062:' \
    $'\tbash ./scripts/commit-t062.sh stage' \
    '.PHONY: commit-t062' \
    'commit-t062:' \
    $'\tbash ./scripts/commit-t062.sh commit' \
    '.PHONY: push-t062' \
    'push-t062:' \
    $'\tbash ./scripts/commit-t062.sh push' \
    '.PHONY: review-t062' \
    'review-t062:' \
    $'\tbash ./scripts/review-t062.sh' >> "$feature"
  if diff -u --label a/Makefile --label b/Makefile "$base" "$feature" > "$patch"; then
    diff_status=0
  else
    diff_status=$?
  fi
  if [[ "$diff_status" -ne 1 ]]; then
    printf 'failed to build isolated Makefile patch (status %s)\n' "$diff_status" >&2
    return 1
  fi
  git apply --cached "$patch"
  verify_staged
}

case "$mode" in
  verify)
    verify_worktree
    ;;
  stage)
    stage_feature
    ;;
  commit)
    stage_feature
    git commit -m "$commit_message"
    ;;
  push)
    git push origin HEAD
    ;;
  *)
    printf 'usage: %s [verify|stage|commit|push]\n' "$0" >&2
    exit 2
    ;;
esac
