#!/usr/bin/env bash
set -euo pipefail

branch="codex/next-inspiration-round98-chg14-20261003"
[[ "$(git branch --show-current)" == "$branch" ]] || {
  printf '%s\n' "unexpected branch: $(git branch --show-current)" >&2
  exit 1
}

expected_paths=(
  BENCHMARK.md
  CHG14_PREPARED_PLAN_METRICS.md
  Makefile
  README.md
  hat/hatSql/prepared_cache_benchmark_test.go
  hat/hatSql/prepared_cache_key.go
  hat/hatSql/prepared_cache_test.go
  hat/hatSql/query.go
  scripts/benchmark-chg14-prepared-cache.sh
  scripts/commit-chg14-prepared-cache.sh
  scripts/push-chg14-prepared-cache.sh
  scripts/race-chg14-prepared-cache.sh
  scripts/test-chg14-prepared-cache.sh
  scripts/vet-chg14-prepared-cache.sh
)

status_file="/tmp/hatrie-round98-chg14-status"
trap 'rm -f "$status_file"' EXIT
git status --short --untracked-files=all > "$status_file"
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  path="${line:3}"
  allowed=false
  for expected in "${expected_paths[@]}"; do
    if [[ "$path" == "$expected" ]]; then
      allowed=true
      break
    fi
  done
  [[ "$allowed" == true ]] || {
    printf '%s\n' "unexpected path in feature worktree: $path" >&2
    exit 1
  }
done < "$status_file"

git add -- \
  BENCHMARK.md \
  CHG14_PREPARED_PLAN_METRICS.md \
  Makefile \
  README.md \
  hat/hatSql/prepared_cache_benchmark_test.go \
  hat/hatSql/prepared_cache_key.go \
  hat/hatSql/prepared_cache_test.go \
  hat/hatSql/query.go \
  scripts/benchmark-chg14-prepared-cache.sh \
  scripts/commit-chg14-prepared-cache.sh \
  scripts/push-chg14-prepared-cache.sh \
  scripts/race-chg14-prepared-cache.sh \
  scripts/test-chg14-prepared-cache.sh \
  scripts/vet-chg14-prepared-cache.sh
git diff --cached --check
git commit --amend --no-edit
