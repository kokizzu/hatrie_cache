#!/usr/bin/env bash
set -euo pipefail

declare -A expected=(
  [Makefile]=1
  [README.md]=1
  [INSPIRATION_ROUND2.md]=1
  [C208_RESULT_CACHE_METRICS.md]=1
  [hat/hatSql/result_cache.go]=1
  [hat/hatSql/query.go]=1
  [hat/hatSql/c208_query_cache_metrics_test.go]=1
  [scripts/benchmark-c208.sh]=1
  [scripts/commit-c208.sh]=1
  [scripts/format-c208.sh]=1
  [scripts/race-c208.sh]=1
  [scripts/test-c208-full.sh]=1
  [scripts/test-c208-metrics.sh]=1
  [scripts/test-c208-package.sh]=1
  [scripts/vet-c208.sh]=1
)
declare -A seen=()

while IFS= read -r status_line; do
  path=${status_line:3}
  if [[ $path == *' -> '* ]]; then
    echo "unexpected rename status: $status_line" >&2
    exit 1
  fi
  if [[ ${expected[$path]+present} != present ]]; then
    echo "unexpected changed path: $status_line" >&2
    exit 1
  fi
  seen[$path]=1
done < <(git status --porcelain --untracked-files=all)

for path in "${!expected[@]}"; do
  if [[ ${seen[$path]+present} != present ]]; then
    echo "expected C208 path is missing from worktree status: $path" >&2
    exit 1
  fi
done

git fetch origin master >/dev/null
if ! git diff --quiet HEAD origin/master; then
  echo "publisher base diverged from origin/master" >&2
  exit 1
fi

git add \
  Makefile \
  README.md \
  INSPIRATION_ROUND2.md \
  C208_RESULT_CACHE_METRICS.md \
  hat/hatSql/result_cache.go \
  hat/hatSql/query.go \
  hat/hatSql/c208_query_cache_metrics_test.go \
  scripts/benchmark-c208.sh \
  scripts/commit-c208.sh \
  scripts/format-c208.sh \
  scripts/race-c208.sh \
  scripts/test-c208-full.sh \
  scripts/test-c208-metrics.sh \
  scripts/test-c208-package.sh \
  scripts/vet-c208.sh
git diff --cached --check
git commit -m 'feat(hatSql): add result-cache metrics'
git push origin HEAD:master
