#!/usr/bin/env bash
set -euo pipefail

declare -A expected=(
  [Makefile]=1
  [README.md]=1
  [INSPIRATION_ROUND2.md]=1
  [C210_TYPED_TABLE_HISTOGRAM.md]=1
  [hat/hatSql/typed_table_histogram.go]=1
  [hat/hatSql/c210_typed_table_histogram_test.go]=1
  [hat/hatSql/c210_typed_table_histogram_benchmark_test.go]=1
  [scripts/benchmark-c210.sh]=1
  [scripts/commit-c210.sh]=1
  [scripts/format-c210.sh]=1
  [scripts/race-c210.sh]=1
  [scripts/test-c210-full.sh]=1
  [scripts/test-c210-package.sh]=1
  [scripts/test-c210-histogram.sh]=1
  [scripts/vet-c210.sh]=1
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
    echo "expected C210 path is missing from worktree status: $path" >&2
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
  C210_TYPED_TABLE_HISTOGRAM.md \
  hat/hatSql/typed_table_histogram.go \
  hat/hatSql/c210_typed_table_histogram_test.go \
  hat/hatSql/c210_typed_table_histogram_benchmark_test.go \
  scripts/benchmark-c210.sh \
  scripts/commit-c210.sh \
  scripts/format-c210.sh \
  scripts/race-c210.sh \
  scripts/test-c210-full.sh \
  scripts/test-c210-package.sh \
  scripts/test-c210-histogram.sh \
  scripts/vet-c210.sh
git diff --cached --check
git commit -m 'feat(hatSql): add typed-table histograms'
git push origin HEAD:master
