#!/usr/bin/env bash
set -euo pipefail

declare -A expected=(
  [Makefile]=1
  [README.md]=1
  [INSPIRATION_ROUND2.md]=1
  [M249_READ_FENCE.md]=1
  [hat/hatCache/journal.go]=1
  [hat/hatCache/journal_read_fence_test.go]=1
  [hat/hatCache/journal_read_fence_benchmark_test.go]=1
  [scripts/benchmark-m249.sh]=1
  [scripts/commit-m249.sh]=1
  [scripts/format-m249.sh]=1
  [scripts/race-m249.sh]=1
  [scripts/test-m249-full.sh]=1
  [scripts/test-m249-package.sh]=1
  [scripts/test-m249-read-fence.sh]=1
  [scripts/vet-m249.sh]=1
)

status=$(git status --porcelain --untracked-files=all)
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  path=${line:3}
  if [[ -z "${expected[$path]+present}" ]]; then
    printf 'unexpected worktree change: %s\n' "$line" >&2
    exit 1
  fi
done <<< "$status"

git fetch origin master >/dev/null
if ! git diff --quiet HEAD origin/master; then
  printf '%s\n' 'origin/master advanced or publisher base is not clean; refusing to publish' >&2
  exit 1
fi

git add \
  Makefile \
  README.md \
  INSPIRATION_ROUND2.md \
  M249_READ_FENCE.md \
  hat/hatCache/journal.go \
  hat/hatCache/journal_read_fence_test.go \
  hat/hatCache/journal_read_fence_benchmark_test.go \
  scripts/benchmark-m249.sh \
  scripts/commit-m249.sh \
  scripts/format-m249.sh \
  scripts/race-m249.sh \
  scripts/test-m249-full.sh \
  scripts/test-m249-package.sh \
  scripts/test-m249-read-fence.sh \
  scripts/vet-m249.sh
git diff --cached --check
git commit -m 'feat(hatCache): add journal read fences'
git push origin HEAD:master
