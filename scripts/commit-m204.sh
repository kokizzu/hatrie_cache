#!/usr/bin/env bash
set -euo pipefail

declare -A expected=(
  [Makefile]=1
  [README.md]=1
  [INSPIRATION_ROUND2.md]=1
  [M204_BOUNDED_SUBSCRIPTIONS.md]=1
  [hat/hatCache/journal_subscription.go]=1
  [hat/hatCache/m204_bounded_subscription_benchmark_test.go]=1
  [hat/hatCache/m204_bounded_subscription_test.go]=1
  [scripts/benchmark-m204.sh]=1
  [scripts/commit-m204.sh]=1
  [scripts/format-m204.sh]=1
  [scripts/race-m204.sh]=1
  [scripts/test-m204-bounded.sh]=1
  [scripts/test-m204-full.sh]=1
  [scripts/test-m204-package.sh]=1
  [scripts/vet-m204.sh]=1
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
  M204_BOUNDED_SUBSCRIPTIONS.md \
  hat/hatCache/journal_subscription.go \
  hat/hatCache/m204_bounded_subscription_benchmark_test.go \
  hat/hatCache/m204_bounded_subscription_test.go \
  scripts/benchmark-m204.sh \
  scripts/commit-m204.sh \
  scripts/format-m204.sh \
  scripts/race-m204.sh \
  scripts/test-m204-bounded.sh \
  scripts/test-m204-full.sh \
  scripts/test-m204-package.sh \
  scripts/vet-m204.sh
git diff --cached --check
git commit -m 'feat(hatCache): bound journal subscriptions'
git push origin HEAD:master
