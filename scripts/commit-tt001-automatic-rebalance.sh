#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
  printf '%s\n' 'refusing to commit: the index already contains staged changes' >&2
  exit 1
fi

base_file=$(mktemp)
desired_file=$(mktemp)
raw_patch=$(mktemp)
makefile_patch=$(mktemp)
trap 'rm -f "$base_file" "$desired_file" "$raw_patch" "$makefile_patch"' EXIT

git show HEAD:Makefile > "$base_file"
{
  printf '%s\n' "$(<"$base_file")"
  printf '%s\n' \
    '' \
    '.PHONY: test-tt001-automatic-rebalance' \
    'test-tt001-automatic-rebalance:' \
    $'\tbash ./scripts/test-tt001-automatic-rebalance.sh' \
    '' \
    '.PHONY: format-tt001-automatic-rebalance' \
    'format-tt001-automatic-rebalance:' \
    $'\tbash ./scripts/format-tt001-automatic-rebalance.sh' \
    '' \
    '.PHONY: benchmark-tt001-automatic-rebalance' \
    'benchmark-tt001-automatic-rebalance:' \
    $'\tbash ./scripts/benchmark-tt001-automatic-rebalance.sh' \
    '' \
    '.PHONY: test-tt001-package' \
    'test-tt001-package:' \
    $'\tbash ./scripts/test-tt001-package.sh' \
    '' \
    '.PHONY: race-tt001-automatic-rebalance' \
    'race-tt001-automatic-rebalance:' \
    $'\tbash ./scripts/race-tt001-automatic-rebalance.sh' \
    '' \
    '.PHONY: vet-tt001-automatic-rebalance' \
    'vet-tt001-automatic-rebalance:' \
    $'\tbash ./scripts/vet-tt001-automatic-rebalance.sh' \
    '' \
    '.PHONY: compile-tt001-all' \
    'compile-tt001-all:' \
    $'\tbash ./scripts/compile-tt001-all.sh' \
    '' \
    '.PHONY: commit-tt001-automatic-rebalance' \
    'commit-tt001-automatic-rebalance:' \
    $'\tbash ./scripts/commit-tt001-automatic-rebalance.sh' \
    '' \
    '.PHONY: push-tt001-automatic-rebalance' \
    'push-tt001-automatic-rebalance:' \
    $'\tbash ./scripts/push-tt001-automatic-rebalance.sh'
} > "$desired_file"

set +e
git diff --no-index --src-prefix=a/ --dst-prefix=b/ "$base_file" "$desired_file" > "$raw_patch"
diff_status=$?
set -e
if [[ "$diff_status" -ne 1 ]]; then
  printf 'unexpected Makefile diff status: %d\n' "$diff_status" >&2
  exit 1
fi
sed \
  -e "s|a$base_file|a/Makefile|g" \
  -e "s|b$desired_file|b/Makefile|g" \
  -e "s|$base_file|Makefile|g" \
  -e "s|$desired_file|Makefile|g" \
  "$raw_patch" > "$makefile_patch"
git apply --cached "$makefile_patch"

git add -- \
  ENGINE_IDEAS.md \
  TT001_AUTOMATIC_BUCKET_REBALANCE.md \
  hat/hatTopology/tt001_automatic_rebalance.go \
  hat/hatTopology/tt001_automatic_rebalance_test.go \
  hat/hatTopology/tt001_automatic_rebalance_benchmark_test.go \
  scripts/benchmark-tt001-automatic-rebalance.sh \
  scripts/commit-tt001-automatic-rebalance.sh \
  scripts/compile-tt001-all.sh \
  scripts/format-tt001-automatic-rebalance.sh \
  scripts/push-tt001-automatic-rebalance.sh \
  scripts/race-tt001-automatic-rebalance.sh \
  scripts/test-tt001-automatic-rebalance.sh \
  scripts/test-tt001-package.sh \
  scripts/vet-tt001-automatic-rebalance.sh

git diff --cached --check
git commit -m 'feat: add automatic bucket rebalance planning'
