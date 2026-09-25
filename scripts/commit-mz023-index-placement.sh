#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
	:
else
	printf '%s\n' 'refusing to mix pre-staged changes into the MZ-023 commit' >&2
	exit 1
fi

base=/tmp/hatrie-cache-mz023-makefile-base
desired=/tmp/hatrie-cache-mz023-makefile-desired
raw_patch=/tmp/hatrie-cache-mz023-makefile-raw.patch
makefile_patch=/tmp/hatrie-cache-mz023-makefile.patch
trap 'rm -f "$base" "$desired" "$raw_patch" "$makefile_patch"' EXIT

git show HEAD:Makefile > "$base"
cp "$base" "$desired"
printf '%s\n' \
    '.PHONY: test-mz023-index-placement' \
    'test-mz023-index-placement:' \
    $'\tbash ./scripts/test-mz023-index-placement.sh' \
    '.PHONY: test-mz023-package' \
    'test-mz023-package:' \
    $'\tbash ./scripts/test-mz023-package.sh' \
    '.PHONY: compile-mz023-all' \
    'compile-mz023-all:' \
    $'\tbash ./scripts/compile-mz023-all.sh' \
    '.PHONY: format-mz023-index-placement' \
    'format-mz023-index-placement:' \
    $'\tbash ./scripts/format-mz023-index-placement.sh' \
    '.PHONY: benchmark-mz023-index-placement' \
    'benchmark-mz023-index-placement:' \
    $'\tbash ./scripts/benchmark-mz023-index-placement.sh' \
    '.PHONY: race-mz023-index-placement' \
    'race-mz023-index-placement:' \
    $'\tbash ./scripts/race-mz023-index-placement.sh' \
    '.PHONY: vet-mz023-index-placement' \
    'vet-mz023-index-placement:' \
    $'\tbash ./scripts/vet-mz023-index-placement.sh' \
    '.PHONY: commit-mz023-index-placement' \
    'commit-mz023-index-placement:' \
    $'\tbash ./scripts/commit-mz023-index-placement.sh' \
    '.PHONY: push-mz023-index-placement' \
    'push-mz023-index-placement:' \
    $'\tbash ./scripts/push-mz023-index-placement.sh' >> "$desired"

set +e
git diff --no-index --binary "$base" "$desired" > "$raw_patch"
diff_status=$?
set -e
if [ "$diff_status" -ne 1 ]; then
	printf '%s\n' "unexpected Makefile diff status: $diff_status" >&2
	exit "$diff_status"
fi
sed -e "s|a$base|a/Makefile|g" -e "s|b$desired|b/Makefile|g" -e "s|$base|a/Makefile|g" -e "s|$desired|b/Makefile|g" "$raw_patch" > "$makefile_patch"
git apply --cached "$makefile_patch"

git add -- \
    ENGINE_IDEAS.md \
    MZ023_INDEX_PLACEMENT.md \
    hat/hatSql/contracts.go \
    hat/hatSql/index_hint.go \
    hat/hatSql/index_strategy.go \
    hat/hatSql/index_usage.go \
    hat/hatSql/query.go \
    hat/hatSql/mz023_index_placement_test.go \
    hat/hatSql/mz023_index_placement_benchmark_test.go \
    scripts/benchmark-mz023-index-placement.sh \
    scripts/compile-mz023-all.sh \
    scripts/commit-mz023-index-placement.sh \
    scripts/format-mz023-index-placement.sh \
    scripts/push-mz023-index-placement.sh \
    scripts/race-mz023-index-placement.sh \
    scripts/test-mz023-index-placement.sh \
    scripts/test-mz023-package.sh \
    scripts/vet-mz023-index-placement.sh

git diff --cached --check
git commit -m 'feat: add opt-in SQL index cluster placement'
