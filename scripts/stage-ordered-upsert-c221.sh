#!/usr/bin/env bash
set -euo pipefail

base_makefile=$(mktemp)
trap 'rm -f "$base_makefile"' EXIT
git show HEAD:Makefile > "$base_makefile"
printf '%s\n' \
    '' \
    'test-ordered-upsert-c221:' \
    $'\t@bash ./scripts/test-ordered-upsert-c221.sh' \
    '' \
    'benchmark-ordered-upsert-c221:' \
    $'\t@bash ./scripts/benchmark-ordered-upsert-c221.sh' \
    '' \
    'format-ordered-upsert-c221:' \
    $'\t@bash ./scripts/format-ordered-upsert-c221.sh' \
    '' \
    'verify-ordered-upsert-c221:' \
    $'\t@bash ./scripts/verify-ordered-upsert-c221.sh' \
    '' \
    'stage-ordered-upsert-c221:' \
    $'\t@bash ./scripts/stage-ordered-upsert-c221.sh' \
    '' \
    'commit-ordered-upsert-c221:' \
    $'\t@bash ./scripts/commit-ordered-upsert-c221.sh' \
    '' \
    'push-ordered-upsert-c221:' \
    $'\t@bash ./scripts/push-ordered-upsert-c221.sh' \
    >> "$base_makefile"

git add -- \
    BENCHMARK.md \
    INSPIRATION.md \
    hat/hatDataStructure/ordered_index.go \
    hat/hatDataStructure/ordered_upsert_fastpath_test.go \
    scripts/benchmark-ordered-upsert-c221.sh \
    scripts/commit-ordered-upsert-c221.sh \
    scripts/format-ordered-upsert-c221.sh \
    scripts/push-ordered-upsert-c221.sh \
    scripts/stage-ordered-upsert-c221.sh \
    scripts/test-ordered-upsert-c221.sh \
    scripts/verify-ordered-upsert-c221.sh
blob=$(git hash-object -w "$base_makefile")
git update-index --add --cacheinfo "100644,$blob,Makefile"
git diff --cached --check
git diff --cached --name-only
