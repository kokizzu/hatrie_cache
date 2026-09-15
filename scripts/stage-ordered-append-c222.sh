#!/usr/bin/env bash
set -euo pipefail

base_makefile=$(mktemp)
trap 'rm -f "$base_makefile"' EXIT
git show HEAD:Makefile > "$base_makefile"
printf '%s\n' \
    '' \
    'test-ordered-append-c222:' \
    $'\t@bash ./scripts/test-ordered-append-c222.sh' \
    '' \
    'benchmark-ordered-append-c222:' \
    $'\t@bash ./scripts/benchmark-ordered-append-c222.sh' \
    '' \
    'format-ordered-append-c222:' \
    $'\t@bash ./scripts/format-ordered-append-c222.sh' \
    '' \
    'verify-ordered-append-c222:' \
    $'\t@bash ./scripts/verify-ordered-append-c222.sh' \
    '' \
    'stage-ordered-append-c222:' \
    $'\t@bash ./scripts/stage-ordered-append-c222.sh' \
    '' \
    'commit-ordered-append-c222:' \
    $'\t@bash ./scripts/commit-ordered-append-c222.sh' \
    '' \
    'push-ordered-append-c222:' \
    $'\t@bash ./scripts/push-ordered-append-c222.sh' \
    >> "$base_makefile"

git add -- \
    BENCHMARK.md \
    INSPIRATION.md \
    hat/hatDataStructure/ordered_append_fastpath_benchmark_test.go \
    hat/hatDataStructure/ordered_append_fastpath_test.go \
    hat/hatDataStructure/ordered_index.go \
    scripts/benchmark-ordered-append-c222.sh \
    scripts/commit-ordered-append-c222.sh \
    scripts/format-ordered-append-c222.sh \
    scripts/push-ordered-append-c222.sh \
    scripts/stage-ordered-append-c222.sh \
    scripts/test-ordered-append-c222.sh \
    scripts/verify-ordered-append-c222.sh
blob=$(git hash-object -w "$base_makefile")
git update-index --add --cacheinfo "100644,$blob,Makefile"
git diff --cached --check
git diff --cached --name-only
