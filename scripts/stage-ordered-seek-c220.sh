#!/usr/bin/env bash
set -euo pipefail

base_makefile=$(mktemp)
trap 'rm -f "$base_makefile"' EXIT
git show HEAD:Makefile > "$base_makefile"
printf '%s\n' \
    '' \
    'inspect-ordered-c220:' \
    $'\t@bash ./scripts/inspect-ordered-c220.sh' \
    '' \
    'test-ordered-seek-c220:' \
    $'\t@bash ./scripts/test-ordered-seek-c220.sh' \
    '' \
    'benchmark-ordered-seek-c220:' \
    $'\t@bash ./scripts/benchmark-ordered-seek-c220.sh' \
    '' \
    'format-ordered-seek-c220:' \
    $'\t@bash ./scripts/format-ordered-seek-c220.sh' \
    '' \
    'verify-ordered-seek-c220:' \
    $'\t@bash ./scripts/verify-ordered-seek-c220.sh' \
    '' \
    'test-ordered-seek-c220-package:' \
    $'\t@bash ./scripts/test-ordered-seek-c220-package.sh' \
    '' \
    'stage-ordered-seek-c220:' \
    $'\t@bash ./scripts/stage-ordered-seek-c220.sh' \
    '' \
    'commit-ordered-seek-c220:' \
    $'\t@bash ./scripts/commit-ordered-seek-c220.sh' \
    '' \
    'push-ordered-seek-c220:' \
    $'\t@bash ./scripts/push-ordered-seek-c220.sh' \
    >> "$base_makefile"

git add -- \
    BENCHMARK.md \
    INSPIRATION.md \
    hat/hatDataStructure/ordered_reverse.go \
    hat/hatDataStructure/ordered_seek_fastpath_benchmark_test.go \
    hat/hatDataStructure/ordered_seek_fastpath_test.go \
    hat/hatDataStructure/ordered_snapshot_cursor.go \
    scripts/benchmark-ordered-seek-c220.sh \
    scripts/commit-ordered-seek-c220.sh \
    scripts/format-ordered-seek-c220.sh \
    scripts/inspect-ordered-c220.sh \
    scripts/push-ordered-seek-c220.sh \
    scripts/stage-ordered-seek-c220.sh \
    scripts/test-ordered-seek-c220-package.sh \
    scripts/test-ordered-seek-c220.sh \
    scripts/verify-ordered-seek-c220.sh
blob=$(git hash-object -w "$base_makefile")
git update-index --add --cacheinfo "100644,$blob,Makefile"
git diff --cached --check
git diff --cached --name-only
