#!/usr/bin/env bash
set -euo pipefail

files=(
    AGGREGATE_COMBINATORS.md
    ADOPTED_QUERY_ENGINE_IDEAS.md
    BENCHMARK.md
    ENGINE_IDEAS.md
    INSPIRATION_BACKLOG.md
    README.md
    hat/hatSql/aggregate_state.go
    hat/hatSql/ch036_aggregate_state_benchmark_test.go
    hat/hatSql/ch036_aggregate_state_test.go
    hat/hatSql/query.go
    scripts/commit-ch036-c236.sh
    scripts/format-ch036-c236.sh
    scripts/push-ch036-c236.sh
    scripts/stage-ch036-c236.sh
    scripts/test-ch036-c231.sh
)

for file in "${files[@]}"; do
    if [[ ! -e "$file" ]]; then
        printf 'missing CH-036 file: %s\n' "$file" >&2
        exit 1
    fi
done

git add -- "${files[@]}"
staged_makefile="$(mktemp /tmp/hatrie-cache-ch036-makefile.XXXXXX)"
trap 'rm -f "$staged_makefile"' EXIT
git show HEAD:Makefile > "$staged_makefile"
if rg -q '^test-ch036-c231:|^benchmark-ch036-c231:|^commit-ch036-c236:' "$staged_makefile"; then
    git diff --cached --check
    git diff --cached --name-status
    exit 0
fi
printf '%s\n' \
    '' \
    '.PHONY: test-ch036-c231' \
    'test-ch036-c231:' \
    $'\tbash ./scripts/test-ch036-c231.sh test' \
    '' \
    '.PHONY: test-ch036-package-c236' \
    'test-ch036-package-c236:' \
    $'\tbash ./scripts/test-ch036-c231.sh package' \
    '' \
    '.PHONY: vet-ch036-c236' \
    'vet-ch036-c236:' \
    $'\tbash ./scripts/test-ch036-c231.sh vet' \
    '' \
    '.PHONY: race-ch036-c236' \
    'race-ch036-c236:' \
    $'\tbash ./scripts/test-ch036-c231.sh race' \
    '' \
    '.PHONY: benchmark-ch036-c231' \
    'benchmark-ch036-c231:' \
    $'\tbash ./scripts/test-ch036-c231.sh benchmark' \
    '' \
    '.PHONY: format-ch036-c236' \
    'format-ch036-c236:' \
    $'\tbash ./scripts/format-ch036-c236.sh' \
    '' \
    '.PHONY: stage-ch036-c236' \
    'stage-ch036-c236:' \
    $'\tbash ./scripts/stage-ch036-c236.sh' \
    '' \
    '.PHONY: commit-ch036-c236' \
    'commit-ch036-c236: stage-ch036-c236' \
    $'\tbash ./scripts/commit-ch036-c236.sh' \
    '' \
    '.PHONY: push-ch036-c236' \
    'push-ch036-c236: commit-ch036-c236' \
    $'\tbash ./scripts/push-ch036-c236.sh' \
    >> "$staged_makefile"
blob="$(git hash-object -w "$staged_makefile")"
git update-index --cacheinfo "100644,$blob,Makefile"
git diff --cached --check
git diff --cached --name-status
