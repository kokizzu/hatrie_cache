#!/usr/bin/env bash
set -euo pipefail

branch="$(git branch --show-current)"
if [[ "$branch" != "codex/inspiration-next-10" ]]; then
	printf 'unexpected branch: %s\n' "$branch" >&2
	exit 1
fi

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to deliver with pre-existing staged changes' >&2
	exit 1
fi

tmp_dir="$(mktemp -d /tmp/hatrie-ch048-deliver.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT
git show HEAD:Makefile > "$tmp_dir/Makefile"
printf '%s\n' \
    '' \
    '.PHONY: format-ch048-not-between' \
    'format-ch048-not-between:' \
    $'\tbash ./scripts/format-ch048-not-between.sh' \
    '' \
    '.PHONY: test-ch048-not-between' \
    'test-ch048-not-between:' \
    $'\tbash ./scripts/test-ch048-not-between.sh' \
    '' \
    '.PHONY: benchmark-ch048-not-between' \
    'benchmark-ch048-not-between:' \
    $'\tbash ./scripts/benchmark-ch048-not-between.sh' \
    '' \
    '.PHONY: test-ch048-not-between-package' \
    'test-ch048-not-between-package:' \
    $'\tbash ./scripts/test-ch048-package.sh' \
    '' \
    '.PHONY: race-ch048-not-between-package' \
    'race-ch048-not-between-package:' \
    $'\tbash ./scripts/race-ch048-package.sh' \
    '' \
    '.PHONY: vet-ch048-not-between-package' \
    'vet-ch048-not-between-package:' \
    $'\tbash ./scripts/vet-ch048-package.sh' \
    '' \
    '.PHONY: test-ch048-not-between-all' \
    'test-ch048-not-between-all:' \
    $'\tbash ./scripts/test-ch048-all.sh' \
    '' \
    '.PHONY: deliver-ch048-not-between' \
    'deliver-ch048-not-between:' \
    $'\tbash ./scripts/deliver-ch048-not-between.sh' \
    >> "$tmp_dir/Makefile"

git add -- \
    CH048_BETWEEN_PREDICATE.md \
    CH048_NOT_BETWEEN.md \
    ENGINE_IDEAS.md \
    hat/hatSql/columnar_numeric_predicate.go \
    hat/hatSql/query.go \
    hat/hatSql/ch048_not_between_benchmark_test.go \
    scripts/benchmark-ch048-not-between.sh \
    scripts/deliver-ch048-not-between.sh \
    scripts/format-ch048-not-between.sh \
    scripts/race-ch048-package.sh \
    scripts/test-ch048-all.sh \
    scripts/test-ch048-not-between.sh \
    scripts/test-ch048-package.sh \
    scripts/vet-ch048-package.sh

makefile_blob="$(git hash-object -w "$tmp_dir/Makefile")"
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
git diff --cached --name-only
git commit -m 'feat: add CH-048 numeric NOT BETWEEN fast path [skip ci]'
git push origin HEAD
