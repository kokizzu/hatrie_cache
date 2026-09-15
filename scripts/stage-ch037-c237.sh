#!/usr/bin/env bash
set -euo pipefail

files=(
    AGGREGATE_COMBINATORS.md
    ADOPTED_QUERY_ENGINE_IDEAS.md
    BENCHMARK.md
    ENGINE_IDEAS.md
    INSPIRATION_BACKLOG.md
    hat/hatSql/arg_extreme.go
    hat/hatSql/arg_extreme_state.go
    hat/hatSql/ch037_arg_extreme_state_test.go
    hat/hatSql/ch037_arg_extreme_state_codec_test.go
    hat/hatSql/ch037_arg_extreme_state_benchmark_test.go
    hat/hatSql/query.go
    scripts/format-ch037-c237.sh
    scripts/stage-ch037-c237.sh
    scripts/commit-ch037-c237.sh
    scripts/push-ch037-c237.sh
    scripts/test-ch037-c237.sh
)

for path in "${files[@]}"; do
    if [[ ! -e "$path" ]]; then
        printf 'missing feature path: %s\n' "$path" >&2
        exit 1
    fi
done

mapfile -t staged_before < <(git diff --cached --name-only)
if ((${#staged_before[@]} != 0)); then
    printf 'pre-existing staged changes block an isolated CH-037 commit:\n' >&2
    printf '  %s\n' "${staged_before[@]}" >&2
    exit 1
fi

git add -- "${files[@]}"

tmp_dir="$(mktemp -d /tmp/hatrie-cache-stage-ch037.XXXXXX)"
trap 'rm -rf "$tmp_dir"' EXIT
git show HEAD:Makefile > "$tmp_dir/Makefile.base"
cp "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.feature"
printf '%s\n' \
    '.PHONY: test-ch037-c237' \
    'test-ch037-c237:' \
    $'\tbash ./scripts/test-ch037-c237.sh test' \
    '.PHONY: test-ch037-package-c237' \
    'test-ch037-package-c237:' \
    $'\tbash ./scripts/test-ch037-c237.sh package' \
    '.PHONY: vet-ch037-c237' \
    'vet-ch037-c237:' \
    $'\tbash ./scripts/test-ch037-c237.sh vet' \
    '.PHONY: race-ch037-c237' \
    'race-ch037-c237:' \
    $'\tbash ./scripts/test-ch037-c237.sh race' \
    '.PHONY: benchmark-ch037-c237' \
    'benchmark-ch037-c237:' \
    $'\tbash ./scripts/test-ch037-c237.sh benchmark' \
    '.PHONY: format-ch037-c237' \
    'format-ch037-c237:' \
    $'\tbash ./scripts/format-ch037-c237.sh' >> "$tmp_dir/Makefile.feature"

diff_status=0
diff -u --label a/Makefile --label b/Makefile "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.feature" > "$tmp_dir/Makefile.patch" || diff_status=$?
if [[ "$diff_status" -ne 1 ]]; then
    printf 'failed to build isolated Makefile patch (diff status %s)\n' "$diff_status" >&2
    exit 1
fi
git apply --cached "$tmp_dir/Makefile.patch"
git diff --cached --check
git diff --cached --name-only
