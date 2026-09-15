#!/usr/bin/env bash
set -euo pipefail

mode=${1:-apply}
if [[ "$mode" == "plan" ]]; then
	printf '%s\n' 'CH-U08 files to stage:'
	printf '%s\n' \
		Makefile \
		README.md \
		PRODUCT_IDEA_GAPS.md \
		ADOPTED_QUERY_ENGINE_IDEAS.md \
		BENCHMARK.md \
		CHU08_AUTOMATIC_SQL_RESULT_CACHE.md \
		hat/hatCache/main.go \
		hat/hatCache/sql_query.go \
		hat/hatCache/sql_result_cache_auto.go \
		hat/hatCache/ch008_auto_result_cache_test.go \
		hat/hatCache/ch008_auto_result_cache_benchmark_test.go \
		scripts/test-chu08-c248.sh \
		scripts/benchmark-chu08-c248.sh \
		scripts/format-chu08-c248.sh \
		scripts/test-chu08-package-c248.sh \
		scripts/test-chu08-repo-c248.sh \
		scripts/race-chu08-c248.sh \
		scripts/vet-chu08-c248.sh \
		scripts/verify-chu08-docs-c248.sh \
		scripts/review-chu08-c248.sh \
		scripts/stage-chu08-c248.sh \
		scripts/commit-chu08-c248.sh \
		scripts/push-chu08-c248.sh
	exit 0
fi
if [[ "$mode" != "apply" ]]; then
	printf 'usage: %s [plan|apply]\n' "$0" >&2
	exit 2
fi

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to stage CH-U08 while the index already contains changes' >&2
	exit 1
fi

feature_files=(
	README.md
	PRODUCT_IDEA_GAPS.md
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	CHU08_AUTOMATIC_SQL_RESULT_CACHE.md
	hat/hatCache/main.go
	hat/hatCache/sql_query.go
	hat/hatCache/sql_result_cache_auto.go
	hat/hatCache/ch008_auto_result_cache_test.go
	hat/hatCache/ch008_auto_result_cache_benchmark_test.go
	scripts/test-chu08-c248.sh
	scripts/benchmark-chu08-c248.sh
	scripts/format-chu08-c248.sh
	scripts/test-chu08-package-c248.sh
	scripts/test-chu08-repo-c248.sh
	scripts/race-chu08-c248.sh
	scripts/vet-chu08-c248.sh
	scripts/verify-chu08-docs-c248.sh
	scripts/review-chu08-c248.sh
	scripts/stage-chu08-c248.sh
	scripts/commit-chu08-c248.sh
	scripts/push-chu08-c248.sh
)
for path in "${feature_files[@]}"; do
	if [[ ! -f "$path" ]]; then
		printf 'missing CH-U08 file: %s\n' "$path" >&2
		exit 1
	fi
done

git add -- "${feature_files[@]}"

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-chu08-stage.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT
base_makefile="$tmp_dir/Makefile.base"
candidate_makefile="$tmp_dir/Makefile.candidate"
makefile_patch="$tmp_dir/Makefile.patch"
git show HEAD:Makefile > "$base_makefile"
cp "$base_makefile" "$candidate_makefile"
printf '%s\n' \
	'.PHONY: test-chu08-c248' \
	'test-chu08-c248:' \
	$'\t@bash ./scripts/test-chu08-c248.sh' \
	'.PHONY: benchmark-chu08-c248' \
	'benchmark-chu08-c248:' \
	$'\t@bash ./scripts/benchmark-chu08-c248.sh' \
	'.PHONY: format-chu08-c248' \
	'format-chu08-c248:' \
	$'\t@bash ./scripts/format-chu08-c248.sh' \
	'.PHONY: test-chu08-package-c248' \
	'test-chu08-package-c248:' \
	$'\t@bash ./scripts/test-chu08-package-c248.sh' \
	'.PHONY: test-chu08-repo-c248' \
	'test-chu08-repo-c248:' \
	$'\t@bash ./scripts/test-chu08-repo-c248.sh' \
	'.PHONY: race-chu08-c248' \
	'race-chu08-c248:' \
	$'\t@bash ./scripts/race-chu08-c248.sh' \
	'.PHONY: vet-chu08-c248' \
	'vet-chu08-c248:' \
	$'\t@bash ./scripts/vet-chu08-c248.sh' \
	'.PHONY: verify-chu08-docs-c248' \
	'verify-chu08-docs-c248:' \
	$'\t@bash ./scripts/verify-chu08-docs-c248.sh' \
	'.PHONY: review-chu08-c248' \
	'review-chu08-c248:' \
	$'\t@bash ./scripts/review-chu08-c248.sh' \
	'.PHONY: stage-chu08-c248' \
	'stage-chu08-c248:' \
	$'\t@bash ./scripts/stage-chu08-c248.sh' \
	'.PHONY: commit-chu08-c248' \
	'commit-chu08-c248:' \
	$'\t@bash ./scripts/commit-chu08-c248.sh' \
	'.PHONY: push-chu08-c248' \
	'push-chu08-c248:' \
	$'\t@bash ./scripts/push-chu08-c248.sh' >> "$candidate_makefile"

set +e
diff -u "$base_makefile" "$candidate_makefile" > "$makefile_patch"
diff_status=$?
set -e
if [[ "$diff_status" -ne 1 ]]; then
	printf 'unexpected Makefile diff status: %d\n' "$diff_status" >&2
	exit 1
fi
sed -i \
	-e '1s|^--- .*|--- a/Makefile|' \
	-e '2s|^+++ .*|+++ b/Makefile|' \
	"$makefile_patch"
git apply --cached "$makefile_patch"

git diff --cached --check
printf '%s\n' 'CH-U08 staged with an exact allowlist.'
