#!/bin/sh
set -eu

commit_message="feat: add generic streamed prewhere"
base_file=""
block_file=""
patch_file=""
staged_file=""

cleanup() {
	rm -f "$base_file" "$block_file" "$patch_file" "$staged_file"
}
trap cleanup EXIT HUP INT TERM

feature_paths='Makefile
hat/hatSql/query.go
hat/hatSql/prewhere.go
hat/hatSql/prewhere_test.go
hat/hatSql/prewhere_benchmark_test.go
BENCHMARK.md
ADOPTED_QUERY_ENGINE_IDEAS.md
scripts/test-sql-prewhere.sh
scripts/test-race-sql-prewhere.sh
scripts/benchmark-sql-prewhere.sh
scripts/format-sql-prewhere.sh
scripts/check-sql-prewhere.sh
scripts/deliver-sql-prewhere.sh'

make_target_block() {
	printf '%s\n' '.PHONY: test-sql-prewhere' 'test-sql-prewhere:'
	printf '\tbash scripts/test-sql-prewhere.sh\n\n'
	printf '%s\n' '.PHONY: test-race-sql-prewhere' 'test-race-sql-prewhere:'
	printf '\tbash scripts/test-race-sql-prewhere.sh\n\n'
	printf '%s\n' '.PHONY: format-sql-prewhere' 'format-sql-prewhere:'
	printf '\tbash scripts/format-sql-prewhere.sh\n\n'
	printf '%s\n' '.PHONY: benchmark-sql-prewhere' 'benchmark-sql-prewhere:'
	printf '\tbash scripts/benchmark-sql-prewhere.sh\n\n'
	printf '%s\n' '.PHONY: check-sql-prewhere' 'check-sql-prewhere:'
	printf '\tbash scripts/check-sql-prewhere.sh\n\n'
	printf '%s\n' '.PHONY: deliver-sql-prewhere' 'deliver-sql-prewhere:'
	printf '\tbash scripts/deliver-sql-prewhere.sh\n\n'
	printf '%s\n' '.PHONY: commit-sql-prewhere' 'commit-sql-prewhere:'
	printf '\tbash scripts/deliver-sql-prewhere.sh commit\n\n'
	printf '%s\n' '.PHONY: push-sql-prewhere' 'push-sql-prewhere:'
	printf '\tbash scripts/deliver-sql-prewhere.sh push\n'
}

stage_feature() {
	if git diff --cached --quiet; then
		:
	else
		printf '%s\n' 'refusing to stage with pre-existing staged changes' >&2
		exit 1
	fi

	base_file=$(mktemp)
	block_file=$(mktemp)
	patch_file=$(mktemp)
	git show HEAD:Makefile > "$base_file"
	if grep -F -q 'test-sql-prewhere:' "$base_file"; then
		printf '%s\n' 'SQL PREWHERE Makefile targets already exist in HEAD' >&2
		exit 1
	fi
	make_target_block > "$block_file"
	base_lines=$(wc -l < "$base_file")
	block_lines=$(wc -l < "$block_file")
	{
		printf '%s\n' 'diff --git a/Makefile b/Makefile' '--- a/Makefile' '+++ b/Makefile'
		printf '@@ -%s,0 +%s,%s @@\n' "$base_lines" "$((base_lines + 1))" "$block_lines"
		while IFS= read -r line || [ -n "$line" ]; do
			printf '+%s\n' "$line"
		done < "$block_file"
	} > "$patch_file"
	git apply --cached --check --unidiff-zero "$patch_file"
	git apply --cached --unidiff-zero "$patch_file"

	for path in $feature_paths; do
		if [ "$path" = Makefile ]; then
			continue
		fi
		git add -- "$path"
	done
	git diff --cached --check
	printf '%s\n' 'feature staged; run make commit-sql-prewhere next'
}

verify_staged_feature() {
	staged_file=$(mktemp)
	git diff --cached --name-only > "$staged_file"
	while IFS= read -r path || [ -n "$path" ]; do
		case "$path" in
			Makefile|hat/hatSql/query.go|hat/hatSql/prewhere.go|hat/hatSql/prewhere_test.go|hat/hatSql/prewhere_benchmark_test.go|BENCHMARK.md|ADOPTED_QUERY_ENGINE_IDEAS.md|scripts/test-sql-prewhere.sh|scripts/test-race-sql-prewhere.sh|scripts/benchmark-sql-prewhere.sh|scripts/format-sql-prewhere.sh|scripts/check-sql-prewhere.sh|scripts/deliver-sql-prewhere.sh)
				;;
			*)
				printf 'unexpected staged path: %s\n' "$path" >&2
				exit 1
				;;
		esac
	done < "$staged_file"
	for path in $feature_paths; do
		if grep -F -x -q "$path" "$staged_file"; then
			continue
		fi
		printf 'missing staged path: %s\n' "$path" >&2
		exit 1
	done
}

case "${1:-stage}" in
stage)
	stage_feature
	;;
commit)
	verify_staged_feature
	git commit -m "$commit_message"
	;;
push)
	git push
	;;
*)
	printf 'usage: %s [stage|commit|push]\n' "$0" >&2
	exit 2
	;;
esac
