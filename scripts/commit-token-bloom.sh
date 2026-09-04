#!/bin/sh
set -eu

mode="${1:-commit}"
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT HUP INT TERM

expected_paths="$tmpdir/expected"
cat > "$expected_paths" <<'EOF'
ADOPTED_QUERY_ENGINE_IDEAS.md
ARCHITECTURE.md
BENCHMARK.md
DATA_STRUCTURE.md
INSPIRATION.md
README.md
Makefile
TOKEN_BLOOM_FILTER.md
hat/hatDataStructure/token_bloom_filter.go
hat/hatDataStructure/token_bloom_filter_benchmark_test.go
hat/hatDataStructure/token_bloom_filter_test.go
scripts/audit-inspiration-unchecked.sh
scripts/benchmark-token-bloom.sh
scripts/commit-token-bloom.sh
scripts/format-token-bloom.sh
scripts/test-race-token-bloom.sh
scripts/test-token-bloom-api.sh
scripts/test-token-bloom.sh
scripts/vet-token-bloom.sh
token_bloom_filter.go
token_bloom_filter_api_test.go
EOF

stage_files() {
	git diff --cached --name-only > "$tmpdir/cached"
	if [ -s "$tmpdir/cached" ]; then
		awk 'NR == FNR { allowed[$0] = 1; next } !allowed[$0] { print }' "$expected_paths" "$tmpdir/cached" > "$tmpdir/unexpected"
		if [ -s "$tmpdir/unexpected" ]; then
			printf '%s\n' 'refusing to commit unexpected pre-staged paths:' >&2
			cat "$tmpdir/unexpected" >&2
			exit 1
		fi
	fi

	git add -- ADOPTED_QUERY_ENGINE_IDEAS.md ARCHITECTURE.md BENCHMARK.md DATA_STRUCTURE.md INSPIRATION.md README.md TOKEN_BLOOM_FILTER.md hat/hatDataStructure/token_bloom_filter.go hat/hatDataStructure/token_bloom_filter_benchmark_test.go hat/hatDataStructure/token_bloom_filter_test.go scripts/audit-inspiration-unchecked.sh scripts/benchmark-token-bloom.sh scripts/commit-token-bloom.sh scripts/format-token-bloom.sh scripts/test-race-token-bloom.sh scripts/test-token-bloom-api.sh scripts/test-token-bloom.sh scripts/vet-token-bloom.sh token_bloom_filter.go token_bloom_filter_api_test.go

	git diff --cached --name-only -- Makefile > "$tmpdir/cached-makefile"
	if [ ! -s "$tmpdir/cached-makefile" ]; then
		git show HEAD:Makefile > "$tmpdir/head-makefile"
		awk '
			BEGIN { active = 0 }
			/^\.PHONY: (audit-inspiration-unchecked|test-token-bloom|format-token-bloom|test-token-bloom-api|benchmark-token-bloom|test-race-token-bloom|vet-token-bloom|stage-token-bloom|commit-token-bloom|push-token-bloom)$/ { active = 1 }
			/^\.PHONY:/ && $0 !~ /^(\.PHONY: (audit-inspiration-unchecked|test-token-bloom|format-token-bloom|test-token-bloom-api|benchmark-token-bloom|test-race-token-bloom|vet-token-bloom|stage-token-bloom|commit-token-bloom|push-token-bloom))$/ { active = 0 }
			active { print }
		' Makefile > "$tmpdir/token-targets"
		sed -i '${/^$/d;}' "$tmpdir/token-targets"
		cat "$tmpdir/head-makefile" "$tmpdir/token-targets" > "$tmpdir/feature-makefile"
		diff -u "$tmpdir/head-makefile" "$tmpdir/feature-makefile" > "$tmpdir/makefile.patch" || status=$?
		status="${status:-0}"
		if [ "$status" -gt 1 ]; then
			printf '%s\n' 'could not construct the isolated Makefile patch' >&2
			exit "$status"
		fi
		sed -i '1c\\--- a/Makefile' "$tmpdir/makefile.patch"
		sed -i '2c\\+++ b/Makefile' "$tmpdir/makefile.patch"
		git apply --cached "$tmpdir/makefile.patch"
	fi

	git diff --cached --check
	git diff --cached --name-only > "$tmpdir/staged"
	awk 'NR == FNR { allowed[$0] = 1; next } !allowed[$0] { print }' "$expected_paths" "$tmpdir/staged" > "$tmpdir/unexpected-staged"
	if [ -s "$tmpdir/unexpected-staged" ]; then
		printf '%s\n' 'staging selected an unexpected path:' >&2
		cat "$tmpdir/unexpected-staged" >&2
		exit 1
	fi
}

case "$mode" in
stage)
	stage_files
	git diff --cached --stat
	;;
commit)
	stage_files
	git commit -m 'data: add token bloom prefilter'
	;;
push)
	git push
	;;
*)
	printf 'usage: %s {stage|commit|push}\n' "$0" >&2
	exit 2
	;;
esac
