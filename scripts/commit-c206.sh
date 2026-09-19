#!/bin/sh
set -eu

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to commit because the index already contains staged changes' >&2
	exit 1
fi

git add -- \
	BENCHMARK.md \
	C206_QUERY_CACHE_ELIGIBILITY.md \
	INSPIRATION_ROUND2.md \
	hat/hatSql/c206_query_cache_eligibility_test.go \
	hat/hatSql/c206_query_cache_eligibility_benchmark_test.go \
	scripts/benchmark-c206.sh \
	scripts/commit-c206.sh \
	scripts/format-c206.sh \
	scripts/race-c206.sh \
	scripts/test-c206.sh \
	scripts/vet-c206.sh

makefile_stage=$(mktemp)
trap 'rm -f "$makefile_stage"' EXIT
git show HEAD:Makefile >"$makefile_stage"
printf '%s\n' '' '.PHONY: test-c206' 'test-c206:' >>"$makefile_stage"
printf '\tsh scripts/test-c206.sh\n' >>"$makefile_stage"
printf '%s\n' '' '.PHONY: benchmark-c206' 'benchmark-c206:' >>"$makefile_stage"
printf '\tsh scripts/benchmark-c206.sh\n' >>"$makefile_stage"
printf '%s\n' '' '.PHONY: race-c206' 'race-c206:' >>"$makefile_stage"
printf '\tsh scripts/race-c206.sh\n' >>"$makefile_stage"
printf '%s\n' '' '.PHONY: vet-c206' 'vet-c206:' >>"$makefile_stage"
printf '\tsh scripts/vet-c206.sh\n' >>"$makefile_stage"
printf '%s\n' '' '.PHONY: format-c206' 'format-c206:' >>"$makefile_stage"
printf '\tsh scripts/format-c206.sh\n' >>"$makefile_stage"
printf '%s\n' '' '.PHONY: commit-c206' 'commit-c206:' >>"$makefile_stage"
printf '\tsh scripts/commit-c206.sh\n' >>"$makefile_stage"
makefile_blob=$(git hash-object -w "$makefile_stage")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git commit -m 'docs: verify ClickHouse-style query cache eligibility'
