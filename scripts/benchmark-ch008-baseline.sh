#!/usr/bin/env bash
set -euo pipefail

baseline_revision="${CH008_BASELINE_REVISION:-c75220608ecb425164f9d5736ec6e67548b07aac}"
workdir=$(mktemp -d)
cleanup() {
	rm -rf "$workdir"
}
trap cleanup EXIT
git archive --format=tar "$baseline_revision" | tar -x -C "$workdir"
cp scripts/benchmark-ch008-baseline_test.txt "$workdir/hat/hatSql/c208_result_cache_key_benchmark_test.go"
printf '%s\n' "baseline revision: $baseline_revision"
(cd "$workdir" && go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLResultCacheKeyDefault$' -benchmem -count=5)
