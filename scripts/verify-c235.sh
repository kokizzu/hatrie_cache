#!/usr/bin/env bash
set -euo pipefail

mode="${1:-all}"
cache_dir="${TMPDIR:-/tmp}/hatrie-cache-c235-gocache"
rm -rf "$cache_dir"
mkdir -p "$cache_dir"
trap 'rm -rf "$cache_dir"' EXIT

case "$mode" in
format)
	gofmt -w hat/hatSql/task_profiler.go hat/hatSql/c235_task_profiler_test.go hat/hatSql/c235_task_profiler_benchmark_test.go
	;;
package)
	GOCACHE="$cache_dir" go test ./hat/hatSql -count=1
	;;
race)
	GOCACHE="$cache_dir" go test -race ./hat/hatSql -run 'TestSQLTaskProfiler' -count=1
	;;
vet)
	GOCACHE="$cache_dir" go vet ./hat/hatSql
	;;
docs)
	rg -n 'SQLTaskProfiler|C235_TASK_PROFILER.md' README.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION_ROUND2.md BENCHMARK.md C235_TASK_PROFILER.md
	;;
all)
	gofmt -w hat/hatSql/task_profiler.go hat/hatSql/c235_task_profiler_test.go hat/hatSql/c235_task_profiler_benchmark_test.go
	GOCACHE="$cache_dir" go test ./hat/hatSql -count=1
	GOCACHE="$cache_dir" go test -race ./hat/hatSql -run 'TestSQLTaskProfiler' -count=1
	GOCACHE="$cache_dir" go vet ./hat/hatSql
	rg -n 'SQLTaskProfiler|C235_TASK_PROFILER.md' README.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION_ROUND2.md BENCHMARK.md C235_TASK_PROFILER.md
	;;
*)
	printf 'unknown C235 verification mode: %s\n' "$mode" >&2
	exit 2
	;;
esac
