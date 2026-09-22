#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}
case "$mode" in
format)
	command gofmt -w \
		hat/hatSql/m224_materialized_hydration_progress_test.go \
		hat/hatSql/m224_materialized_hydration_progress_benchmark_test.go \
		hat/hatSql/materialized.go \
		hat/hatSql/materialized_hydration.go \
		hat/hatSql/contracts.go \
		hat/hatSql/query.go
	;;
test)
	command go test ./hat/hatSql -run '^TestM224MaterializedViewHydrationProgress' -count=1
	;;
benchmark)
	command go test ./hat/hatSql -run '^$' -bench '^BenchmarkM224MaterializedViewHydrationProgress' -benchmem -count=5
	;;
race)
	command go test ./hat/hatSql -run '^TestM224MaterializedViewHydrationProgress' -race -count=1
	;;
vet)
	command go vet ./hat/hatSql
	;;
package)
	command go test ./hat/hatSql -count=1
	;;
docs)
	command test -s M224_MATERIALIZED_HYDRATION_PROGRESS.md
	command rg -n 'M224|HydrationProgressSourceResolver|m224-materialized-hydration-progress' \
		M224_MATERIALIZED_HYDRATION_PROGRESS.md INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
	;;
status)
	command git diff --check
	command git diff --stat
	command git status --short
	;;
staged)
	command git diff --cached --check
	command git diff --cached --stat
	command git diff --cached --name-only
	;;
diff)
	command git diff -- \
		ADOPTED_QUERY_ENGINE_IDEAS.md \
		BENCHMARK.md \
		INSPIRATION_ROUND2.md \
		Makefile \
		hat/hatSql/contracts.go \
		hat/hatSql/materialized.go \
		hat/hatSql/materialized_hydration.go \
		hat/hatSql/query.go
	;;
*)
	printf 'usage: %s {format|test|benchmark|race|vet|package|docs|status|staged|diff}\n' "$0" >&2
	exit 2
	;;
esac
