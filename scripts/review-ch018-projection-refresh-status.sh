#!/usr/bin/env bash
set -euo pipefail

paths=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	CH018_PROJECTION_REFRESH_STATUS.md
	INSPIRATION_BACKLOG.md
	README.md
	Makefile
	hat/hatSql/ch018_projection_refresh_status.go
	hat/hatSql/ch018_projection_refresh_status_test.go
	hat/hatSql/ch018_projection_refresh_status_benchmark_test.go
	hat/hatSql/incremental_projection.go
	scripts/benchmark-ch018-projection-refresh-status.sh
	scripts/format-ch018-projection-refresh-status-permanent.sh
	scripts/test-ch018-projection-refresh-status.sh
	scripts/test-race-ch018-projection-refresh-status.sh
	scripts/verify-ch018-projection-refresh-status.sh
	scripts/vet-ch018-projection-refresh-status.sh
)

git diff --check -- "${paths[@]}"
printf '%s\n' 'CH-18 working-tree paths:'
git status --short -- "${paths[@]}"
printf '%s\n' 'CH-18 diff stat:'
git diff --stat -- "${paths[@]}"
printf '%s\n' 'CH-18 tracked diffs excluding the concurrently edited Makefile:'
git diff -- ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md INSPIRATION_BACKLOG.md README.md hat/hatSql/incremental_projection.go
