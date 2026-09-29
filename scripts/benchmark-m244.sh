#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
baseline_ref=${M244_BASELINE_REF:-HEAD^}
baseline=$(mktemp -d /tmp/hatrie-cache-m244-baseline.XXXXXX)
baseline_cache=$(mktemp -d /tmp/hatrie-cache-m244-baseline-gocache.XXXXXX)
candidate_cache=$(mktemp -d /tmp/hatrie-cache-m244-candidate-gocache.XXXXXX)
cleanup() {
	git -C "$repo_root" worktree remove --force "$baseline" >/dev/null 2>&1 || true
	rm -rf "$baseline_cache" "$candidate_cache"
}
trap cleanup EXIT

git -C "$repo_root" worktree add --detach "$baseline" "$baseline_ref" >/dev/null
cp "$repo_root/hat/hatSql/m244_arrangement_stats_benchmark_test.go" "$baseline/hat/hatSql/m244_arrangement_stats_benchmark_test.go"

printf '%s\n' '--- baseline arrangement stats ---'
(cd "$baseline" && GOCACHE="$baseline_cache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkM244TypedTableAggregateArrangementsStats$' -benchmem -cpu=1 -benchtime=200ms -count=7)
printf '%s\n' '--- candidate arrangement stats ---'
(cd "$repo_root" && GOCACHE="$candidate_cache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkM244TypedTableAggregateArrangementsStats$' -benchmem -cpu=1 -benchtime=200ms -count=7)
printf '%s\n' '--- candidate compaction metric ---'
(cd "$repo_root" && GOCACHE="$candidate_cache" go test ./hat/hatSql -run '^$' -bench '^BenchmarkTypedTableAggregateCompactionStats$' -benchmem -cpu=1 -benchtime=200ms -count=5)
