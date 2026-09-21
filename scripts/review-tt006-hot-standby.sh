#!/usr/bin/env bash
set -euo pipefail

expected_paths=(
	BENCHMARK.md
	ENGINE_IDEAS.md
	Makefile
	TT006_HOT_STANDBY_WAL_CATCHUP.md
	hat/hatReplication/tt006_hot_standby.go
	hat/hatReplication/tt006_hot_standby_benchmark_test.go
	hat/hatReplication/tt006_hot_standby_test.go
	scripts/tt006-hot-standby.sh
	scripts/stage-tt006-hot-standby.sh
	scripts/review-tt006-hot-standby.sh
	scripts/commit-tt006-hot-standby.sh
	scripts/push-tt006-hot-standby.sh
)

staged_paths="$(git diff --cached --name-only)"
if [ -z "$staged_paths" ]; then
	printf '%s\n' 'no staged paths' >&2
	exit 1
fi
for path in $staged_paths; do
	allowed=false
	for expected in "${expected_paths[@]}"; do
		if [ "$path" = "$expected" ]; then
			allowed=true
			break
		fi
	done
	if [ "$allowed" != true ]; then
		printf 'unexpected staged path: %s\n' "$path" >&2
		exit 1
	fi
done

git diff --cached --check
git diff --cached --name-status
git diff --cached --stat
