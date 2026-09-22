#!/usr/bin/env bash
set -euo pipefail

for path in \
	TU30_COOPERATIVE_FIBER_SCHEDULER.md \
	README.md \
	BENCHMARK.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_ROUND2.md; do
	test -f "$path"
done

rg -n 'T235|TU30_COOPERATIVE_FIBER_SCHEDULER|hatFiber|cooperative fiber' \
	README.md \
	BENCHMARK.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_ROUND2.md \
	TU30_COOPERATIVE_FIBER_SCHEDULER.md

printf '%s\n' 'T235 documentation verified.'
