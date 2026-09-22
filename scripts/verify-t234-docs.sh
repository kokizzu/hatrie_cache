#!/usr/bin/env bash
set -euo pipefail

for path in \
	TT034_EARLY_TRANSACTION_CONFLICTS.md \
	README.md \
	BENCHMARK.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_ROUND2.md; do
	test -f "$path"
done

rg -n 'TT034_EARLY_TRANSACTION_CONFLICTS|EarlyConflictDetection|T234' \
	TT034_EARLY_TRANSACTION_CONFLICTS.md \
	README.md \
	BENCHMARK.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	INSPIRATION_ROUND2.md

printf '%s\n' 'T234 documentation verified.'
