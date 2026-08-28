#!/usr/bin/env sh
set -eu

report=SQL_IMPROVEMENTS_100.md

if [ ! -f "$report" ]; then
	printf '%s\n' "missing $report" >&2
	exit 1
fi

count=$(grep -E '^[0-9]+\. \*\*P[0-9],' "$report" | wc -l | tr -d ' ')
if [ "$count" -ne 100 ]; then
	printf '%s\n' "expected 100 numbered improvement candidates, found $count" >&2
	exit 1
fi

for heading in \
	'## Query Planning And Costing' \
	'## Execution And Memory' \
	'## Indexes And Access Paths' \
	'## Relational Coverage' \
	'## Analytics And Windows' \
	'## Temporal And Streaming SQL' \
	'## Types, Expressions, And Semantics' \
	'## Durability, Recovery, And Storage' \
	'## Security And Governance' \
	'## Interfaces, Operations, And Developer Tooling' \
	'## Recommended Next Goal'
do
	if ! grep -F -q "$heading" "$report"; then
		printf '%s\n' "missing heading: $heading" >&2
		exit 1
	fi
done

printf '%s\n' "verified $report: 100 categorized improvement candidates"
