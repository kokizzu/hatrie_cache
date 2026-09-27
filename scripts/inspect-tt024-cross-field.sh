#!/usr/bin/env bash
set -euo pipefail

print_function() {
	local file=$1
	local name=$2
	local start
	start=$(grep -n "^func .*${name}" "$file" | head -n 1 | cut -d: -f1)
	if [[ -z "$start" ]]; then
		printf 'missing function %s in %s\n' "$name" "$file" >&2
		exit 1
	fi
	sed -n "${start},$((start + 180))p" "$file"
}

printf '%s\n' '--- text proximity resolver ---'
sed -n '1,120p' hat/hatSql/text_proximity.go
grep -n -A 24 -B 8 'type SQLTextProximity' hat/hatSql/*.go
grep -R -n -A 18 -B 6 'ResolveSQLTextProximityUnionSource' hat/hatSql
print_function hat/hatSql/text_proximity.go resolveSQLTextProximityMixedBooleanUnionIndexedSource
printf '%s\n' '--- indexed source dispatch ---'
print_function hat/hatSql/query.go resolveSQLIndexedSource
printf '%s\n' '--- TT-024 tests and benchmark ---'
sed -n '1,260p' hat/hatSql/tt024_mixed_boolean_benchmark_test.go
printf '%s\n' '--- existing benchmark wrapper ---'
sed -n '1,180p' scripts/benchmark-tt024-text.sh
