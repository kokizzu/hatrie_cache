#!/usr/bin/env bash
set -euo pipefail

awk -F'|' '
/^\| (CH|M|T)-U[0-9]+/ {
	identifier = $2
	gsub(/[[:space:]]+/, "", identifier)
	if (identifier in seen) {
		printf "duplicate idea ID: %s\n", identifier
		failed = 1
	}
	seen[identifier] = 1
	if (identifier ~ /^CH-U/) clickhouse++
	if (identifier ~ /^M-U/) materialize++
	if (identifier ~ /^T-U/) tarantool++
	total++
}
END {
	printf "ClickHouse ideas: %d\n", clickhouse
	printf "Materialize ideas: %d\n", materialize
	printf "Tarantool ideas: %d\n", tarantool
	printf "Total ideas: %d\n", total
	if (clickhouse != 50 || materialize != 50 || tarantool != 50 || total != 150 || failed) exit 1
}
' PRODUCT_IDEA_GAPS.md
