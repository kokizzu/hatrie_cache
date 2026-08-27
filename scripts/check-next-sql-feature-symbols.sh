#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

check() {
	title=$1
	pattern=$2
	printf '\n== %s ==\n' "$title"
	if ! rg -n -w -i "$pattern" "$root/hat/hatSql" "$root/hat/hatCache" "$root/cmd" --glob '*.go'; then
		printf '(no exact symbol)\n'
	fi
}

check 'Table functions' 'TableFunction'
check 'JSON path expressions' 'JSON_EXTRACT'
check 'JSON path indexes' 'JSONPathIndex'
check 'Bitmap SQL indexes' 'BitmapIndex'
check 'Approximate cardinality aggregate' 'APPROX_COUNT_DISTINCT'
check 'Approximate percentile aggregate' 'APPROX_PERCENTILE'
check 'Approximate top-k aggregate' 'APPROX_TOP_K'
check 'Sampling clause' 'TABLESAMPLE'
check 'Plan regression guard' 'PlanGuard'
check 'OpenTelemetry exporter' 'OpenTelemetry'
check 'Prometheus query metric' 'hatrie_cache_sql'
