#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
mode=${AUDIT_SQL_MODE:-overview}

report_sql() {
	title=$1
	pattern=$2
	printf '\n== %s ==\n' "$title"
    if ! rg -l -i "$pattern" \
        "$root/hat/hatSchema" \
		"$root/hat/hatSql" \
		"$root/cmd" \
		"$root/README.md" \
		"$root/SQL.md" \
		"$root/BENCHMARK.md" \
		--glob '*.go' \
		--glob '*.md'; then
		printf '(no matches)\n'
	fi
}

report_cache() {
    title=$1
    pattern=$2
    printf '\n== %s ==\n' "$title"
    if ! rg -l -i "$pattern" "$root/hat/hatCache" --glob '*.go'; then
        printf '(no matches)\n'
    fi
}

report_ui() {
    title=$1
    pattern=$2
    printf '\n== %s ==\n' "$title"
    if ! rg -l -i "$pattern" "$root" \
        --glob '*.svelte' \
        --glob '*.html' \
        --glob '*.js' \
        --glob '*.ts'; then
        printf '(no matches)\n'
    fi
}

case "$mode" in
overview)
    report_sql 'Migrations and model generation' 'type Migration|func \(.*Migration.*\) (Apply|Revert)|modelgen|GenerateGo'
    report_sql 'Constraints and error contracts' 'NotNull|Unique|Check|Foreign|SQLCodedError|SQLErrorCode'
    report_sql 'Snapshots, benchmarks, and query observability' 'QuerySnapshot|BenchmarkSQL|QueryObserver|EXPLAIN ANALYZE'
    report_cache 'Cache fault injection and monitoring' 'disk.full|corrupt spill|Fault|Monitoring'
    ;;
migrations)
    report_sql 'Versioned reversible migrations' 'type Migration|func \(.*Migration.*\) (Apply|Revert)|MigrationApplyAndRevert'
    report_sql 'Generated typed models' 'GenerateGo|modelgen|typed Go model'
    ;;
types)
    report_sql 'SQL scalar types' 'DECIMAL|UUID|DATE|TIMESTAMP|DURATION|BINARY|SQLType'
    report_sql 'Collation and normalization' 'SQLCollation|collation|case.insensitive|Unicode|normaliz'
    ;;
constraints)
    report_sql 'Constraints' 'NotNull|Unique|Check|Foreign|constraint'
    report_sql 'Coded errors' 'SQLCodedError|SQLErrorCode|ErrSQL'
    ;;
quality)
    report_sql 'Snapshots and benchmarks' 'QuerySnapshot|snapshot.*query|BenchmarkSQL|rows/sec|allocs|spill'
    report_sql 'Cancellation and query observability' 'cancell|QueryObserver|QueryEvent|EXPLAIN ANALYZE|query metric'
    report_cache 'Disk-full fault coverage' 'disk.full|ENOSPC'
    report_cache 'Corrupt spill-file coverage' 'corrupt spill|spill.*corrupt'
    report_cache 'Interrupted-write fault coverage' 'interrupted write|write.*interrupted'
    report_cache 'Cache monitoring service' 'MonitoringServer|StartMonitoring|MonitoringOptions'
    report_ui 'Web UI coverage' 'namespace|schema|index|query plan|query metric'
    ;;
*)
    printf 'unknown AUDIT_SQL_MODE: %s\n' "$mode" >&2
    exit 2
    ;;
esac
