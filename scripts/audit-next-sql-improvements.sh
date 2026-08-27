#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

report() {
    title=$1
    pattern=$2
    printf '\n== %s ==\n' "$title"
    if ! rg -l -i "$pattern" \
        "$root/hat/hatSql" \
        "$root/hat/hatCache" \
        "$root/cmd" \
        "$root/svelte-mpa" \
        "$root/SQL.md" \
        "$root/README.md" \
        --glob '*.go' \
        --glob '*.svelte' \
        --glob '*.ts' \
        --glob '*.md'; then
        printf '(no matches)\n'
    fi
}

report 'MERGE, upsert, and RETURNING' 'MERGE|UPSERT|RETURNING|ON CONFLICT'
report 'Atomic transactions and savepoints' 'SAVEPOINT|ROLLBACK TO|RELEASE SAVEPOINT|BEGIN ATOMIC'
report 'Table functions and JSON paths' 'table function|TableFunction|JSON_PATH|json path|JSON_EXTRACT|json_extract'
report 'JSON and bitmap indexes' 'JSON index|JsonIndex|BitmapIndex|bitmap index'
report 'Approximate aggregates and sampling' 'APPROX_COUNT_DISTINCT|percentile|TOP_K|approximate aggregate|TABLESAMPLE|SAMPLE'
report 'Plan guards and telemetry exporters' 'plan regression|PlanGuard|OpenTelemetry|Prometheus|prometheus|otel|spill.*metric|index.*metric'
