#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
printf '%s\n' '== query observer contracts =='
rg -n -A 70 -B 8 'type SQLQuery(Observer|Event|Operator)|func \(.*sqlQueryObservation|func newSQLQueryObservation' "$root/hat/hatSql" --glob '*.go'
printf '%s\n' '== existing metrics packages =='
rg -n -A 45 -B 8 'Prometheus|OpenTelemetry|otel|Metric|Counter|Histogram' "$root/hat/hatMetrics" "$root/hat/hatMonitoring" --glob '*.go'
