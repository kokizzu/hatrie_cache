#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-m-g21-gocache.XXXXXX)"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test ./hat/hatSql -run '^TestTypedTable(AggregateArrangementHydratesInBoundedBatches|JoinArrangementHydratesBothInputs|AggregateArrangementHydrationProgressReportsETA|JoinArrangementHydrationProgressAggregatesInputs)$' -count=5
