#!/usr/bin/env bash
set -euo pipefail

cache_dir="$(mktemp -d /tmp/hatrie-m-g21-race-gocache.XXXXXX)"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test -race ./hat/hatSql -run '^TestTypedTable(AggregateArrangementHydratesInBoundedBatches|JoinArrangementHydratesBothInputs|AggregateArrangementHydrationProgressReportsETA|JoinArrangementHydrationProgressAggregatesInputs)$' -count=1
