#!/usr/bin/env bash
set -euo pipefail

gocache="$PWD/build/mu034-go-cache"
GOCACHE="${GOCACHE:-$gocache}" go test ./hat/hatSql -run '^$' -bench 'BenchmarkMU034HistoricalSubscriptionCheckpoint' -benchmem -count=5
