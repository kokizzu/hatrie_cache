#!/usr/bin/env bash
set -euo pipefail

gocache=/tmp/hatrie-ch-g32-gocache-20261006
rm -rf "$gocache"
trap 'rm -rf "$gocache"' EXIT
GOCACHE="$gocache" go test -tags chg32 ./hat/hatSql -run '^TestCHG32MaintenanceQueue' -count=1
