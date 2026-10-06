#!/usr/bin/env bash
set -euo pipefail

gocache=/tmp/hatrie-ch-g32-vet-gocache-20261006
rm -rf "$gocache"
trap 'rm -rf "$gocache"' EXIT
GOCACHE="$gocache" go vet -tags chg32 ./hat/hatSql
