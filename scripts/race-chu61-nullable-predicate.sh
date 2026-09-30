#!/usr/bin/env bash
set -euo pipefail

GOCACHE="$PWD/.gocache-race" go test -race ./hat/hatSql -run 'TestCHU61Nullable' -count=1
