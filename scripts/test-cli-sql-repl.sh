#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
go test ./cmd/hatrie-cli -run '^TestSQLREPL' -count=1
