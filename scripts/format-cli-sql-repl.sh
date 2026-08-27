#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
gofmt -w ./cmd/hatrie-cli/sql_repl.go ./cmd/hatrie-cli/sql_repl_test.go
