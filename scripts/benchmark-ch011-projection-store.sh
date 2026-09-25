#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH011ProjectionDefinitionStore' -benchmem -benchtime=100x -count=5
