#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkMU014CatalogSourcesBaseline$' -benchmem -count=5
