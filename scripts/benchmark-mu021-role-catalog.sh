#!/usr/bin/env bash
set -euo pipefail

exec env GOMAXPROCS=1 go test ./hat/hatAuth -run '^$' -bench '^BenchmarkMU021(BeforePolicyAuthorize|AfterRoleCatalogAuthorize)$' -benchmem -benchtime=500ms -count=5
