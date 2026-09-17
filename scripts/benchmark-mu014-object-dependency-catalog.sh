#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^$' -bench '^BenchmarkMU014Catalog(SourcesBaseline|Objects|DependencyClosure)$' -benchmem -count=5
