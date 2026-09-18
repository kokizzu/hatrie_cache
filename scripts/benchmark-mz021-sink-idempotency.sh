#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkMZ021(ProvidedIdempotencyKey|DerivedIdempotencyKey)$' -benchmem -benchtime=500ms -count=5
