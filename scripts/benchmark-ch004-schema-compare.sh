#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH004Final(SchemaRegistry|ExplicitResolverSameWorkload)$' -benchmem -count=5 -timeout=2m
