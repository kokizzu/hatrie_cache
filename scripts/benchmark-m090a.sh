#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^$' -bench '^BenchmarkSQLAdapterRegistryExecute(Local|ResolverOnly)$' -benchmem -count=5
