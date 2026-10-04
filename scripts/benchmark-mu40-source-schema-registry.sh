#!/usr/bin/env bash
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cache=$(mktemp -d /tmp/hatrie-cache-mu40-benchmark.XXXXXX)
trap 'rm -rf "$cache"' EXIT

cd -- "$repo"
GOCACHE="$cache" go test ./hat/hatSchema/source_schema_registry_contract_test \
	-run '^$' \
	-bench '^BenchmarkSourceSchemaRegistry' \
	-benchmem \
	-benchtime=100ms \
	-count=3
