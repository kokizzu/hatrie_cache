#!/usr/bin/env bash
set -euo pipefail

cache_dir="/tmp/hatrie-cache-m040-bench-cache-$$"
trap 'rm -rf "$cache_dir"' EXIT
GOCACHE="$cache_dir" go test hat/hatSchema/source_schema_registry.go hat/hatSchema/source_schema_registry_test.go -run '^$' -bench 'BenchmarkSourceSchema(DirectMetadataCheck|RegistryValidate|RegistryActivateSame|RegistryNew|RegistryEagerMapBaseline)$' -benchmem -count=5
