#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatProcedure -run '^$' -bench 'Benchmark(RegistryCall|DirectHandler|RegistryList)$' -benchmem -count=5 -benchtime=200ms
