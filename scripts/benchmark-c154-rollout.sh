#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench 'Benchmark(CheckRollingCompatibility|RollingSchemaDeployment)' -benchmem -benchtime=200ms -count=5
