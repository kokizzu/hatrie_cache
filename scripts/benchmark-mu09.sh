#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkFrontierRegistryDurableSnapshot' -benchmem -count=5
