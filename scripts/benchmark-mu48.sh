#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkConnectorStartWithHealthPolicy(Success|OneRetry)$' -benchmem -count=5
