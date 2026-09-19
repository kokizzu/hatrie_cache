#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkConnectorStartLifecycle$' -benchmem -count=5
