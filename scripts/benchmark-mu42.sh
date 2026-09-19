#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^Benchmark(ConnectorCheckpoint|ConnectorLifecyclePauseResume)' -benchmem -count=5
