#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkConnectorLifecyclePauseResumeBaseline$' -benchmem -count=5
