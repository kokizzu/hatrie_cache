#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkMZ038(SourceLagAlertObserve|SourceLagAlertThresholdControl)$' -benchmem -count=5
