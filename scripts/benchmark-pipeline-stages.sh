#!/usr/bin/env bash
set -euo pipefail

go test -run '^$' -bench BenchmarkPipelineRun -benchmem ./hat/hatPipeline
