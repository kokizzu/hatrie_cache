#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkMZ004(RedundantCompactionBaseline|CoalescedCompaction)$' -benchtime=100ms -count=3
