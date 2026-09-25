#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkMZ003CompactionAdmissionBaseline$|^BenchmarkMZ004SchedulerSubmit(Default|Policy)$' -benchtime=10000x -count=5
