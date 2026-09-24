#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^$' -bench 'BenchmarkMZ002(ManualSnapshot|SnapshotGate)' -benchmem -count=5
