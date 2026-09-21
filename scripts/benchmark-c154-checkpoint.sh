#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench 'BenchmarkRollingSchema(ManualRecovery|Checkpoint)' -benchmem -count=5
