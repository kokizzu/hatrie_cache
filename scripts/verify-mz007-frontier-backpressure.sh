#!/usr/bin/env bash
set -euo pipefail

bash ./scripts/test-mz007-frontier-backpressure.sh
go test ./hat/hatPipeline -count=1
bash ./scripts/race-mz007-frontier-backpressure.sh
go test -race ./hat/hatPipeline -count=1
bash ./scripts/vet-mz007-frontier-backpressure.sh
