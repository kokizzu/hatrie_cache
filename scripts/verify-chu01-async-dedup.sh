#!/usr/bin/env bash
set -euo pipefail

bash scripts/format-chu01-async-dedup.sh
go test ./hat/hatPipeline -run 'TestAsyncInsertDedup'
bash scripts/race-chu01-async-dedup.sh
go vet ./hat/hatPipeline
