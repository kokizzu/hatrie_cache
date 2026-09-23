#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkT242AuditAllOperations(Baseline|Enabled)$' -benchmem -count=1
