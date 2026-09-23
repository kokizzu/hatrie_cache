#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkT242AuditAllOperationsBaseline$' -benchmem -count=1
