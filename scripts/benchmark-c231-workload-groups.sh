#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkC231SQLWorkloadGroupAdmission$' -benchtime=200ms -count=5
