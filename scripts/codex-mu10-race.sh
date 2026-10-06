#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestDifferentialTemporalJoinCompactionScheduler' -race -count=1
