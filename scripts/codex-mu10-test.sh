#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestDifferentialTemporalJoinCompactionScheduler' -count=1
