#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestGlobalJoinBroadcastPlanner' -count=1
