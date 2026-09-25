#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkReplicaHotHandoff(WarmTail|DirectTailControl)$' -benchmem -count=5
