#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkClusterWriteCommitParticipant$' -benchmem -count=5
