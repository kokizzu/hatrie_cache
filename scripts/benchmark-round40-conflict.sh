#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'BenchmarkConflictEventLog' -benchmem -count=5
