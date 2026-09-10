#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench 'BenchmarkSQLSnapshotProviderExecution$' -benchmem -count=5
