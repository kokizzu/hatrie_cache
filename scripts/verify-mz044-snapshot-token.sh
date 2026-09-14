#!/usr/bin/env bash
set -euo pipefail

test -f MZ044_SNAPSHOT_TOKENS.md
rg -q 'MZ-044 Session Snapshot Tokens' BENCHMARK.md
rg -q 'MZ-44.*\[x\]' INSPIRATION_BACKLOG.md
git diff --check
go test ./hat/hatSql ./hat/hatCache -count=1
go test -race ./hat/hatSql -run 'TestSQLSnapshotToken' -count=1
go vet ./hat/hatSql ./hat/hatCache
