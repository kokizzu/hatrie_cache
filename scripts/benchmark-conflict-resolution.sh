#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
go test ./hat/hatReplication -run '^$' -bench BenchmarkResolveConflictVersion -benchmem -count=1
