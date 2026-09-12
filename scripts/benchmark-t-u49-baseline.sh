#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
go test ./hat/hatTopology -run '^$' -bench '^BenchmarkReplicaReadSequential$' -benchmem -count=5
