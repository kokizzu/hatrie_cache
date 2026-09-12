#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
go test ./hat/hatTopology -run 'Test(ExecuteReplicaHedged|NewReplicaHedgePolicy)' -count=1
