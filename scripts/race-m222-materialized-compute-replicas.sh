#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestM222MaterializedViewComputeReplica' -count=1
