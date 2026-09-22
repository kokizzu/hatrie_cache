#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM222MaterializedViewComputeReplica' -count=1
