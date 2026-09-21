#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestM218MaterializedPointLookupResolver' -count=1
