#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM218MaterializedPointLookupResolver' -count=1
