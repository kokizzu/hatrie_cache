#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQL(SetOperationAll|IntersectAll|ExceptAll)' -count=1
