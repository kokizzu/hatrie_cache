#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH237ExplainProjection' -count=1
