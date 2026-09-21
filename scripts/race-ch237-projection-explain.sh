#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestCH237ExplainProjection' -count=1
