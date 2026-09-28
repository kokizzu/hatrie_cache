#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestM037SQLIncrementalGroupMinMax' -count=1
