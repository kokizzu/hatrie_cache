#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM037SQLIncrementalGroupMinMax' -count=1
