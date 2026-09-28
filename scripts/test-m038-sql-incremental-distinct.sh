#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM038SQLIncrementalDistinct' -count=1
