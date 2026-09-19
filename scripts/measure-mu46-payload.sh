#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu46 ./hat/hatSql -run '^TestDifferentialCheckpointPayloadSizes$$' -count=1 -v
