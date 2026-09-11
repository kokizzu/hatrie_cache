#!/usr/bin/env bash
set -euo pipefail

go test -race \
    ./hat/hatSql \
    ./hat/hatCache \
    ./hat/hatPipeline \
    -run 'TestNamespaceQueryGovernor' \
    -count=1
