#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPipeline -run 'TestDataflowOperatorPlacement|TestMZ036' -count=1
