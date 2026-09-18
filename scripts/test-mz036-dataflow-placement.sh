#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run 'TestDataflowOperatorPlacement' -count=1
