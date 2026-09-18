#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPipeline -run 'TestMZ047TimelineRecovery' -count=1
