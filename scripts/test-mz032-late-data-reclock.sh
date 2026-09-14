#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run 'TestLateDataReclock' -count=1
go test ./hat/hatPipeline
