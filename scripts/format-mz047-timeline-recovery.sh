#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
  hat/hatPipeline/mz047_timeline_recovery.go \
  hat/hatPipeline/mz047_timeline_recovery_test.go
