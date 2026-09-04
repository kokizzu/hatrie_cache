#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run 'TestMonitoringSlowCommandCapture' -count=1
