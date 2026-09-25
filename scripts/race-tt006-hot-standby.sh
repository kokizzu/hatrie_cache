#!/usr/bin/env bash
set -euo pipefail
go test -race ./hat/hatReplication -run 'TestTT006HotStandby' -count=1
