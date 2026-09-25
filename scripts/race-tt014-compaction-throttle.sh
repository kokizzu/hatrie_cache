#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatStorage -run 'TestCompactionControllerPendingBytes' -count=1 -timeout=60s
