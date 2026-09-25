#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run 'TestCompactionControllerPendingBytes' -count=1 -timeout=10s
