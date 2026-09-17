#!/usr/bin/env bash
set -euo pipefail

exec env GOMAXPROCS=1 go test ./hat/hatAuth -count=1
