#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSchema -count=1 -timeout=60s
