#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSchema -run '^TestTT030' -count=1
