#!/usr/bin/env bash
set -euo pipefail
go test -race ./hat/hatCache -run '^TestT234' -count=1
