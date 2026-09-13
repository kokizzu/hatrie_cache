#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatCache -run '^TestTR033' -count=1
