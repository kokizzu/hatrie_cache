#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatCache -run '^TestTR036ReadOnlySQLTransaction' -count=1
